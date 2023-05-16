import abc as abstract
from collections import abc
import dataclasses
import enum
import json
import logging
import typing

import daiquiri

from mergify_engine import crypto
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import dashboard
from mergify_engine.clients import http


LOG = daiquiri.getLogger(__name__)


SubscriptionT = typing.TypeVar("SubscriptionT", bound="SubscriptionBase")


@enum.unique
class Features(enum.Enum):
    PRIVATE_REPOSITORY = "private_repository"
    PUBLIC_REPOSITORY = "public_repository"
    PRIORITY_QUEUES = "priority_queues"
    CUSTOM_CHECKS = "custom_checks"
    # QUEUE_ACTION is badly named because in the code it actually is
    # the feature flag for multi queues
    QUEUE_ACTION = "queue_action"
    SHOW_SPONSOR = "show_sponsor"
    DEDICATED_WORKER = "dedicated_worker"
    ADVANCED_MONITORING = "advanced_monitoring"
    QUEUE_FREEZE = "queue_freeze"
    EVENTLOGS_SHORT = "eventlogs_short"
    EVENTLOGS_LONG = "eventlogs_long"
    MERGE_QUEUE_STATS = "merge_queue_stats"
    MERGE_QUEUE = "merge_queue"
    WORKFLOW_AUTOMATION = "workflow_automation"


FeaturesLiteralT = typing.Literal[
    "private_repository",
    "public_repository",
    "priority_queues",
    "custom_checks",
    "random_request_reviews",
    "merge_bot_account",
    "queue_action",
    "depends_on",
    "show_sponsor",
    "dedicated_worker",
    "advanced_monitoring",
    "queue_freeze",
    "eventlogs_short",
    "eventlogs_long",
    "merge_queue_stats",
    "merge_queue",
    "workflow_automation",
]


class SubscriptionDict(typing.TypedDict):
    subscription_reason: str
    features: list[FeaturesLiteralT]


@dataclasses.dataclass
class SubscriptionBase(abstract.ABC):
    redis: redis_utils.RedisCache
    owner_id: github_types.GitHubAccountIdType
    reason: str
    # `features` contains a frozenset of `Features` enum we were able to instantiate
    features: frozenset[enum.Enum]
    # `_all_features` contains every feature string that was in database, even
    # the one that we were not able to instantiate. The features we were not able to
    # instantiate might be new features not yet implemented on the engine side.
    _all_features: list[FeaturesLiteralT]
    ttl: int = -2

    feature_flag_log_level: int = logging.WARNING

    @staticmethod
    def _cache_key(owner_id: github_types.GitHubAccountIdType) -> str:
        return f"subscription-cache-owner-{owner_id}"

    @classmethod
    def _to_features(cls, feature_list: abc.Iterable[str]) -> frozenset[Features]:
        features = []
        for f in feature_list:
            try:
                feature = Features(f)
            except ValueError:
                LOG.log(
                    cls.feature_flag_log_level, "Unknown subscription feature %s", f
                )
            else:
                features.append(feature)
        return frozenset(features)

    def has_feature(self, feature: Features) -> bool:
        """Return if the feature for a plan is available."""
        return feature in self.features

    @staticmethod
    def missing_feature_reason(owner: str) -> str:
        return f"⚠ The [subscription]({settings.DASHBOARD_UI_FRONT_URL}/github/{owner}/subscription) needs to be updated to enable this feature."

    @classmethod
    def from_dict(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
        sub: SubscriptionDict,
        ttl: int = -2,
    ) -> SubscriptionT:
        return cls(
            redis,
            owner_id,
            sub["subscription_reason"],
            cls._to_features(sub.get("features", [])),
            sub.get("features", []),
            ttl,
        )

    def to_dict(self) -> SubscriptionDict:
        return {
            "subscription_reason": self.reason,
            "features": self._all_features,
        }

    RETENTION_SECONDS = 60 * 60 * 24 * 3  # 3 days
    VALIDITY_SECONDS = 3600

    async def _has_expired(self) -> bool:
        if self.ttl < 0:  # not cached
            return True
        elapsed_since_stored = self.RETENTION_SECONDS - self.ttl
        return elapsed_since_stored > self.VALIDITY_SECONDS

    @classmethod
    async def get_subscription(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        """Get a subscription."""
        cached_sub = await cls._retrieve_subscription_from_cache(redis, owner_id)
        if cached_sub is None or await cached_sub._has_expired():
            try:
                db_sub = await cls._retrieve_subscription_from_db(redis, owner_id)
            except Exception as exc:
                if cached_sub is not None and (
                    exceptions.should_be_ignored(exc) or exceptions.need_retry(exc)
                ):
                    # NOTE(sileht): return the cached sub, instead of retry the stream,
                    # just because the dashboard have connectivity issue.
                    return cached_sub
                raise

            await db_sub._save_subscription_to_cache()
            return db_sub
        return cached_sub

    @classmethod
    async def update_subscription(
        cls,
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
        sub: SubscriptionDict,
    ) -> None:
        await cls.from_dict(redis, owner_id, sub)._save_subscription_to_cache()

    @classmethod
    async def delete_subscription(
        cls, redis: redis_utils.RedisCache, owner_id: github_types.GitHubAccountIdType
    ) -> None:
        await redis.delete(cls._cache_key(owner_id))

    async def _save_subscription_to_cache(self) -> None:
        """Save a subscription to the cache."""
        await self.redis.setex(
            self._cache_key(self.owner_id),
            self.RETENTION_SECONDS,
            crypto.encrypt(json.dumps(self.to_dict()).encode()),
        )
        self.ttl = self.RETENTION_SECONDS

    @classmethod
    @abstract.abstractmethod
    async def _retrieve_subscription_from_db(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        pass

    @classmethod
    async def _retrieve_subscription_from_cache(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT | None:
        async with await redis.pipeline() as pipe:
            await pipe.get(cls._cache_key(owner_id))
            await pipe.ttl(cls._cache_key(owner_id))
            encrypted_sub, ttl = typing.cast(tuple[bytes, int], await pipe.execute())

        if encrypted_sub:
            return cls.from_dict(
                redis,
                owner_id,
                json.loads(crypto.decrypt(encrypted_sub).decode()),
                ttl,
            )
        return None


@dataclasses.dataclass
class SubscriptionDashboardSaas(SubscriptionBase):
    @classmethod
    async def _retrieve_subscription_from_db(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        async with dashboard.AsyncDashboardSaasClient() as client:
            try:
                resp = await client.get(f"/engine/subscription/{owner_id}")
            except http.HTTPNotFound:
                raise exceptions.MergifyNotInstalled()
            else:
                sub = resp.json()
                return cls.from_dict(redis, owner_id, sub)


class SubscriptionDashboardOnPremise(SubscriptionBase):
    feature_flag_log_level: int = logging.INFO

    @classmethod
    async def _retrieve_subscription_from_db(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        async with dashboard.AsyncDashboardOnPremiseClient() as client:
            try:
                resp = await client.get(f"/on-premise/subscription/{owner_id}")
            except http.HTTPUnauthorized:
                LOG.critical(
                    "The SUBSCRIPTION_TOKEN is invalid, the subscription can't be checked"
                )
                raise exceptions.MergifyNotInstalled()
            except http.HTTPForbidden:
                LOG.critical(
                    "The subscription attached to SUBSCRIPTION_TOKEN is not valid"
                )
                raise exceptions.MergifyNotInstalled()
            else:
                sub = resp.json()

                return cls.from_dict(redis, owner_id, sub)


if settings.SAAS_MODE:

    @dataclasses.dataclass
    class Subscription(SubscriptionDashboardSaas):
        pass

else:

    @dataclasses.dataclass
    class Subscription(SubscriptionDashboardOnPremise):  # type: ignore [no-redef]
        pass
