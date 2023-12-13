import abc as abstract
from collections import abc
import dataclasses
import datetime
import enum
import logging
import typing

import daiquiri

from mergify_engine import crypto
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.clients import shadow_office


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
    QUEUE_PAUSE = "queue_pause"
    MERGE_QUEUE_STATS = "merge_queue_stats"
    MERGE_QUEUE = "merge_queue"
    WORKFLOW_AUTOMATION = "workflow_automation"


FeaturesLiteralT = typing.Literal[
    "private_repository",
    "public_repository",
    "priority_queues",
    "custom_checks",
    "queue_action",
    "show_sponsor",
    "dedicated_worker",
    "advanced_monitoring",
    "queue_freeze",
    "queue_pause",
    "merge_queue_stats",
    "merge_queue",
    "workflow_automation",
]

LEGACY_FEATURES = ("eventlogs_long", "eventlogs_short")


class SubscriptionDict(typing.TypedDict):
    subscription_reason: str
    features: list[FeaturesLiteralT]
    expire_at: datetime.datetime | None


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
    expire_at: datetime.datetime | None = None

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
                if f in LEGACY_FEATURES:
                    continue
                LOG.log(
                    cls.feature_flag_log_level,
                    "Unknown subscription feature %s",
                    f,
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
    ) -> SubscriptionT:
        return cls(
            redis,
            owner_id,
            sub["subscription_reason"],
            cls._to_features(sub.get("features", [])),
            sub.get("features", []),
            sub.get("expire_at"),
        )

    def to_dict(self) -> SubscriptionDict:
        return {
            "subscription_reason": self.reason,
            "features": self._all_features,
            "expire_at": self.expire_at,
        }

    RETENTION_SECONDS = datetime.timedelta(days=3)
    VALIDITY_SECONDS = datetime.timedelta(hours=1)

    @classmethod
    def default_expire_at(cls) -> datetime.datetime:
        return date.utcnow() + cls.VALIDITY_SECONDS

    async def _has_expired(self) -> bool:
        if self.expire_at is None:  # not cached
            return True
        return date.utcnow() > self.expire_at

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
                    exceptions.should_be_ignored(exc) or exceptions.need_retry_in(exc)
                ):
                    # NOTE(charly): Shadow Office is temporary unavailable,
                    # renew the cache validity and return it, instead of
                    # retrying the stream. But leave the data retention as is,
                    # so Redis can remove the subscription cache itself after
                    # some days. It prevents on premise installations from
                    # cutting the network link to our subscription service.
                    await cached_sub._save_subscription_to_cache(update_retention=False)
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
        cls,
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> None:
        await redis.delete(cls._cache_key(owner_id))

    async def _save_subscription_to_cache(self, update_retention: bool = True) -> None:
        """Save a subscription to the cache."""
        self.expire_at = self.default_expire_at()

        if update_retention:
            await self.redis.setex(
                self._cache_key(self.owner_id),
                self.RETENTION_SECONDS,
                crypto.encrypt(json.dumps(self.to_dict()).encode()),
            )
        else:
            await self.redis.set(
                self._cache_key(self.owner_id),
                crypto.encrypt(json.dumps(self.to_dict()).encode()),
                keepttl=True,
            )

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
        encrypted_sub = await redis.get(cls._cache_key(owner_id))

        if encrypted_sub:
            return cls.from_dict(
                redis,
                owner_id,
                json.loads(crypto.decrypt(encrypted_sub).decode()),
            )
        return None


@dataclasses.dataclass
class SubscriptionShadowOfficeSaas(SubscriptionBase):
    @classmethod
    async def _retrieve_subscription_from_db(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        async with shadow_office.AsyncShadowOfficeSaasClient() as client:
            try:
                resp = await client.get(f"/engine/subscription/{owner_id}")
            except http.HTTPNotFound:
                raise exceptions.MergifyNotInstalled()
            else:
                sub = resp.json()
                return cls.from_dict(redis, owner_id, sub)


class SubscriptionShadowOfficeOnPremise(SubscriptionBase):
    feature_flag_log_level: int = logging.INFO

    @classmethod
    async def _retrieve_subscription_from_db(
        cls: type[SubscriptionT],
        redis: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> SubscriptionT:
        async with shadow_office.AsyncShadowOfficeOnPremiseClient() as client:
            try:
                resp = await client.get(f"/on-premise/subscription/{owner_id}")
            except http.HTTPUnauthorized:
                LOG.critical(
                    "The SUBSCRIPTION_TOKEN is invalid, the subscription can't be checked",
                )
                raise exceptions.MergifyNotInstalled()
            except http.HTTPForbidden:
                LOG.critical(
                    "The subscription attached to SUBSCRIPTION_TOKEN is not valid",
                )
                raise exceptions.MergifyNotInstalled()
            else:
                sub = resp.json()

                return cls.from_dict(redis, owner_id, sub)


if settings.SAAS_MODE:

    @dataclasses.dataclass
    class Subscription(SubscriptionShadowOfficeSaas):
        pass

else:

    @dataclasses.dataclass
    class Subscription(SubscriptionShadowOfficeOnPremise):  # type: ignore [no-redef]
        pass
