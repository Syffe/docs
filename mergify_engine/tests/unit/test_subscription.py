import enum
import typing
from unittest import mock

import pydantic
import pytest
import respx

from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.clients import http
from mergify_engine.tests.tardis import time_travel


async def test_init(redis_cache: redis_utils.RedisCache) -> None:
    subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset({subscription.Features.PRIVATE_REPOSITORY}),
        ["private_repository"],
        expire_at=subscription.Subscription.default_expire_at(),
    )


async def test_dict(redis_cache: redis_utils.RedisCache) -> None:
    owner_id = github_types.GitHubAccountIdType(1234)
    sub = subscription.Subscription(
        redis_cache,
        owner_id,
        "friend",
        frozenset({subscription.Features.PRIVATE_REPOSITORY}),
        ["private_repository"],
        expire_at=subscription.Subscription.default_expire_at(),
    )

    assert sub.from_dict(redis_cache, owner_id, sub.to_dict()) == sub


@pytest.mark.parametrize(
    "features",
    (
        {},
        {subscription.Features.PRIVATE_REPOSITORY},
        {
            subscription.Features.PRIVATE_REPOSITORY,
            subscription.Features.PUBLIC_REPOSITORY,
            subscription.Features.PRIORITY_QUEUES,
        },
    ),
)
async def test_save_sub(
    features: set[subscription.Features], redis_cache: redis_utils.RedisCache
) -> None:
    owner_id = github_types.GitHubAccountIdType(1234)
    sub = subscription.Subscription(
        redis_cache,
        owner_id,
        "friend",
        frozenset(features),
        [typing.cast(subscription.FeaturesLiteralT, f.value) for f in features],
    )

    await sub._save_subscription_to_cache()
    rsub = await subscription.Subscription._retrieve_subscription_from_cache(
        redis_cache, owner_id
    )
    assert rsub == sub


async def test_save_sub_with_unhandled_feature(
    redis_cache: redis_utils.RedisCache,
) -> None:
    owner_id = github_types.GitHubAccountIdType(1234)

    sub = subscription.Subscription.from_dict(
        redis_cache,
        owner_id,
        {
            "subscription_reason": "sub reason",
            "features": [
                "private_repository",
                "public_repository",
                "new_feature",  # type: ignore[list-item]
            ],
            "expire_at": None,
        },
    )

    assert sorted(sub._all_features) == sorted(
        ["public_repository", "private_repository", "new_feature"]
    )
    assert sub.features == frozenset(
        [
            subscription.Features.PRIVATE_REPOSITORY,
            subscription.Features.PUBLIC_REPOSITORY,
        ]
    )

    await sub._save_subscription_to_cache()

    @enum.unique
    class FeaturesMock(enum.Enum):
        PRIVATE_REPOSITORY = "private_repository"
        PUBLIC_REPOSITORY = "public_repository"
        NEW_FEATURE = "new_feature"

    with mock.patch.object(subscription, "Features", FeaturesMock):
        rsub = await subscription.Subscription._retrieve_subscription_from_cache(
            redis_cache, owner_id
        )
        assert rsub is not None
        assert sorted(rsub._all_features) == sorted(
            ["public_repository", "private_repository", "new_feature"]
        )
        assert rsub.features == frozenset(
            [
                FeaturesMock.PRIVATE_REPOSITORY,
                FeaturesMock.PUBLIC_REPOSITORY,
                FeaturesMock.NEW_FEATURE,
            ]
        )


@mock.patch.object(subscription.Subscription, "_retrieve_subscription_from_db")
async def test_subscription_db_unavailable(
    retrieve_subscription_from_db_mock: mock.Mock,
    redis_cache: redis_utils.RedisCache,
) -> None:
    with time_travel("2023-08-25 12:00") as frozen_time:
        owner_id = github_types.GitHubAccountIdType(1234)
        sub = subscription.Subscription(
            redis_cache,
            owner_id,
            "friend",
            frozenset([subscription.Features.PUBLIC_REPOSITORY]),
            ["public_repository"],
        )
        retrieve_subscription_from_db_mock.return_value = sub

        # no cache, no db -> reraise
        retrieve_subscription_from_db_mock.side_effect = http.HTTPServiceUnavailable(
            "boom!", response=mock.Mock(), request=mock.Mock()
        )
        with pytest.raises(http.HTTPServiceUnavailable):
            await subscription.Subscription.get_subscription(redis_cache, owner_id)
            retrieve_subscription_from_db_mock.assert_called_once()

        # no cache, but db -> got db sub
        retrieve_subscription_from_db_mock.reset_mock()
        retrieve_subscription_from_db_mock.side_effect = None
        rsub = await subscription.Subscription.get_subscription(redis_cache, owner_id)
        assert sub == rsub
        retrieve_subscription_from_db_mock.assert_called_once()

        # cache not expired and not db -> got cached sub
        retrieve_subscription_from_db_mock.reset_mock()
        rsub = await subscription.Subscription.get_subscription(redis_cache, owner_id)
        assert rsub == sub
        retrieve_subscription_from_db_mock.assert_not_called()

        # cache expired and not db -> got cached sub and expiration is updated
        retrieve_subscription_from_db_mock.reset_mock()
        retrieve_subscription_from_db_mock.side_effect = http.HTTPServiceUnavailable(
            "boom!", response=mock.Mock(), request=mock.Mock()
        )
        frozen_time.move_to("2023-08-25 13:01")
        await redis_cache.expire(f"subscription-cache-owner-{owner_id}", 7200)
        rsub = await subscription.Subscription.get_subscription(redis_cache, owner_id)
        sub.expire_at = sub.default_expire_at()
        assert rsub == sub
        retrieve_subscription_from_db_mock.assert_called_once()
        # TTL hasn't changed, Redis will remove the key automatically after some time
        assert await redis_cache.ttl(f"subscription-cache-owner-{owner_id}") == 7200

        # cache expired and unexpected db issue -> reraise
        retrieve_subscription_from_db_mock.reset_mock()
        retrieve_subscription_from_db_mock.side_effect = Exception("WTF")
        frozen_time.move_to("2023-08-25 14:02")
        with pytest.raises(Exception, match="WTF"):
            await subscription.Subscription.get_subscription(redis_cache, owner_id)
        retrieve_subscription_from_db_mock.assert_called_once()


async def test_unknown_sub(redis_cache: redis_utils.RedisCache) -> None:
    sub = await subscription.Subscription._retrieve_subscription_from_cache(
        redis_cache, github_types.GitHubAccountIdType(98732189)
    )
    assert sub is None


async def test_from_dict_unknown_features(redis_cache: redis_utils.RedisCache) -> None:
    assert subscription.Subscription.from_dict(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        {
            "subscription_reason": "friend",
            "features": ["unknown feature"],  # type: ignore[list-item]
            "expire_at": None,
        },
    ) == subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset(),
        ["unknown feature"],  # type: ignore[list-item]
    )


async def test_active_feature(redis_cache: redis_utils.RedisCache) -> None:
    sub = subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset([subscription.Features.PRIORITY_QUEUES]),
        ["priority_queues"],
    )

    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is True
    sub = subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset([subscription.Features.PRIORITY_QUEUES]),
        ["priority_queues"],
    )
    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is True

    sub = subscription.Subscription.from_dict(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        {
            "subscription_reason": "friend",
            "features": ["private_repository"],
            "expire_at": None,
        },
    )
    assert sub.has_feature(subscription.Features.PRIVATE_REPOSITORY) is True
    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is False


async def test_subscription_on_premise_valid(
    redis_cache: redis_utils.RedisCache,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SUBSCRIPTION_TOKEN", pydantic.SecretStr("something"))

    route = respx_mock.get(
        f"{settings.SUBSCRIPTION_URL}/on-premise/subscription/1234"
    ).respond(
        200,
        json={
            "subscription_reason": "azertyuio",
            "features": [
                "private_repository",
                "public_repository",
                "priority_queues",
                "custom_checks",
                "random_request_reviews",
                "merge_bot_account",
                "queue_action",
                "show_sponsor",
            ],
        },
    )

    await subscription.SubscriptionShadowOfficeOnPremise.get_subscription(
        redis_cache, github_types.GitHubAccountIdType(1234)
    )

    assert route.call_count == 1


@pytest.mark.ignored_logging_errors(
    "The SUBSCRIPTION_TOKEN is invalid, the subscription can't be checked"
)
async def test_subscription_on_premise_wrong_token(
    redis_cache: redis_utils.RedisCache,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SUBSCRIPTION_TOKEN", pydantic.SecretStr("something"))
    route = respx_mock.get(
        f"{settings.SUBSCRIPTION_URL}/on-premise/subscription/1234"
    ).respond(401, json={"message": "error"})

    with pytest.raises(exceptions.MergifyNotInstalled):
        await subscription.SubscriptionShadowOfficeOnPremise.get_subscription(
            redis_cache, github_types.GitHubAccountIdType(1234)
        )

    assert route.call_count == 1


@pytest.mark.ignored_logging_errors(
    "The subscription attached to SUBSCRIPTION_TOKEN is not valid"
)
async def test_subscription_on_premise_invalid_sub(
    redis_cache: redis_utils.RedisCache,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SUBSCRIPTION_TOKEN", pydantic.SecretStr("something"))
    route = respx_mock.get(
        f"{settings.SUBSCRIPTION_URL}/on-premise/subscription/1234"
    ).respond(
        403,
        json={"message": "error"},
    )
    with pytest.raises(exceptions.MergifyNotInstalled):
        await subscription.SubscriptionShadowOfficeOnPremise.get_subscription(
            redis_cache, github_types.GitHubAccountIdType(1234)
        )

    assert route.call_count == 1
