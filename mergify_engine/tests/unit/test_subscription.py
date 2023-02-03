from unittest import mock

import pytest
import respx

from mergify_engine import config
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription


async def test_init(redis_cache: redis_utils.RedisCache) -> None:
    subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset({subscription.Features.PRIVATE_REPOSITORY}),
    )


async def test_dict(redis_cache: redis_utils.RedisCache) -> None:
    owner_id = github_types.GitHubAccountIdType(1234)
    sub = subscription.Subscription(
        redis_cache,
        owner_id,
        "friend",
        frozenset({subscription.Features.PRIVATE_REPOSITORY}),
    )

    assert sub.from_dict(redis_cache, owner_id, sub.to_dict(), -2) == sub


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
    )

    await sub._save_subscription_to_cache()
    rsub = await subscription.Subscription._retrieve_subscription_from_cache(
        redis_cache, owner_id
    )
    assert rsub == sub


@mock.patch.object(subscription.Subscription, "_retrieve_subscription_from_db")
async def test_subscription_db_unavailable(
    retrieve_subscription_from_db_mock: mock.Mock,
    redis_cache: redis_utils.RedisCache,
) -> None:
    owner_id = github_types.GitHubAccountIdType(1234)
    sub = subscription.Subscription(
        redis_cache,
        owner_id,
        "friend",
        frozenset([subscription.Features.PUBLIC_REPOSITORY]),
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

    # cache not expired and not db -> got cached  sub
    retrieve_subscription_from_db_mock.reset_mock()
    rsub = await subscription.Subscription.get_subscription(redis_cache, owner_id)
    sub.ttl = 259200
    assert rsub == sub
    retrieve_subscription_from_db_mock.assert_not_called()

    # cache expired and not db -> got cached  sub
    retrieve_subscription_from_db_mock.reset_mock()
    retrieve_subscription_from_db_mock.side_effect = http.HTTPServiceUnavailable(
        "boom!", response=mock.Mock(), request=mock.Mock()
    )
    await redis_cache.expire(f"subscription-cache-owner-{owner_id}", 7200)
    rsub = await subscription.Subscription.get_subscription(redis_cache, owner_id)
    sub.ttl = 7200
    assert rsub == sub
    retrieve_subscription_from_db_mock.assert_called_once()

    # cache expired and unexpected db issue -> reraise
    retrieve_subscription_from_db_mock.reset_mock()
    retrieve_subscription_from_db_mock.side_effect = Exception("WTF")
    await redis_cache.expire(f"subscription-cache-owner-{owner_id}", 7200)
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
        },
    ) == subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset(),
        -2,
    )


async def test_active_feature(redis_cache: redis_utils.RedisCache) -> None:
    sub = subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset([subscription.Features.PRIORITY_QUEUES]),
    )

    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is True
    sub = subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "friend",
        frozenset([subscription.Features.PRIORITY_QUEUES]),
    )
    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is True

    sub = subscription.Subscription.from_dict(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        {
            "subscription_reason": "friend",
            "features": ["private_repository"],
        },
    )
    assert sub.has_feature(subscription.Features.PRIVATE_REPOSITORY) is True
    assert sub.has_feature(subscription.Features.PRIORITY_QUEUES) is False


async def test_subscription_on_premise_valid(
    redis_cache: redis_utils.RedisCache,
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.get(
        f"{config.SUBSCRIPTION_BASE_URL}/on-premise/subscription/1234"
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

    await subscription.SubscriptionDashboardOnPremise.get_subscription(
        redis_cache, github_types.GitHubAccountIdType(1234)
    )

    assert route.call_count == 1


async def test_subscription_on_premise_wrong_token(
    redis_cache: redis_utils.RedisCache, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.get(
        f"{config.SUBSCRIPTION_BASE_URL}/on-premise/subscription/1234"
    ).respond(401, json={"message": "error"})

    with pytest.raises(exceptions.MergifyNotInstalled):
        await subscription.SubscriptionDashboardOnPremise.get_subscription(
            redis_cache, github_types.GitHubAccountIdType(1234)
        )

    assert route.call_count == 1


async def test_subscription_on_premise_invalid_sub(
    redis_cache: redis_utils.RedisCache, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.get(
        f"{config.SUBSCRIPTION_BASE_URL}/on-premise/subscription/1234"
    ).respond(
        403,
        json={"message": "error"},
    )
    with pytest.raises(exceptions.MergifyNotInstalled):
        await subscription.SubscriptionDashboardOnPremise.get_subscription(
            redis_cache, github_types.GitHubAccountIdType(1234)
        )

    assert route.call_count == 1
