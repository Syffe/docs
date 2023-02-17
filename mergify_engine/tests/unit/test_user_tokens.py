from unittest import mock

import pytest

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import user_tokens


async def test_init(redis_cache: redis_utils.RedisCache) -> None:
    user_tokens.UserTokens(redis_cache, 123, [])


@pytest.mark.parametrize(
    "users",
    (
        [],
        [
            {
                "id": 54321,
                "login": "foo",
                "oauth_access_token": "bar",
                "name": None,
                "email": None,
            }
        ],
        [
            {
                "id": 42,
                "login": "foo",
                "oauth_access_token": "bar",
                "name": None,
                "email": None,
            },
            {
                "id": 123,
                "login": "login",
                "oauth_access_token": "token",
                "name": None,
                "email": None,
            },
        ],
    ),
)
async def test_save_ut(
    users: list[user_tokens.UserTokensUser], redis_cache: redis_utils.RedisCache
) -> None:
    owner_id = 1234
    ut = user_tokens.UserTokens(
        redis_cache,
        owner_id,
        users,
    )

    await ut.save_to_cache()
    rut = await user_tokens.UserTokens._retrieve_from_cache(
        redis_cache, owner_id, False
    )
    assert ut == rut

    for user in ut.users:
        user["oauth_access_token"] = github_types.GitHubOAuthToken("")
    rut = await user_tokens.UserTokens._retrieve_from_cache(redis_cache, owner_id, True)
    assert ut == rut


@mock.patch.object(user_tokens.UserTokens, "_retrieve_from_db")
async def test_user_tokens_db_unavailable(
    retrieve_from_db_mock: mock.Mock, redis_cache: redis_utils.RedisCache
) -> None:
    owner_id = 1234
    ut = user_tokens.UserTokens(redis_cache, owner_id, [])
    retrieve_from_db_mock.return_value = ut

    # no cache, no db -> reraise
    retrieve_from_db_mock.side_effect = http.HTTPServiceUnavailable(
        "boom!", response=mock.Mock(), request=mock.Mock()
    )
    with pytest.raises(http.HTTPServiceUnavailable):
        await user_tokens.UserTokens.get(redis_cache, owner_id, False)
        retrieve_from_db_mock.assert_called_once()

    # no cache, but db -> got db ut
    retrieve_from_db_mock.reset_mock()
    retrieve_from_db_mock.side_effect = None
    rut = await user_tokens.UserTokens.get(redis_cache, owner_id, False)
    assert ut == rut
    retrieve_from_db_mock.assert_called_once()

    # cache not expired and not db -> got cached  ut
    retrieve_from_db_mock.reset_mock()
    rut = await user_tokens.UserTokens.get(redis_cache, owner_id, False)
    ut.ttl = 259200
    assert rut == ut
    retrieve_from_db_mock.assert_not_called()

    # cache expired and not db -> got cached  ut
    retrieve_from_db_mock.reset_mock()
    retrieve_from_db_mock.side_effect = http.HTTPServiceUnavailable(
        "boom!", response=mock.Mock(), request=mock.Mock()
    )
    await redis_cache.expire(f"user-tokens-cache-owner-{owner_id}", 7200)
    rut = await user_tokens.UserTokens.get(redis_cache, owner_id, False)
    ut.ttl = 7200
    assert rut == ut
    retrieve_from_db_mock.assert_called_once()

    # cache expired and unexpected db issue -> reraise
    retrieve_from_db_mock.reset_mock()
    retrieve_from_db_mock.side_effect = Exception("WTF")
    await redis_cache.expire(f"user-tokens-cache-owner-{owner_id}", 7200)
    with pytest.raises(Exception, match="WTF"):
        await user_tokens.UserTokens.get(redis_cache, owner_id, False)
    retrieve_from_db_mock.assert_called_once()


async def test_unknown_ut(redis_cache: redis_utils.RedisCache) -> None:
    tokens = await user_tokens.UserTokens._retrieve_from_cache(
        redis_cache, 98732189, False
    )
    assert tokens is None


async def test_user_tokens_tokens_via_env(
    monkeypatch: pytest.MonkeyPatch, redis_cache: redis_utils.RedisCache
) -> None:
    ut = await user_tokens.UserTokensOnPremise.get(redis_cache, 123, False)

    assert ut.get_token_for(github_types.GitHubLogin("foo")) is None
    assert ut.get_token_for(github_types.GitHubLogin("login")) is None
    assert ut.get_token_for(github_types.GitHubLogin("nop")) is None

    monkeypatch.setattr(
        config, "ACCOUNT_TOKENS", config.AccountTokens("1:foo:bar,5:login:token")
    )

    ut = await user_tokens.UserTokensOnPremise.get(redis_cache, 123, False)
    foo_token = ut.get_token_for(github_types.GitHubLogin("foo"))
    assert foo_token is not None
    assert foo_token["id"] == 1
    assert foo_token["oauth_access_token"] == "bar"
    login_token = ut.get_token_for(github_types.GitHubLogin("login"))
    assert login_token is not None
    assert login_token["id"] == 5
    assert login_token["oauth_access_token"] == "token"
    assert ut.get_token_for(github_types.GitHubLogin("nop")) is None
