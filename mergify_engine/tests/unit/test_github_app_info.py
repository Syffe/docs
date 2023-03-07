import respx

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import github


async def test_github_app_info_getter(
    respx_mock: respx.MockRouter, redis_cache: redis_utils.RedisCache
) -> None:
    account = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(4567),
            "login": github_types.GitHubLogin("foobar"),
            "type": "User",
            "avatar_url": "",
        }
    )
    expected_bot = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(4848),
            "login": github_types.GitHubLogin("app[bot]"),
            "type": "Bot",
            "avatar_url": "",
        }
    )
    expected_app = github_types.GitHubApp(
        {"id": 4242, "name": "app", "slug": "app", "owner": account}
    )

    route_installation = respx_mock.get("/app/installations").respond(
        200,
        json=[
            github_types.GitHubInstallation(
                {
                    "id": github_types.GitHubInstallationIdType(1234),
                    "account": account,
                    "target_type": "User",
                    "suspended_at": None,
                    "permissions": {"checks": "write", "contents": "write"},
                }
            )
        ],
    )
    route_token = respx_mock.post("/app/installations/1234/access_tokens").respond(
        200, json={"token": "foobar", "expires_at": "9999-09-26T07:58:30Z"}
    )
    route_app = respx_mock.get("/app").respond(
        200,
        json=expected_app,  # type: ignore[arg-type]
    )
    route_user = respx_mock.get("/users/app[bot]").respond(
        200,
        json=expected_bot,  # type: ignore[arg-type]
    )

    keys = await redis_cache.keys()
    assert keys == []

    # From API
    github.GitHubAppInfo._app = None
    github.GitHubAppInfo._bot = None
    bot = await github.GitHubAppInfo.get_bot(redis_cache)
    assert bot == expected_bot
    app = await github.GitHubAppInfo.get_app(redis_cache)
    assert app == expected_app

    assert route_user.call_count == 1
    assert route_app.call_count == 1
    assert route_installation.call_count == 1

    keys = await redis_cache.keys()
    assert sorted(keys) == [b"github-info-app", b"github-info-bot"]

    # From globals cache
    bot = await github.GitHubAppInfo.get_bot(redis_cache)
    assert bot == expected_bot
    app = await github.GitHubAppInfo.get_app(redis_cache)
    assert app == expected_app
    assert route_user.call_count == 1
    assert route_app.call_count == 1
    assert route_installation.call_count == 1

    # From redis
    github.GitHubAppInfo._app = None
    github.GitHubAppInfo._bot = None
    bot = await github.GitHubAppInfo.get_bot(redis_cache)
    assert bot == expected_bot
    app = await github.GitHubAppInfo.get_app(redis_cache)
    assert app == expected_app

    assert route_user.call_count == 1
    assert route_app.call_count == 1
    assert route_installation.call_count == 1

    # From globals cache again to ensure we don't fetch it from redis and not
    # api call are done
    await redis_cache.flushdb()
    keys = await redis_cache.keys()
    assert keys == []

    bot = await github.GitHubAppInfo.get_bot(redis_cache)
    assert bot == expected_bot
    app = await github.GitHubAppInfo.get_app(redis_cache)
    assert app == expected_app
    assert route_user.call_count == 1
    assert route_app.call_count == 1
    assert route_installation.call_count == 1

    assert route_token.call_count == 1
