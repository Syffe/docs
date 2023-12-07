from urllib import parse

import httpx
import pytest
import respx
import sqlalchemy.orm
import starlette

from mergify_engine import github_types
from mergify_engine.models.github import user as github_user
from mergify_engine.tests import conftest
from mergify_engine.web.front import auth


def build_request(path: str) -> starlette.requests.Request:
    scope = {  # type: ignore[var-annotated]
        "session": {},
        "auth": None,
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": path,
        "query_string": b"",
        "headers": [(b"host", b"www.example.org"), (b"accept", b"application/json")],
        "client": ("134.56.78.4", 1453),
        "server": ("www.example.org", 443),
    }
    return starlette.requests.Request(scope=scope)


async def test_new_user(
    respx_mock: respx.MockRouter,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    github_api_user = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(42),
            "login": github_types.GitHubLogin("user-login"),
            "avatar_url": "http://example.com/logo",
            "type": "User",
        },
    )

    respx_mock.post(
        "http://localhost:5000/engine/user-update",
        json={"user": github_api_user, "token": "user-token"},
    ).respond(204)
    respx_mock.get("/user").respond(200, json=dict(github_api_user))

    await auth.create_or_update_user(
        build_request("/front/auth/authorized"),
        db,
        github_types.GitHubOAuthToken("user-token"),
    )

    db.expire_all()

    result = await db.execute(
        sqlalchemy.select(github_user.GitHubUser).where(
            github_user.GitHubUser.id == 42,
        ),
    )

    new_account = result.unique().scalar_one()
    assert new_account.id == 42
    assert new_account.login == "user-login"
    assert new_account.oauth_access_token == "user-token"


@pytest.mark.parametrize(
    ("query_string", "status_code", "redirect_url", "has_repositories"),
    (
        (
            "setup_action=install&installation_id=123",
            200,
            "/github/user-login?new=true",
            True,
        ),
        ("setup_action=install&installation_id=123", 200, "/github?new=true", False),
        ("setup_action=request", 200, "/github?request=true", None),
        ("setup_action=unknown", 200, "/", None),
        ("setup_action=install", 400, None, None),
    ),
)
async def test_auth_setup(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    query_string: str,
    status_code: int,
    redirect_url: str | None,
    has_repositories: bool | None,
    respx_mock: respx.MockRouter,
) -> None:
    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    github_api_user = user.to_github_account()

    setup_action = parse.parse_qs(query_string).get("setup_action")
    if redirect_url and setup_action and setup_action[0] == "install":
        respx_mock.post(
            "http://localhost:5000/engine/user-update",
            json={"user": github_api_user, "token": user.oauth_access_token},
        ).respond(204)
        respx_mock.get("/user").respond(200, json=dict(github_api_user))

    if has_repositories is True:
        respx_mock.get("/user/installations/123/repositories").respond(
            200,
            json={"repositories": [{"owner": github_api_user}], "total_count": 1},
        )
    elif has_repositories is False:
        respx_mock.get("/user/installations/123/repositories").respond(404)

    await web_client.log_as(user.id)
    assert await web_client.logged_as() == "user-login"

    r = await web_client.get(f"/front/auth/setup?{query_string}")
    assert r.status_code == status_code
    if redirect_url is not None:
        assert r.json()["url"] == redirect_url


async def test_auth_lifecycle(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    github_api_user = user.to_github_account()

    respx_mock.post(
        "http://localhost:5000/engine/user-update",
        json={"user": github_api_user, "token": user.oauth_access_token},
    ).respond(204)
    respx_mock.get("/user").respond(200, json=dict(github_api_user))

    respx_mock.post("https://github.com/login/oauth/access_token").respond(
        200,
        json={"access_token": user.oauth_access_token},
    )
    respx_mock.get("https://api.github.com/repos/foo/bar/pulls").respond(200, json=[])

    await web_client.log_as(user.id)
    assert await web_client.logged_as() == "user-login"
    saved_cookies_logged = httpx.Cookies(web_client.cookies)

    r = await web_client.get("/front/proxy/github/repos/foo/bar/pulls")
    assert r.status_code == 200
    assert r.json() == []

    r = await web_client.get("/front/auth/authorize", follow_redirects=False)
    assert r.status_code == 200
    assert r.json()["url"].startswith("https://github.com/login/oauth/authorize?")
    query_string = parse.parse_qs(parse.urlparse(r.json()["url"]).query)
    saved_cookies_mid_login = httpx.Cookies(web_client.cookies)

    # Ensure session id change
    assert (
        saved_cookies_logged["mergify-session"]
        != saved_cookies_mid_login["mergify-session"]
    )

    # old cookie doesn't work anymore
    r = await web_client.get(
        "/front/proxy/github/repos/foo/bar/pulls",
        cookies=saved_cookies_logged,
    )
    assert r.status_code == 401

    # new cookie doesn't work yet
    r = await web_client.get("/front/proxy/github/repos/foo/bar/pulls")
    assert r.status_code == 401

    r = await web_client.get(
        f"/front/auth/authorized?code=XXXXX&state={query_string['state'][0]}",
    )
    assert r.status_code == 204, r.text

    # Ensure session id change again
    assert (
        saved_cookies_mid_login["mergify-session"]
        != web_client.cookies["mergify-session"]
    )
    assert (
        saved_cookies_logged["mergify-session"] != web_client.cookies["mergify-session"]
    )

    # old cookie doesn't work anymore
    r = await web_client.get(
        "/front/proxy/github/repos/foo/bar/pulls",
        cookies=saved_cookies_logged,
    )
    assert r.status_code == 401

    # mid-session cookie still doesn't work anymore
    r = await web_client.get(
        "/front/proxy/github/repos/foo/bar/pulls",
        cookies=saved_cookies_mid_login,
    )
    assert r.status_code == 401

    # new cookie works
    r = await web_client.get("/front/proxy/github/repos/foo/bar/pulls")
    assert r.status_code == 200
    assert r.json() == []

    # mid-session cookie doesn't work
    r = await web_client.get(
        "/front/proxy/github/repos/foo/bar/pulls",
        cookies=saved_cookies_mid_login,
    )
    assert r.status_code == 401

    # old cookie doesn't work
    r = await web_client.get(
        "/front/proxy/github/repos/foo/bar/pulls",
        cookies=saved_cookies_logged,
    )
    assert r.status_code == 401
