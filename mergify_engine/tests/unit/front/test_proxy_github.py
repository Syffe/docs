import pytest
import respx
import sqlalchemy

from mergify_engine import config
from mergify_engine.models import github_user
from mergify_engine.tests import conftest


async def test_github_proxy(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    user = github_user.GitHubUser(
        id=42, login="user-login", oauth_access_token="user-token"
    )
    db.add(user)
    await db.commit()

    unwanted_headers = respx.patterns.M(headers={"dnt": "1"})
    assert unwanted_headers is not None
    respx_mock.route(
        respx.patterns.M(
            method="get",
            url="https://api.github.com/repos",
            headers={"Authorization": "token user-token"},
        )
        & ~unwanted_headers
    ).respond(
        200,
        json={"data": 42},
        headers={
            "link": '<https://api.github.com/repos?page=2>; rel="next", <https://api.github.com/repos?page=7>; rel="last"'
        },
    )

    await web_client.log_as(user.id)
    resp = await web_client.get("/")
    resp = await web_client.get(
        "/front/proxy/github/repos?per_page=100", headers={"dnt": "1"}
    )

    assert resp.json() == {"data": 42}
    assert (
        resp.headers["link"]
        == f'<{config.DASHBOARD_UI_FRONT_BASE_URL}/front/proxy/github/repos?page=2>; rel="next", <{config.DASHBOARD_UI_FRONT_BASE_URL}/front/proxy/github/repos?page=7>; rel="last"'
    )

    await web_client.logout()
    resp = await web_client.get("/front/proxy/github/repos?per_page=100")
    assert resp.status_code == 401
