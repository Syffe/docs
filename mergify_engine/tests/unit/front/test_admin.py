import pytest
import respx
import sqlalchemy.orm

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models import github_user
from mergify_engine.tests import conftest


async def test_sudo_user(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    monkeypatch: pytest.MonkeyPatch,
    respx_mock: respx.MockRouter,
) -> None:
    monkeypatch.setattr(settings, "DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO", [2644])

    admin_account = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(2644),
        login=github_types.GitHubLogin("jd"),
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )
    impersonated_user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(26441),
        login=github_types.GitHubLogin("impersonated_user"),
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )
    db.add(admin_account)
    await db.commit()

    respx_mock.get(
        "http://localhost:5000/engine/associated-users/impersonated_user"
    ).respond(404)

    await web_client.log_as(admin_account.id)
    r = await web_client.get(f"/front/sudo/{impersonated_user.login}")
    assert r.status_code == 404

    db.add(impersonated_user)
    await db.commit()

    r = await web_client.get(f"/front/sudo/{impersonated_user.login}")
    assert r.status_code == 200
    assert await web_client.logged_as() == "impersonated_user"

    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 403
    assert await web_client.logged_as() == "impersonated_user"

    await web_client.logout()
    assert await web_client.logged_as() is None

    await web_client.log_as(impersonated_user.id)
    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 403
    assert await web_client.logged_as() == "impersonated_user"


async def test_sudo_org(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    monkeypatch: pytest.MonkeyPatch,
    respx_mock: respx.MockRouter,
) -> None:
    monkeypatch.setattr(settings, "DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO", [2644])
    admin_account = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(2644),
        login=github_types.GitHubLogin("jd"),
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )
    impersonated_user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(26441),
        login=github_types.GitHubLogin("impersonated_user"),
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )
    db.add(admin_account)
    await db.commit()

    respx_mock.get("http://localhost:5000/engine/associated-users/an-org").respond(
        200,
        json={
            "account": {"id": 1234},
            "associated_users": [
                {"id": 42422, "membership": "member"},
                {"id": 26441, "membership": "admin"},
            ],
        },
    )

    await web_client.log_as(admin_account.id)
    r = await web_client.get("/front/sudo/an-org")
    assert r.status_code == 404

    db.add(impersonated_user)
    await db.commit()

    r = await web_client.get("/front/sudo/an-org")
    assert r.status_code == 200
    assert await web_client.logged_as() == "impersonated_user"

    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 403
    assert await web_client.logged_as() == "impersonated_user"

    await web_client.logout()
    assert await web_client.logged_as() is None

    await web_client.log_as(impersonated_user.id)
    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 403
    assert await web_client.logged_as() == "impersonated_user"
