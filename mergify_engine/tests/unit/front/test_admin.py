from unittest import mock

import pytest
import respx
import sqlalchemy.orm

from mergify_engine import debug
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models.github import user as github_user
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
    impersonated_other_user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(26442),
        login=github_types.GitHubLogin("impersonated_other_user"),
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )
    deleted_github_user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(1),
        login=impersonated_user.login,
        oauth_access_token=github_types.GitHubOAuthToken("foobar"),
    )

    db.add(admin_account)
    await db.commit()

    respx_mock.get(
        "http://localhost:5000/engine/associated-users/impersonated_user",
    ).respond(404)

    respx_mock.get("http://localhost:5000/engine/associated-users/anotheruser").respond(
        404,
    )

    respx_mock.get(
        "http://localhost:5000/engine/associated-users/impersonated_other_user",
    ).respond(404)

    await web_client.log_as(admin_account.id)
    r = await web_client.get(f"/front/sudo/{impersonated_user.login}")
    assert r.status_code == 404
    r = await web_client.get(f"/front/sudo/{impersonated_other_user.login}")
    assert r.status_code == 404

    db.add(impersonated_user)
    db.add(impersonated_other_user)
    db.add(deleted_github_user)
    await db.commit()

    r = await web_client.get(f"/front/sudo/{impersonated_user.login}")
    assert r.status_code == 200
    assert await web_client.logged_as() == "impersonated_user"

    r = await web_client.get(f"/front/sudo/{impersonated_other_user.login}")
    assert r.status_code == 200
    assert await web_client.logged_as() == "impersonated_other_user"

    # user is unknown I'm back to real myself
    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 404
    assert await web_client.logged_as() == admin_account.login

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

    respx_mock.get("http://localhost:5000/engine/associated-users/anotheruser").respond(
        404,
    )

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

    # user is unknown I'm back to real myself
    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 404
    assert await web_client.logged_as() == admin_account.login

    await web_client.logout()
    assert await web_client.logged_as() is None

    await web_client.log_as(impersonated_user.id)
    r = await web_client.get("/front/sudo/anotheruser")
    assert r.status_code == 403
    assert await web_client.logged_as() == "impersonated_user"


async def test_sudo_debug(
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
    db.add(impersonated_user)
    await db.commit()

    r = await web_client.get("/front/sudo-debug/anotheruser/reponame/pull/1234")
    assert r.status_code == 401

    await web_client.log_as(admin_account.id)

    def fake_debug(url: str) -> None:
        print("hello world")  # noqa: T201

    with mock.patch.object(debug, "report", side_effect=fake_debug):
        # user is unknown I'm back to real myself
        r = await web_client.get("/front/sudo-debug/anotheruser/reponame/pull/1234")
        assert r.status_code == 200
        assert r.text == "hello world\n"
        assert await web_client.logged_as() == admin_account.login
