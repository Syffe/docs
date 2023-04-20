from unittest import mock

import pytest
import respx
import sqlalchemy.orm

from mergify_engine import settings
from mergify_engine.models import github_account
from mergify_engine.models import github_user
from mergify_engine.tests import conftest
from mergify_engine.web.front import applications


@mock.patch.object(applications, "APPLICATIONS_LIMIT", 2)
async def test_applications_life_cycle(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    user = github_user.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    db.add(user)
    await db.commit()

    respx_mock.post(f"{settings.SUBSCRIPTION_URL}/engine/applications").respond(
        status_code=404
    )

    await web_client.log_as(user.id)

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200, resp.text
    list_data = resp.json()
    assert len(list_data["applications"]) == 0

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "my first app"},
    )
    assert resp.status_code == 200
    create_data = resp.json()
    assert create_data["id"] == 1
    assert create_data["name"] == "my first app"
    assert create_data["created_at"] is not None
    assert create_data["created_by"] is not None
    assert create_data["created_by"]["login"] == "user1"
    assert len(create_data["api_access_key"]) == 32
    assert len(create_data["api_secret_key"]) == 43

    # test the generated key
    resp = await web_client.get(
        "/v1/application",
        headers={
            "Authorization": f"bearer {create_data['api_access_key']}{create_data['api_secret_key']}"
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "account_scope": {"id": 424242, "login": "user1"},
        "id": 1,
        "name": "my first app",
    }

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "my second app"},
    )
    assert resp.status_code == 200
    create2_data = resp.json()
    assert create2_data["id"] == 2
    assert create2_data["name"] == "my second app"
    assert create2_data["created_at"] is not None
    assert create2_data["created_by"] is not None
    assert create2_data["created_by"]["login"] == "user1"
    assert len(create2_data["api_access_key"]) == 32
    assert len(create2_data["api_secret_key"]) == 43

    resp = await web_client.patch(
        f"/front/github-account/424242/applications/{create2_data['id']}",
        json={"name": "new name"},
    )
    assert resp.status_code == 200
    update_data = resp.json()
    assert update_data["id"] == 2
    assert update_data["name"] == "new name"
    assert update_data["created_at"] is not None
    assert update_data["created_by"] is not None
    assert update_data["created_by"]["login"] == "user1"

    resp = await web_client.get(
        "/front/github-account/424242/applications",
    )
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == 2
    assert list_data["applications"][0]["created_at"] is not None
    assert list_data["applications"][1]["created_at"] is not None
    assert list_data["applications"][0]["created_by"] is not None
    assert list_data["applications"][1]["created_by"] is not None
    assert list_data["applications"][0]["created_by"]["login"] == "user1"
    assert list_data["applications"][1]["created_by"]["login"] == "user1"

    # Check I reach the limit
    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "my last app"},
    )
    assert resp.status_code == 400

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == 2

    resp = await web_client.delete(
        f"/front/github-account/424242/applications/{create2_data['id']}",
    )
    assert resp.status_code == 204

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == 1

    resp = await web_client.delete(
        f"/front/github-account/424242/applications/{create_data['id']}",
    )
    assert resp.status_code == 204

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == 0


async def test_applications_bad_body(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
) -> None:
    gha = github_account.GitHubAccount(
        id=424242,
        login="user1",
    )
    user = github_user.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    db.add(gha)
    db.add(user)
    await db.commit()

    await web_client.log_as(user.id)

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "no" * 500},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"limit_value": 255},
            "loc": ["body", "name"],
            "msg": "ensure this value has at most 255 characters",
            "type": "value_error.any_str.max_length",
        }
    ]

    resp = await web_client.post(
        "/front/github-account/424242/applications", json={"name": ""}
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"limit_value": 1},
            "loc": ["body", "name"],
            "msg": "ensure this value has at least 1 characters",
            "type": "value_error.any_str.min_length",
        }
    ]

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"noway": "azerty"},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "loc": ["body", "name"],
            "msg": "field required",
            "type": "value_error.missing",
        }
    ]

    resp = await web_client.patch(
        "/front/github-account/424242/applications/123",
        json={"name": "no" * 500},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"limit_value": 255},
            "loc": ["body", "name"],
            "msg": "ensure this value has at most 255 characters",
            "type": "value_error.any_str.max_length",
        }
    ]

    resp = await web_client.patch(
        "/front/github-account/424242/applications/123",
        json={"name": ""},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"limit_value": 1},
            "loc": ["body", "name"],
            "msg": "ensure this value has at least 1 characters",
            "type": "value_error.any_str.min_length",
        }
    ]

    # TODO(sileht): it would be better to raise a 422 instead of ignoring the attribute.
    resp = await web_client.patch(
        "/front/github-account/424242/applications/123",
        json={"name": "hellothere", "noway": "azerty"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Not Found"


@pytest.mark.parametrize(
    "role, status_code",
    (("admin", 200), ("member", 403), (None, 403)),
)
async def test_applications_permissions_for_orgs(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
    role: str | None,
    status_code: int,
) -> None:
    gha = github_account.GitHubAccount(
        id=1234,
        login="org1",
    )
    user = github_user.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )

    db.add(gha)
    db.add(user)
    await db.commit()

    request_mock = respx_mock.get("https://api.github.com/user/memberships/orgs/1234")
    if role is None:
        request_mock.respond(status_code=404)
    else:
        request_mock.respond(
            status_code=200,
            json={"state": "active", "role": role},
        )

    await web_client.log_as(user.id)
    resp = await web_client.get("/front/github-account/1234/applications")
    assert resp.status_code == status_code
