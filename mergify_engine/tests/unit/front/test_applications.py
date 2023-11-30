import secrets

import pytest
import respx
import sqlalchemy.orm

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.models import application_keys
from mergify_engine.models import github as gh_models
from mergify_engine.tests import conftest
from mergify_engine.tests import utils


async def test_applications_life_cycle(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    user = gh_models.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    db.add(user)
    await db.commit()

    get_installation_mock = respx_mock.get("/user/424242/installation").respond(
        200,
        json={
            "id": 1234,
            "account": user.to_github_account(),
            "suspended_at": None,
        },
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
    assert create_data["id"] == utils.ANY_UUID4
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
            "Authorization": f"bearer {create_data['api_access_key']}{create_data['api_secret_key']}",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "account_scope": {"id": 424242, "login": "user1"},
        "id": utils.ANY_UUID4,
        "name": "my first app",
    }

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "my second app"},
    )
    assert resp.status_code == 200
    create2_data = resp.json()
    assert create2_data["id"] == utils.ANY_UUID4
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
    assert update_data["id"] == utils.ANY_UUID4
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

    # User uninstall Mergify or delete it's account
    get_installation_mock.respond(404, json={})

    # Check we can't use the application anymore
    resp = await web_client.get(
        "/v1/application",
        headers={
            "Authorization": f"bearer {create_data['api_access_key']}{create_data['api_secret_key']}",
        },
    )
    assert resp.status_code == 403

    # Check that even if the http session is valid, we can't manipulate applications
    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 403

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "my second app"},
    )
    assert resp.status_code == 403

    resp = await web_client.patch(
        "/front/github-account/424242/applications/123",
    )
    assert resp.status_code == 403

    resp = await web_client.delete(
        "/front/github-account/424242/applications/123",
    )
    assert resp.status_code == 403


async def assert_database_application_keys_count(
    github_account_id: int,
    expected: int,
) -> None:
    async with database.create_session() as session:
        updated_user = await session.scalar(
            sqlalchemy.select(gh_models.GitHubAccount).where(
                gh_models.GitHubAccount.id == github_account_id,
            ),
        )
        assert updated_user is not None
        assert updated_user.application_keys_count == expected

        application_keys_count = await session.scalar(
            sqlalchemy.select(
                sqlalchemy.func.count(application_keys.ApplicationKey.id),
            ).where(
                application_keys.ApplicationKey.github_account_id == github_account_id,
            ),
        )
        assert application_keys_count == expected


async def test_applications_limit(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    user = gh_models.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    account = gh_models.GitHubAccount(
        id=user.id,
        login=user.login,
        type="User",
        avatar_url="https://dummy.com",
    )
    db.add(user)
    db.add(account)
    await db.commit()

    respx_mock.get("/user/424242/installation").respond(
        200,
        json={
            "id": 1234,
            "account": user.to_github_account(),
            "suspended_at": None,
        },
    )

    api_access_key = f"mka_{secrets.token_urlsafe(21)}"
    api_secret_key = secrets.token_urlsafe(32)  # 256bytes encoded in base64

    for i in range(application_keys.APPLICATIONS_LIMIT):
        application = application_keys.ApplicationKey(
            name=f"whatever {i}",
            api_access_key=api_access_key,
            api_secret_key=api_secret_key,
            github_account_id=account.id,
            created_by_github_user_id=user.id,
        )
        db.add(application)

    await db.commit()

    await assert_database_application_keys_count(user.id, 200)

    await web_client.log_as(user.id)
    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == application_keys.APPLICATIONS_LIMIT

    await assert_database_application_keys_count(user.id, 200)

    # Check I reach the limit
    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "not this one"},
    )
    assert resp.status_code == 400

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == application_keys.APPLICATIONS_LIMIT

    await assert_database_application_keys_count(user.id, 200)

    for i in range(10):
        resp = await web_client.delete(
            f"/front/github-account/424242/applications/{list_data['applications'][i]['id']}",
        )
        assert resp.status_code == 204, resp.text

    resp = await web_client.get("/front/github-account/424242/applications")
    assert resp.status_code == 200
    list_data = resp.json()
    assert len(list_data["applications"]) == application_keys.APPLICATIONS_LIMIT - 10

    await assert_database_application_keys_count(user.id, 190)


async def test_create_application_for_orgs(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    user = gh_models.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    db.add(user)
    await db.commit()

    respx_mock.get("/user/1234/installation").respond(
        200,
        json={
            "id": 1234,
            "account": {
                "id": 1234,
                "login": "theorg",
            },
            "suspended_at": None,
        },
    )

    await web_client.log_as(user.id)

    respx_mock.get("https://api.github.com/user/memberships/orgs/1234").respond(
        status_code=200,
        json={
            "state": "active",
            "role": "admin",
            "user": {"id": 424242, "login": "user1"},
            "organization": {"id": 1234, "login": "org1"},
        },
    )
    resp = await web_client.post(
        "/front/github-account/1234/applications",
        json={"name": "my first app"},
    )
    assert resp.status_code == 200
    create_data = resp.json()
    assert create_data["id"] == utils.ANY_UUID4
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
            "Authorization": f"bearer {create_data['api_access_key']}{create_data['api_secret_key']}",
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "account_scope": {"id": 1234, "login": "org1"},
        "id": utils.ANY_UUID4,
        "name": "my first app",
    }


async def test_applications_bad_body(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    gha = gh_models.GitHubAccount(
        id=424242,
        login="user1",
        avatar_url="https://dummy.com",
    )
    user = gh_models.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )
    db.add(gha)
    db.add(user)
    await db.commit()

    respx_mock.get("/user/424242/installation").respond(
        200,
        json={
            "id": 1234,
            "account": user.to_github_account(),
            "suspended_at": None,
        },
    )

    await web_client.log_as(user.id)

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "no" * 500},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"max_length": 255},
            "input": "no" * 500,
            "loc": ["body", "name"],
            "msg": "String should have at most 255 characters",
            "type": "string_too_long",
        },
    ]

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": ""},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"min_length": 1},
            "input": "",
            "loc": ["body", "name"],
            "msg": "String should have at least 1 character",
            "type": "string_too_short",
        },
    ]

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"noway": "azerty"},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "input": {"noway": "azerty"},
            "loc": ["body", "name"],
            "msg": "Field required",
            "type": "missing",
        },
    ]

    resp = await web_client.patch(
        "/front/github-account/424242/applications/e155518c-ca1b-443c-9be9-fe90fdab7345",
        json={"name": "no" * 500},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"max_length": 255},
            "input": "no" * 500,
            "loc": ["body", "name"],
            "msg": "String should have at most 255 characters",
            "type": "string_too_long",
        },
    ]

    resp = await web_client.patch(
        "/front/github-account/424242/applications/e155518c-ca1b-443c-9be9-fe90fdab7345",
        json={"name": ""},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "ctx": {"min_length": 1},
            "input": "",
            "loc": ["body", "name"],
            "msg": "String should have at least 1 character",
            "type": "string_too_short",
        },
    ]

    # TODO(sileht): it would be better to raise a 422 instead of ignoring the attribute.
    resp = await web_client.patch(
        "/front/github-account/424242/applications/e155518c-ca1b-443c-9be9-fe90fdab7345",
        json={"name": "hellothere", "noway": "azerty"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Not Found"

    resp = await web_client.post(
        "/front/github-account/424242/applications",
        json={"name": "such-\x00-string"},
    )
    assert resp.status_code == 422


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
    gha = gh_models.GitHubAccount(
        id=1234,
        login="org1",
        avatar_url="https://dummy.com",
    )
    user = gh_models.GitHubUser(
        id=424242,
        login="user1",
        oauth_access_token="token",
    )

    db.add(gha)
    db.add(user)
    await db.commit()

    respx_mock.get("/user/1234/installation").respond(
        200,
        json={
            "id": 1234,
            "account": {
                "id": 1234,
                "login": "theorg",
            },
            "suspended_at": None,
        },
    )

    request_mock = respx_mock.get("https://api.github.com/user/memberships/orgs/1234")
    if role is None:
        request_mock.respond(status_code=404)
    else:
        request_mock.respond(
            status_code=200,
            json={
                "state": "active",
                "role": role,
                "user": {"id": 424242, "login": "user1"},
                "organization": {"id": 1234, "login": "org1"},
            },
        )

    await web_client.log_as(user.id)
    resp = await web_client.get("/front/github-account/1234/applications")
    assert resp.status_code == status_code


async def test_application_tokens_via_env(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    redis_cache: redis_utils.RedisCache,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    api_access_key1 = "1" * 32
    api_secret_key1 = "1" * 32
    account_id1 = github_types.GitHubAccountIdType(12345)
    account_login1 = github_types.GitHubLogin("login1")

    api_access_key2 = "2" * 32
    api_secret_key2 = "2" * 32
    account_id2 = github_types.GitHubAccountIdType(67891)
    account_login2 = github_types.GitHubLogin("login2")

    respx_mock.get("/user/67891/installation").respond(
        200,
        json={
            "id": 1234,
            "account": {
                "id": 67891,
                "login": "login2",
            },
            "suspended_at": None,
        },
    )
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 1234,
            "account": {
                "id": 12345,
                "login": "login1",
            },
            "suspended_at": None,
        },
    )

    resp = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {api_access_key1}{api_secret_key1}"},
    )
    assert resp.status_code == 403
    resp = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {api_access_key2}{api_secret_key2}"},
    )
    assert resp.status_code == 403

    monkeypatch.setattr(
        settings,
        "APPLICATION_APIKEYS",
        types.ApplicationAPIKeys(
            f"{api_access_key1}{api_secret_key1}:{account_id1}:{account_login1},{api_access_key2}{api_secret_key2}:{account_id2}:{account_login2}",
        ),
    )

    resp = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {api_access_key1}{api_secret_key1}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "account_scope": {"id": 12345, "login": "login1"},
        "id": utils.ANY_UUID4,
        "name": "on-premise-app-from-env",
    }

    resp = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {api_access_key2}{api_secret_key2}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "account_scope": {"id": 67891, "login": "login2"},
        "id": utils.ANY_UUID4,
        "name": "on-premise-app-from-env",
    }
