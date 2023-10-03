from base64 import encodebytes

import pytest
import respx
import sqlalchemy

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.models.github import user as github_user
from mergify_engine.tests import conftest


async def prepare_respx_mock(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    permission: github_types.GitHubRepositoryPermissionLiteral,
    web_client: conftest.CustomTestClient,
    will_access_to_repo: bool,
) -> github_user.GitHubUser:
    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    api_user = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(42),
            "login": github_types.GitHubLogin("user-login"),
            "type": "User",
            "avatar_url": "",
        }
    )
    respx_mock.get(
        "https://api.github.com/repos/user-login/engine/collaborators/user-login/permission"
    ).respond(200, json={"user": api_user, "permission": permission})

    config = """
queue_rules:
  - name: main
    merge_conditions: []
"""
    if will_access_to_repo:
        respx_mock.get(
            "https://api.github.com/repos/user-login/engine/contents/.mergify.yml?ref=main"
        ).respond(
            200,
            json=github_types.GitHubContentFile(  # type: ignore[arg-type]
                type="file",
                content=encodebytes(config.encode()).decode(),
                sha=github_types.SHAType("azertyuiop"),
                path=github_types.GitHubFilePath("whatever"),
            ),
        )

    respx_mock.get("https://api.github.com/users/Mergifyio/installation").respond(
        200, json={"account": api_user}
    )
    respx_mock.get("https://api.github.com/repos/Mergifyio/engine").respond(
        200,
        json=github_types.GitHubRepository(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubRepositoryIdType(123),
                "private": False,
                "archived": False,
                "name": github_types.GitHubRepositoryName("engine"),
                "full_name": "Mergifyio/engine",
                "url": "",
                "html_url": "",
                "default_branch": github_types.GitHubRefType("main"),
                "owner": api_user,
            }
        ),
    )
    respx_mock.get("http://localhost:5000/engine/subscription/42").respond(
        200,
        json={
            "subscription_active": True,
            "subscription_reason": "",
            "features": [
                "private_repository",
                "public_repository",
                "priority_queues",
                "custom_checks",
                "random_request_reviews",
                "merge_bot_account",
                "queue_action",
                "depends_on",
                "show_sponsor",
                "dedicated_worker",
                "advanced_monitoring",
                "queue_freeze",
                "eventlogs_short",
                "eventlogs_long",
                "merge_queue_stats",
            ],
        },
    )
    return user


@pytest.mark.parametrize(
    "permission,expected_status_code", (("write", 200), ("read", 200), ("none", 403))
)
async def test_engine_proxy_get_queue_freeze(
    redis_links: redis_utils.RedisLinks,  # FIXME(sileht): this fixture should be autouse to always cleanup redis
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    permission: github_types.GitHubRepositoryPermissionLiteral,
    expected_status_code: int,
) -> None:
    user = await prepare_respx_mock(
        db, respx_mock, permission, web_client, expected_status_code != 403
    )

    url = "/front/proxy/engine/v1/repos/Mergifyio/engine/queues/freezes"
    response = await web_client.get(url)
    assert response.status_code == 403

    await web_client.log_as(user.id)
    response = await web_client.get(url)
    assert response.status_code == expected_status_code, response.text


@pytest.mark.parametrize(
    "permission,expected_status_code", (("write", 200), ("read", 403), ("none", 403))
)
async def test_engine_proxy_update_queue_freeze(
    redis_links: redis_utils.RedisLinks,  # FIXME(sileht): this fixture should be autouse to always cleanup redis
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    permission: github_types.GitHubRepositoryPermissionLiteral,
    expected_status_code: int,
) -> None:
    user = await prepare_respx_mock(
        db, respx_mock, permission, web_client, expected_status_code != 403
    )

    url = "/front/proxy/engine/v1/repos/Mergifyio/engine/queue/main/freeze"
    response = await web_client.put(url, json={"reason": "stop", "cascading": True})
    assert response.status_code == 403, response.text

    await web_client.log_as(user.id)
    response = await web_client.put(url, json={"reason": "stop", "cascading": True})
    assert response.status_code == expected_status_code, response.text


@pytest.mark.parametrize(
    "permission,expected_status_code,expected_json",
    (
        ("write", 404, {"detail": 'The queue "main" is not currently frozen.'}),
        ("read", 403, {"detail": "Forbidden"}),
        ("none", 403, {"detail": "Forbidden"}),
    ),
)
async def test_engine_proxy_delete_queue_freeze(
    redis_links: redis_utils.RedisLinks,  # FIXME(sileht): this fixture should be autouse to always cleanup redis
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    permission: github_types.GitHubRepositoryPermissionLiteral,
    expected_status_code: int,
    expected_json: dict[str, str],
) -> None:
    user = await prepare_respx_mock(
        db, respx_mock, permission, web_client, expected_status_code != 403
    )

    url = "/front/proxy/engine/v1/repos/Mergifyio/engine/queue/main/freeze"
    response = await web_client.delete(url)
    assert response.status_code == 403, response.text

    await web_client.log_as(user.id)
    response = await web_client.delete(url)
    assert response.status_code == expected_status_code
    assert response.json() == expected_json
