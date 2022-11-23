import respx
import sqlalchemy

from mergify_engine import github_types
from mergify_engine.models import github_user
from mergify_engine.tests import conftest


async def test_engine_proxy(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
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
    respx_mock.get("https://api.github.com/users/Mergifyio/installation").respond(
        200, json={"account": api_user}
    )
    respx_mock.get("https://api.github.com/repos/Mergifyio/engine").respond(
        200,
        json=github_types.GitHubRepository(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubRepositoryIdType(123),
                "private": True,
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
    url = "/front/proxy/engine/v1/repos/Mergifyio/engine/queues/freezes"

    response = await web_client.get(url)
    assert response.status_code == 403

    await web_client.log_as(user.id)
    response = await web_client.get(url)
    assert response.status_code == 200, response.text
