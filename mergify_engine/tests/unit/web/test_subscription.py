import json

import httpx
import sqlalchemy

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.dashboard import subscription
from mergify_engine.models import github_user
from mergify_engine.tests import conftest


async def test_tokens_cache_delete(web_client: httpx.AsyncClient) -> None:
    owner_id = 123
    headers = {
        "Authorization": f"Bearer {settings.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value()}"
    }
    reply = await web_client.delete(
        f"/subscriptions/tokens-cache/{owner_id}", headers=headers
    )
    assert reply.status_code == 200
    assert reply.content == b"Cache cleaned"


async def test_subscription_cache_delete(web_client: httpx.AsyncClient) -> None:
    owner_id = 123
    headers = {
        "Authorization": f"Bearer {settings.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value()}"
    }
    reply = await web_client.delete(
        f"/subscriptions/subscription-cache/{owner_id}", headers=headers
    )
    assert reply.status_code == 200
    assert reply.content == b"Cache cleaned"


async def test_subscription_cache_update(web_client: httpx.AsyncClient) -> None:
    owner_id = 123
    charset = "utf-8"

    data = json.dumps(
        subscription.SubscriptionDict(
            {
                "subscription_reason": "Customer",
                "features": [],
            }
        )
    ).encode(charset)
    headers = {
        "Authorization": f"Bearer {settings.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value()}",
        "Content-Type": f"application/json; charset={charset}",
    }
    reply = await web_client.put(
        f"/subscriptions/subscription-cache/{owner_id}", content=data, headers=headers
    )
    assert reply.status_code == 200
    assert reply.content == b"Cache updated"


async def test_subscription_user_oauth_token_unauthororized(
    web_client: conftest.CustomTestClient,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    reply = await web_client.get("/subscriptions/user-oauth-access-token/42")
    assert reply.status_code == 403


async def test_subscription_user_oauth_token(
    web_client: conftest.CustomTestClient,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    web_client.headers[
        "Authorization"
    ] = f"Bearer {settings.DASHBOARD_TO_ENGINE_API_KEY.get_secret_value()}"

    reply = await web_client.get("/subscriptions/user-oauth-access-token/42")
    assert reply.status_code == 404

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    reply = await web_client.get("/subscriptions/user-oauth-access-token/42")
    assert reply.status_code == 200
    assert reply.json() == {"oauth_access_token": "user-token"}
