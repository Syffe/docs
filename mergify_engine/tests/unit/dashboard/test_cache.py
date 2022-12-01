import json

import httpx

from mergify_engine import config
from mergify_engine.dashboard import subscription


async def test_tokens_cache_delete(web_client: httpx.AsyncClient) -> None:
    owner_id = 123
    headers = {"Authorization": f"Bearer {config.DASHBOARD_TO_ENGINE_API_KEY}"}
    reply = await web_client.delete(f"/tokens-cache/{owner_id}", headers=headers)
    assert reply.status_code == 200
    assert reply.content == b"Cache cleaned"


async def test_subscription_cache_delete(web_client: httpx.AsyncClient) -> None:
    owner_id = 123
    headers = {"Authorization": f"Bearer {config.DASHBOARD_TO_ENGINE_API_KEY}"}
    reply = await web_client.delete(f"/subscription-cache/{owner_id}", headers=headers)
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
        "Authorization": f"Bearer {config.DASHBOARD_TO_ENGINE_API_KEY}",
        "Content-Type": f"application/json; charset={charset}",
    }
    reply = await web_client.put(
        f"/subscription-cache/{owner_id}", content=data, headers=headers
    )
    assert reply.status_code == 200
    assert reply.content == b"Cache updated"
