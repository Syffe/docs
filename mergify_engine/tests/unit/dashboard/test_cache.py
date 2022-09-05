import json

from starlette import testclient

from mergify_engine import config
from mergify_engine import utils
from mergify_engine.dashboard import subscription
from mergify_engine.web import root


def test_tokens_cache_delete() -> None:
    owner_id = 123
    headers = {"Authorization": f"Bearer {config.DASHBOARD_TO_ENGINE_API_KEY}"}
    with testclient.TestClient(root.app) as client:
        reply = client.delete(f"/tokens-cache/{owner_id}", headers=headers)
        assert reply.status_code == 200
        assert reply.content == b"Cache cleaned"


def test_subscription_cache_delete() -> None:
    owner_id = 123
    headers = {"Authorization": f"Bearer {config.DASHBOARD_TO_ENGINE_API_KEY}"}
    with testclient.TestClient(root.app) as client:
        reply = client.delete(f"/subscription-cache/{owner_id}", headers=headers)
        assert reply.status_code == 200
        assert reply.content == b"Cache cleaned"


def test_subscription_cache_update() -> None:
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
    with testclient.TestClient(root.app) as client:
        reply = client.put(
            f"/subscription-cache/{owner_id}", data=data, headers=headers
        )
        assert reply.status_code == 200
        assert reply.content == b"Cache updated"


def test_legacy_authentication() -> None:
    owner_id = 123

    data = b"azerty"
    headers = {
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data, config.WEBHOOK_SECRET)}",
    }
    with testclient.TestClient(root.app) as client:
        reply = client.delete(f"/tokens-cache/{owner_id}", data=data, headers=headers)
        assert reply.status_code == 200
        assert reply.content == b"Cache cleaned"
