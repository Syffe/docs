from starlette import testclient

from mergify_engine import config
from mergify_engine.web import root


def test_legacy_badge_endpoint() -> None:
    with testclient.TestClient(root.app) as client:
        reply = client.get(
            "/badges/mergifyio/mergify-engine.png", allow_redirects=False
        )
        assert reply.status_code == 302
        assert reply.headers["Location"] == (
            "https://img.shields.io/endpoint.png"
            f"?url={config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine&style=flat"
        )

        reply = client.get(
            "/badges/mergifyio/mergify-engine.svg", allow_redirects=False
        )
        assert reply.status_code == 302
        assert reply.headers["Location"] == (
            "https://img.shields.io/endpoint.svg"
            f"?url={config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine&style=flat"
        )

        reply = client.get("/badges/mergifyio/mergify-engine", allow_redirects=False)
        assert reply.headers["Location"] == (
            f"{config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine"
        )
