import httpx

from mergify_engine import settings


async def test_legacy_badge_endpoint(web_client: httpx.AsyncClient) -> None:
    reply = await web_client.get(
        "/badges/mergifyio/mergify-engine.png", follow_redirects=False
    )
    assert reply.status_code == 302
    assert reply.headers["Location"] == (
        "https://img.shields.io/endpoint.png"
        f"?url={settings.SUBSCRIPTION_URL}/badges/mergifyio/mergify-engine&style=flat"
    )

    reply = await web_client.get(
        "/badges/mergifyio/mergify-engine.svg", follow_redirects=False
    )
    assert reply.status_code == 302
    assert reply.headers["Location"] == (
        "https://img.shields.io/endpoint.svg"
        f"?url={settings.SUBSCRIPTION_URL}/badges/mergifyio/mergify-engine&style=flat"
    )

    reply = await web_client.get(
        "/badges/mergifyio/mergify-engine", follow_redirects=False
    )
    assert reply.headers["Location"] == (
        f"{settings.SUBSCRIPTION_URL}/badges/mergifyio/mergify-engine"
    )
