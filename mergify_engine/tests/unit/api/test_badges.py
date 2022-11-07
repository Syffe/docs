import httpx

from mergify_engine import config


async def test_api_badge(web_client: httpx.AsyncClient) -> None:
    reply = await web_client.get(
        "/v1/badges/mergifyio/mergify-engine.png", follow_redirects=False
    )
    assert reply.status_code == 302
    assert reply.headers["Location"] == (
        "https://img.shields.io/endpoint.png"
        f"?url={config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine&style=flat"
    )

    reply = await web_client.get(
        "/v1/badges/mergifyio/mergify-engine.svg", follow_redirects=False
    )
    assert reply.status_code == 302
    assert reply.headers["Location"] == (
        "https://img.shields.io/endpoint.svg"
        f"?url={config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine&style=flat"
    )

    reply = await web_client.get(
        "/v1/badges/mergifyio/mergify-engine", follow_redirects=False
    )
    assert reply.headers["Location"] == (
        f"{config.SUBSCRIPTION_BASE_URL}/badges/mergifyio/mergify-engine"
    )
