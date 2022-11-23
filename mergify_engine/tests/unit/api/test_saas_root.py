import httpx


async def test_saas_api_root(web_client: httpx.AsyncClient) -> None:
    reply = await web_client.get("/")
    assert reply.status_code == 200
    assert reply.json() == {}
