import respx

from mergify_engine.tests import conftest
from mergify_engine.web.front.proxy import sentry


async def test_sentry_tunneling(
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(
        f"https://{sentry.SENTRY_HOST}/api/{sentry.ALLOWED_PROJECT_IDS[0]}/envelope/",
    ).respond(200)

    response = await web_client.post(
        "/front/proxy/sentry/",
        content="""{"dsn": "https://5a6db64c2ac246ec97b85af6ba917104@o162266.ingest.sentry.io/1506419"}

a lot of traces we should not parse!!!
a lot of traces we should not parse!!!
a lot of traces we should not parse!!!
a lot of traces we should not parse!!!
""",
    )

    assert response.status_code == 200
