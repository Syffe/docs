import fastapi
import pytest
import starlette

from mergify_engine.tests import conftest


router = fastapi.APIRouter()


@router.get("/testing-heroku-headers")
def _test_heroku_headers(request: fastapi.Request) -> fastapi.Response:
    return fastapi.responses.JSONResponse(
        {"scheme": request.scope["scheme"], "url": str(request.url)}
    )


class FakeHTTPSHerokuProxyMiddleware:
    def __init__(self, app: starlette.types.ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ) -> None:
        if scope["type"] == "http":
            heroku_headers = {
                k.lower().encode("latin-1"): v.encode("latin-1")
                for k, v in {
                    "X-Forwarded-For": "10.10.123.123",
                    "X-Forwarded-Proto": "https",
                    "X-Forwarded-Port": "443",
                    "X-Request-Start": "1656316515",
                    "X-Request-Id": "azertyu",
                    "Via": "192.158.1.38:8763",
                    "Host": "dashboard.mergify.com",
                }.items()
            }
            names = list(heroku_headers.keys())
            scope["headers"] = [
                header for header in scope["headers"] if header[0] not in names
            ]
            scope["headers"].extend(heroku_headers.items())

        await self.app(scope, receive, send)


async def test_heroku_proxying(web_client: conftest.CustomTestClient) -> None:
    app = web_client.get_root_app()
    app.add_middleware(FakeHTTPSHerokuProxyMiddleware)
    app.include_router(router)
    r = await web_client.get("/testing-heroku-headers")
    r.raise_for_status()
    assert r.json()["scheme"] == "https"
    assert r.json()["url"] == "https://dashboard.mergify.com/testing-heroku-headers"


@pytest.mark.parametrize("web_server", (True,), indirect=["web_server"])
async def test_http_redirect_to_https(
    web_client: conftest.CustomTestClient,
) -> None:
    app = web_client.get_root_app()
    app.include_router(router)
    r = await web_client.get(
        "http://dashboard.mergify.com/testing-heroku-headers", follow_redirects=False
    )
    assert r.status_code == 307
    assert "Location" in r.headers
    assert (
        r.headers["Location"] == "https://dashboard.mergify.com/testing-heroku-headers"
    )
