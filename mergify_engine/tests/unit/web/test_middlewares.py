from unittest import mock

import asgi_lifespan
import fastapi
import pytest
import starlette

from mergify_engine.middlewares import logging
from mergify_engine.tests import conftest
from mergify_engine.web import root as web_root


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


async def test_heroku_proxying() -> None:
    app = web_root.create_app(https_only=False, debug=True)
    app.add_middleware(FakeHTTPSHerokuProxyMiddleware)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get("/testing-heroku-headers")
            r.raise_for_status()
            assert r.json()["scheme"] == "https"
            assert (
                r.json()["url"]
                == "https://dashboard.mergify.com/testing-heroku-headers"
            )


async def test_http_redirect_to_https() -> None:
    app = web_root.create_app(https_only=True, debug=True)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get(
                "http://dashboard.mergify.com/testing-heroku-headers",
                follow_redirects=False,
            )
            assert r.status_code == 307, r.text
            assert "Location" in r.headers
            assert (
                r.headers["Location"]
                == "https://dashboard.mergify.com/testing-heroku-headers"
            )


async def test_loggin_middleware_error() -> None:
    app = mock.Mock()
    fake_middleware = mock.AsyncMock()
    fake_middleware.side_effect = Exception("boom")
    middleware = logging.LoggingMiddleware(app)

    req = mock.Mock(headers={})
    with pytest.raises(Exception, match="boom"):
        await middleware.dispatch(req, fake_middleware)
