import logging

import asgi_lifespan
import fastapi
import fastapi.testclient
import pytest
import starlette
import starlette.middleware.sessions

from mergify_engine.middlewares.logging import LoggingMiddleware
from mergify_engine.middlewares.security import SecurityMiddleware
from mergify_engine.middlewares.sudo import SudoMiddleware
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


@pytest.mark.parametrize(
    "status_code,log_level",
    ((0, logging.ERROR), (500, logging.ERROR), (200, logging.INFO)),
)
async def test_logging_middleware(
    status_code: int, log_level: int, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = fastapi.FastAPI(debug=True)
    app.add_middleware(LoggingMiddleware)

    @app.get("/")
    def root() -> starlette.responses.PlainTextResponse:
        if status_code == 0:
            raise Exception("boom")  # noqa: TRY002

        return starlette.responses.PlainTextResponse(
            content="", status_code=status_code, headers={"see": "me"}
        )

    client = fastapi.testclient.TestClient(app, raise_server_exceptions=False)
    response = client.get(
        "/", headers={"authorization": "should-be-hidden", "see": "me"}
    )
    if status_code == 0:
        assert response.status_code == 500
    else:
        assert response.status_code == status_code
        assert response.headers["see"] == "me"

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.msg == "request"
    assert record.levelno == log_level
    assert "authorization" not in record.request["headers"]  # type: ignore[attr-defined]
    assert b"authorization" not in record.request["headers"]  # type: ignore[attr-defined]
    assert record.response["status"] == status_code  # type: ignore[attr-defined]
    if status_code == 0:
        assert record.exc_info
        assert record.exc_info[1]
        assert record.exc_info[1].args[0] == "boom"
    else:
        assert record.exc_info is None
        assert ("see", "me") in record.response["headers"]  # type: ignore[attr-defined]


async def test_sudo_middleware() -> None:
    app = fastapi.FastAPI(debug=True)
    app.add_middleware(SudoMiddleware)
    app.add_middleware(
        starlette.middleware.sessions.SessionMiddleware, secret_key="foobar"
    )

    @app.get("/")
    def root(request: fastapi.Request) -> starlette.responses.PlainTextResponse:
        request.session["sudoGrantedTo"] = "me"
        return starlette.responses.PlainTextResponse(content="", status_code=200)

    client = fastapi.testclient.TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["Mergify-Sudo-Granted-To"] == "me"


async def test_security_middleware() -> None:
    app = fastapi.FastAPI(debug=True)
    app.add_middleware(SecurityMiddleware)

    @app.get("/")
    def root(request: fastapi.Request) -> starlette.responses.PlainTextResponse:
        return starlette.responses.PlainTextResponse(content="", status_code=200)

    client = fastapi.testclient.TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers == {
        "content-length": "0",
        "content-type": "text/plain; charset=utf-8",
        "strict-transport-security": "max-age=2592000",
        "content-security-policy": "default-src 'none'",
        "x-frame-options": "DENY",
        "x-content-type-options": "nosniff",
        "referrer-policy": "no-referrer",
        "permissions-policy": "accelerometer=(),ambient-light-sensor=(),attribution-reporting=(),autoplay=(),battery=(),camera=(),clipboard-read=(),clipboard-write=(),conversion-measurement=(),cross-origin-isolated=(),direct-sockets=(),display-capture=(),document-domain=(),encrypted-media=(),execution-while-not-rendered=(),execution-while-out-of-viewport=(),focus-without-user-activation=(),fullscreen=(),gamepad=(),geolocation=(),gyroscope=(),hid=(),idle-detection=(),interest-cohort=(),magnetometer=(),microphone=(),midi=(),navigation-override=(),otp-credentials=(),payment=(),picture-in-picture=(),publickey-credentials-get=(),screen-wake-lock=(),serial=(),shared-autofill=(),speaker-selection=(),storage-access-api=(),sync-script=(),sync-xhr=(),trust-token-redemption=(),usb=(),vertical-scroll=(),wake-lock=(),web-share=(),window-placement=(),xr-spatial-tracking=()",
    }
