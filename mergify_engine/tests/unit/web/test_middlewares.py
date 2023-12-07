import logging

import asgi_lifespan
import fastapi
import fastapi.testclient
import pydantic
import pytest
import starlette
import starlette.middleware.sessions

from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.middlewares.content_length import ContentLengthMiddleware
from mergify_engine.middlewares.logging import LoggingMiddleware
from mergify_engine.middlewares.saas_addons import SaasSecurityMiddleware
from mergify_engine.middlewares.security import SecurityMiddleware
from mergify_engine.middlewares.sudo import SudoMiddleware
from mergify_engine.tests import conftest
from mergify_engine.web import root as web_root


router = fastapi.APIRouter()


@router.get("/testing-route", name="easy-route")
def _testing_route(request: fastapi.Request) -> fastapi.Response:
    return fastapi.responses.JSONResponse(
        {
            "scheme": request.scope["scheme"],
            "url": str(request.url),
            "url_for": str(request.url_for("easy-route")),
        },
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


async def test_heroku_proxying(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "HTTP_TO_HTTPS_REDIRECT", False)
    app = web_root.create_app(debug=True)
    app.add_middleware(FakeHTTPSHerokuProxyMiddleware)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get("/testing-route")
            r.raise_for_status()
            assert r.json()["scheme"] == "https"
            assert r.json()["url"] == "https://dashboard.mergify.com/testing-route"


async def test_http_redirect_to_https(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "HTTP_TO_HTTPS_REDIRECT", True)
    app = web_root.create_app(debug=True)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get(
                "http://dashboard.mergify.com/testing-route",
                follow_redirects=False,
            )
            assert r.status_code == 307, r.text
            assert "Location" in r.headers
            assert (
                r.headers["Location"] == "https://dashboard.mergify.com/testing-route"
            )


@pytest.mark.ignored_logging_errors("request")
@pytest.mark.parametrize(
    ("status_code", "log_level"),
    ((0, logging.ERROR), (200, logging.INFO)),
)
async def test_logging_middleware(
    status_code: int,
    log_level: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = fastapi.FastAPI(debug=True)
    app.add_middleware(LoggingMiddleware)

    @app.get("/")
    def root() -> starlette.responses.PlainTextResponse:
        if status_code == 0:
            raise Exception("boom")  # noqa: TRY002

        return starlette.responses.PlainTextResponse(
            content="",
            status_code=status_code,
            headers={"see": "me"},
        )

    client = fastapi.testclient.TestClient(app, raise_server_exceptions=False)
    response = client.get(
        "/",
        headers={"authorization": "should-be-hidden", "see": "me"},
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
        starlette.middleware.sessions.SessionMiddleware,
        secret_key="foobar",
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


async def test_without_trusted_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "HTTP_TO_HTTPS_REDIRECT", False)
    app = web_root.create_app(debug=True)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get("/testing-route", headers={"Host": "hacker.com"})
            r.raise_for_status()
            assert r.json()["url_for"] == "http://hacker.com/testing-route"


async def test_with_trusted_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "HTTP_TO_HTTPS_REDIRECT", False)
    monkeypatch.setattr(
        settings,
        "HTTP_TRUSTED_HOSTS",
        types.StrListFromStrWithComma(["*.mergify.com"]),
    )

    app = web_root.create_app(debug=True)
    app.include_router(router)
    app.router.routes.insert(0, app.router.routes.pop(-1))

    async with asgi_lifespan.LifespanManager(app):
        async with conftest.CustomTestClient(app=app) as client:
            r = await client.get("/testing-route", headers={"Host": "api.mergify.com"})
            r.raise_for_status()
            assert r.json()["url_for"] == "http://api.mergify.com/testing-route"

            r = await client.get("/testing-route", headers={"Host": "hacker.com"})
            assert r.status_code == 400
            assert r.text == "Invalid host header"


async def test_content_length_middleware() -> None:
    app = fastapi.FastAPI(debug=True)
    app.add_middleware(
        ContentLengthMiddleware,
        default_max_content_size=10,
        endpoints_max_content_size={("POST", "/foo"): 60},
    )

    @app.post("/")
    @app.post("/foo")
    async def root(request: fastapi.Request) -> starlette.responses.PlainTextResponse:
        return starlette.responses.PlainTextResponse(
            content=(await request.body()).decode(),
            status_code=200,
        )

    client = fastapi.testclient.TestClient(app)

    # Too big body with wrong content-length
    response = client.post(
        "/",
        json={"fooooooooooo": "barrrrrrrrrrrrrrr"},
        headers={"Content-Length": "3"},
    )
    assert response.status_code == 413

    # Too big content-length
    response = client.post("/", json={}, headers={"Content-Length": "30000"})
    assert response.status_code == 413

    # Too big content-length for special endpoint
    response = client.post("/foo", json={}, headers={"Content-Length": "30"})
    assert response.status_code == 200
    response = client.post("/foo", json={}, headers={"Content-Length": "70"})
    assert response.status_code == 413

    # Invalid content-length
    response = client.post("/", json={}, headers={"Content-Length": "foobar"})
    assert response.status_code == 411

    # Looks good
    response = client.post(
        "/",
        json={"f": "b"},
    )
    assert response.status_code == 200


async def test_saas_addons_middleware(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "HTTP_CF_TO_MERGIFY_SECRET",
        pydantic.SecretStr("so-secret"),
    )
    monkeypatch.setattr(settings, "HTTP_CF_TO_MERGIFY_HOSTS", ["allowed.example.com"])
    monkeypatch.setattr(settings, "HTTP_GITHUB_TO_MERGIFY_HOST", "github.example.com")

    app = fastapi.FastAPI(debug=True)
    app.add_middleware(SaasSecurityMiddleware)

    @app.get("/")
    async def root(request: fastapi.Request) -> starlette.responses.PlainTextResponse:
        return starlette.responses.PlainTextResponse(content="hello", status_code=200)

    client = fastapi.testclient.TestClient(app)

    cf_headers = {"X-Mergify-CF-secret": "so-secret"}
    github_headers = {"X-Hub-Signature": "sha1=foobar"}

    def check_request(unexpected_status_code: int) -> None:
        # Good API host, good secret
        response = client.get("http://allowed.example.com/", headers=cf_headers)
        assert response.status_code == 200

        # Good API host, wrong secret
        response = client.get("http://allowed.example.com/")
        assert response.status_code == unexpected_status_code

        # wrong API host, good secret
        response = client.get("http://hacker.example.com/", headers=cf_headers)
        assert response.status_code == unexpected_status_code

        # wrong API host, wrong secret
        response = client.get("http://hacker.example.com/")
        assert response.status_code == unexpected_status_code

        # Good GitHub host, good header
        response = client.get("http://github.example.com/", headers=github_headers)
        assert response.status_code == 200

        # Good gitHub host, wrong header
        response = client.get("http://github.example.com/")
        assert response.status_code == unexpected_status_code

        # wrong GitHub host, good header
        response = client.get("http://hacker.example.com/", headers=github_headers)
        assert response.status_code == unexpected_status_code

        # wrong GitHub host, wrong header
        response = client.get(
            "http://hacker.example.com/",
            headers={"X-Hub-Signature": "noway-it-works"},
        )
        assert response.status_code == unexpected_status_code

    check_request(200)

    monkeypatch.setattr(settings, "HTTP_SAAS_SECURITY_ENFORCE", True)
    check_request(542)
