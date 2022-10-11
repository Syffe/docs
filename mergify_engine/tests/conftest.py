import asyncio
from collections import abc
import contextlib
import functools
import logging
import os
import re
import typing
from unittest import mock

import asgi_lifespan
import fastapi
import freezegun
import freezegun.api
import httpx
import imia
import msgpack
import pytest
import respx
import sqlalchemy
import starlette

from mergify_engine import config
from mergify_engine import logs
from mergify_engine import models
from mergify_engine import redis_utils
from mergify_engine.clients import github
from mergify_engine.models import github_user
from mergify_engine.models import manage
from mergify_engine.web import root as web_root


# for jwt generation
freezegun.configure(  # type:ignore[attr-defined]
    extend_ignore_list=["mergify_engine.clients.github_app"]
)


def msgpack_freezegun_fixes(obj: typing.Any) -> typing.Any:
    # NOTE(sileht): msgpack isinstance check doesn't support override of
    # __instancecheck__ like freezegun does, so we convert the fake one into a
    # real one
    if isinstance(obj, freezegun.api.FakeDatetime):  # type: ignore[attr-defined]
        return freezegun.api.real_datetime(  # type: ignore[attr-defined]
            obj.year,
            obj.month,
            obj.day,
            obj.hour,
            obj.minute,
            obj.second,
            obj.microsecond,
            obj.tzinfo,
        )
    return obj


# serialize freezegum FakeDatetime as datetime
msgpack.packb = functools.partial(msgpack.packb, default=msgpack_freezegun_fixes)

original_os_environ = os.environ.copy()


@pytest.fixture()
def original_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> abc.Generator[None, None, None]:
    current = os.environ.copy()
    os.environ.clear()
    os.environ.update(original_os_environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(current)


@pytest.fixture()
def logger_checker(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> abc.Generator[None, None, None]:
    # daiquiri removes all handlers during setup, as we want to sexy output and the pytest
    # capability at the same, we must add back the pytest handler
    logs.setup_logging()
    logging.getLogger(None).addHandler(caplog.handler)
    yield

    whens: tuple[typing.Literal["setup", "call", "teardown"], ...] = (
        "setup",
        "call",
        "teardown",
    )
    for when in whens:
        messages = [
            rec.getMessage()
            for rec in caplog.get_records(when)
            if rec.levelname in ("CRITICAL", "ERROR")
        ]
        assert [] == messages


@pytest.fixture(autouse=True)
def setup_new_event_loop() -> None:
    # ensure each tests have a fresh event loop
    asyncio.set_event_loop(asyncio.new_event_loop())


@pytest.fixture(autouse=True, scope="session")
def enable_api() -> None:
    config.API_ENABLE = True


CONFIG_URLS_TO_MOCK = (
    "LEGACY_CACHE_URL",
    "STREAM_URL",
    "QUEUE_URL",
    "TEAM_MEMBERS_CACHE_URL",
    "TEAM_PERMISSIONS_CACHE_URL",
    "USER_PERMISSIONS_CACHE_URL",
    "EVENTLOGS_URL",
    "ACTIVE_USERS_URL",
    "STATISTICS_URL",
    "AUTHENTICATION_URL",
)


@pytest.fixture(autouse=True)
def mock_redis_db_values(worker_id: str) -> abc.Generator[None, None, None]:
    # Need to have different database for each tests to avoid breaking
    # everything in other tests.
    if not re.match(r"gw\d+", worker_id):
        worker_id_int = 0
    else:
        worker_id_int = int(worker_id.replace("gw", ""))

    mocks = []
    for config_url_to_mock in CONFIG_URLS_TO_MOCK:
        config_url = getattr(config, config_url_to_mock)
        db_number_re = re.search(r"\?db=(\d+)", config_url)
        if db_number_re is None:
            raise RuntimeError(
                (
                    f"Expected to find `?db=` at the end of config URL '{config_url_to_mock}', "
                    f"got '{config_url}' instead"
                )
            )
        db_number = int(db_number_re.group(1))
        new_db_number = db_number + (len(CONFIG_URLS_TO_MOCK) * worker_id_int)

        mocks.append(
            mock.patch.object(
                config,
                config_url_to_mock,
                re.sub(r"\?db=\d+", f"?db={new_db_number}", config_url),
            )
        )

    with contextlib.ExitStack() as es:
        for url_mock in mocks:
            es.enter_context(url_mock)

        yield


@pytest.fixture(autouse=True)
async def setup_database() -> abc.AsyncGenerator[None, None]:
    models.init_sqlalchemy()
    await manage.create_all()
    try:
        yield
    finally:
        await manage.drop_all()
        models.APP_STATE = None


@pytest.fixture
async def db(
    setup_database: None,
) -> abc.AsyncGenerator[sqlalchemy.ext.asyncio.AsyncSession, None]:
    async with models.create_session() as session:
        yield session


@pytest.fixture()
async def redis_links(
    mock_redis_db_values: typing.Any,
) -> abc.AsyncGenerator[redis_utils.RedisLinks, None]:
    links = redis_utils.RedisLinks(name="global-fixture")
    await links.flushall()
    try:
        yield links
    finally:
        await links.flushall()
        await links.shutdown_all()


@pytest.fixture()
async def redis_cache(
    redis_links: redis_utils.RedisLinks,
) -> redis_utils.RedisCache:
    return redis_links.cache


@pytest.fixture()
async def redis_stream(
    redis_links: redis_utils.RedisLinks,
) -> redis_utils.RedisStream:
    return redis_links.stream


@pytest.fixture()
async def github_server(
    monkeypatch: pytest.MonkeyPatch,
) -> abc.AsyncGenerator[respx.MockRouter, None]:
    monkeypatch.setattr(github.CachedToken, "STORAGE", {})
    async with respx.mock(base_url=config.GITHUB_REST_API_URL) as respx_mock:
        respx_mock.post("/app/installations/12345/access_tokens").respond(
            200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"}
        )
        yield respx_mock


log_as_router = fastapi.APIRouter()


@log_as_router.post("/log-as/{user_id}")  # noqa: FS003
async def log_as(request: fastapi.Request, user_id: int) -> fastapi.Response:
    async with models.create_session() as session:
        result = await session.execute(
            sqlalchemy.select(github_user.GitHubUser).where(
                github_user.GitHubUser.id == int(user_id),
                github_user.GitHubUser.oauth_access_token.isnot(None),
            )
        )

        user = typing.cast(github_user.GitHubUser, result.unique().scalar_one_or_none())

    if user:
        await imia.login_user(request, user, "whatever")
        return fastapi.Response(status_code=200)
    else:
        return fastapi.Response(status_code=400, content=f"user id `{user_id}` invalid")


@log_as_router.get("/logged-as")
async def logged_as(request: fastapi.Request) -> fastapi.Response:
    if request.auth.is_authenticated:
        return fastapi.responses.JSONResponse({"login": request.auth.user.login})
    else:
        raise fastapi.HTTPException(401)


class CustomTestClient(httpx.AsyncClient):
    def __init__(self, app: abc.Callable[..., typing.Any]):
        super().__init__(
            base_url=config.DASHBOARD_UI_FRONT_BASE_URL,
            app=app,
            follow_redirects=True,
        )

    def get_front_app(self) -> fastapi.FastAPI:
        self._transport: httpx.ASGITransport
        web_app = typing.cast(fastapi.FastAPI, self._transport.app)
        for route in web_app.routes:
            if isinstance(route, starlette.routing.Mount) and route.path == "/front":
                return typing.cast(fastapi.FastAPI, route.app)
        else:
            raise RuntimeError("/front app not found")

    async def log_as(self, user_id: int) -> None:
        resp = await self.post(f"/front/for-testing/log-as/{user_id}")
        resp.raise_for_status()

    async def logged_as(self) -> str | None:
        resp = await self.get("/front/for-testing/logged-as")
        if resp.status_code == 401:
            return None
        resp.raise_for_status()
        data = resp.json()
        return typing.cast(str, data["login"])

    async def logout(self) -> None:
        resp = await self.get("/front/auth/logout", follow_redirects=False)
        assert resp.status_code == 204


@pytest.fixture
async def web_server() -> abc.AsyncGenerator[fastapi.FastAPI, None]:
    app = web_root.create_app()
    async with asgi_lifespan.LifespanManager(app):
        yield app


@pytest.fixture
async def web_client(
    web_server: fastapi.FastAPI,
) -> abc.AsyncGenerator[httpx.AsyncClient, None]:
    async with CustomTestClient(app=web_server) as client:
        client.get_front_app().include_router(log_as_router, prefix="/for-testing")
        yield client
