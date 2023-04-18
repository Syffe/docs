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
from mergify_engine import database
from mergify_engine import logs
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.models import github_user
from mergify_engine.models import manage
from mergify_engine.tests import utils as test_utils
from mergify_engine.web import root as web_root


# for jwt generation
freezegun.configure(  # type:ignore[attr-defined]
    extend_ignore_list=["mergify_engine.clients.github_app"]
)

RECORD = bool(os.getenv("MERGIFYENGINE_RECORD", False))
GITHUB_CI = utils.strtobool(os.getenv("CI", "false"))


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
def setup_new_event_loop(event_loop: asyncio.BaseEventLoop) -> None:
    # ensure each tests have a fresh event loop
    pass


@pytest.fixture(autouse=True, scope="session")
def enable_api() -> None:
    settings.API_ENABLE = True


def get_worker_id_as_int(worker_id: str) -> int:
    if not re.match(r"gw\d+", worker_id):
        return 0

    return int(worker_id.replace("gw", ""))


@pytest.fixture(autouse=True, scope="session")
def mock_redis_db_values(worker_id: str) -> abc.Generator[None, None, None]:
    worker_id_int = get_worker_id_as_int(worker_id)
    # Need to have different database for each tests to avoid breaking
    # everything in other tests.
    mocks = []
    for name, db_number in config.REDIS_AUTO_DB_SHARDING_MAPPING.items():
        new_db_number = db_number + (
            len(config.REDIS_AUTO_DB_SHARDING_MAPPING) * worker_id_int
        )
        url = settings._build_redis_url(new_db_number)
        mocks.append(mock.patch.object(settings, f"ENV_{name}", url))

    with contextlib.ExitStack() as es:
        for url_mock in mocks:
            es.enter_context(url_mock)

        yield


@pytest.fixture(autouse=True, scope="session")
def mock_postgres_db_value(worker_id: str) -> abc.Generator[None, None, None]:
    worker_id_int = get_worker_id_as_int(worker_id)
    db_name = f"postgres{worker_id_int}"
    mocked_url, mocked_url_without_db_name = test_utils.create_database_url(db_name)

    # We need to manually run the coroutine in an event loop because
    # pytest-asyncio has its own `event_loop` fixture that is function scoped and
    # in autouse (session scope fixture cannot require function scoped fixture)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.run_until_complete(
        test_utils.create_database(mocked_url_without_db_name.geturl(), db_name)
    )
    loop.close()

    with mock.patch.object(settings, "DATABASE_URL", mocked_url):
        yield


async def reset_database() -> None:
    await manage.drop_all()
    if database.APP_STATE is not None:
        await database.APP_STATE["engine"].dispose()
        database.APP_STATE = None


@pytest.fixture
async def database_cleanup() -> abc.AsyncGenerator[None, None]:
    try:
        yield
    finally:
        await reset_database()


@pytest.fixture
async def setup_database(
    database_cleanup: None, mock_postgres_db_value: None
) -> abc.AsyncGenerator[None, None]:
    database.init_sqlalchemy("test")
    await manage.create_all()
    yield


@pytest.fixture
async def db(
    setup_database: None,
) -> abc.AsyncGenerator[sqlalchemy.ext.asyncio.AsyncSession, None]:
    async with database.create_session() as session:
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
async def redis_stats(
    redis_links: redis_utils.RedisLinks,
) -> redis_utils.RedisStats:
    return redis_links.stats


@pytest.fixture()
async def github_server(
    monkeypatch: pytest.MonkeyPatch,
) -> abc.AsyncGenerator[respx.MockRouter, None]:
    monkeypatch.setattr(github.CachedToken, "STORAGE", {})
    async with respx.mock(base_url=settings.GITHUB_REST_API_URL) as respx_mock:
        respx_mock.post("/app/installations/12345/access_tokens").respond(
            200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"}
        )
        yield respx_mock


log_as_router = fastapi.APIRouter()


@log_as_router.post("/log-as/{user_id}")
async def log_as(request: fastapi.Request, user_id: int) -> fastapi.Response:
    async with database.create_session() as session:
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

    return fastapi.Response(status_code=400, content=f"user id `{user_id}` invalid")


@log_as_router.get("/logged-as")
async def logged_as(request: fastapi.Request) -> fastapi.Response:
    if request.auth.is_authenticated:
        return fastapi.responses.JSONResponse({"login": request.auth.user.login})

    raise fastapi.HTTPException(401)


class CustomTestClient(httpx.AsyncClient):
    def __init__(self, app: abc.Callable[..., typing.Any]):
        super().__init__(
            base_url=settings.DASHBOARD_UI_FRONT_URL,
            app=app,
            follow_redirects=True,
            headers={"Content-type": "application/json"},
        )

    def get_root_app(self) -> fastapi.FastAPI:
        self._transport: httpx.ASGITransport
        return typing.cast(fastapi.FastAPI, self._transport.app)

    def get_front_app(self) -> fastapi.FastAPI:
        web_app = self.get_root_app()
        for route in web_app.routes:
            if isinstance(route, starlette.routing.Mount) and route.path == "/front":
                return typing.cast(fastapi.FastAPI, route.app)
        else:
            raise RuntimeError("/front app not found")

    async def log_as(self, user_id: int) -> None:
        resp = await self.post(f"/front/for-testing/log-as/{user_id}")
        if resp.status_code != 200:
            raise Exception(resp.text)  # noqa: TRY002

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
    app = web_root.create_app(https_only=False, debug=True)

    async with asgi_lifespan.LifespanManager(app):
        yield app


@pytest.fixture
async def web_client(
    web_server: fastapi.FastAPI,
) -> abc.AsyncGenerator[httpx.AsyncClient, None]:
    async with CustomTestClient(app=web_server) as client:
        client.get_front_app().include_router(log_as_router, prefix="/for-testing")
        yield client


@pytest.fixture
def logging_reset() -> abc.Generator[None, None, None]:
    root_logger = logging.getLogger()
    saved_loggers = root_logger.manager.loggerDict
    saved_handlers = root_logger.handlers
    saved_filters = root_logger.filters
    root_logger.handlers = []
    root_logger.filters = []
    root_logger.manager.loggerDict = {}
    try:
        yield
    finally:
        root_logger.manager.loggerDict = saved_loggers
        root_logger.filters = saved_filters
        root_logger.handlers = saved_handlers


@pytest.hookimpl(trylast=True)  # type: ignore[misc]
def pytest_configure(config: pytest.Config) -> None:
    logging_plugin = config.pluginmanager.get_plugin("logging-plugin")
    if logging_plugin:
        logging_plugin.report_handler.setFormatter(logs.CUSTOM_FORMATTER)
