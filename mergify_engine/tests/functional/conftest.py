import asyncio
import base64
from collections import abc
import contextlib
import dataclasses
import datetime
import json
import os
import shutil
import tempfile
import typing
from unittest import mock

import filelock
import httpx
import pytest
import tenacity
import vcr
import vcr.request
import vcr.stubs.urllib3_stubs

from mergify_engine import database
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.clients import http
from mergify_engine.config import types
from mergify_engine.models import github_user


RECORD = bool(os.getenv("MERGIFYENGINE_RECORD", False))
CASSETTE_LIBRARY_DIR_BASE = "zfixtures/cassettes"
DEFAULT_SUBSCRIPTION_FEATURES = (subscription.Features.PUBLIC_REPOSITORY,)

# Files used for requests synchronization when recording in parallel.
REQUESTS_SYNC_FILE_PATH = f"{tempfile.gettempdir()}/requests_timestamp"
REQUESTS_SYNC_LOCK_FILE_PATH = f"{REQUESTS_SYNC_FILE_PATH}.lock"

REQUESTS_SYNC_FILE_LOCK = filelock.FileLock(REQUESTS_SYNC_LOCK_FILE_PATH)

SHUTUPVCR = utils.strtobool(os.getenv("SHUTUPVCR", "true"))


class ShutUpVcrCannotOverwriteExistingCassetteException(Exception):
    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.cassette = kwargs["cassette"]
        self.failed_request = kwargs["failed_request"]
        message = self._get_message(kwargs["cassette"], kwargs["failed_request"])  # type: ignore[no-untyped-call]
        super().__init__(message)

    @staticmethod
    def _get_message(cassette, failed_request):  # type: ignore[no-untyped-def]
        return (
            f"Can't overwrite existing cassette ({cassette._path}) "
            f"in your current record mode ({cassette.record_mode}).\n"
            f"Missing request: {failed_request}.\n"
        )


if SHUTUPVCR:
    vcr.errors.CannotOverwriteExistingCassetteException = (
        ShutUpVcrCannotOverwriteExistingCassetteException
    )


class RecordConfigType(typing.TypedDict):
    integration_id: int
    app_user_id: github_types.GitHubAccountIdType
    app_user_login: github_types.GitHubLogin
    organization_id: github_types.GitHubAccountIdType
    organization_name: github_types.GitHubLogin
    repository_id: github_types.GitHubRepositoryIdType
    repository_name: github_types.GitHubRepositoryName
    branch_prefix: str


class DashboardFixture(typing.NamedTuple):
    api_key_admin: str
    subscription: subscription.Subscription


def get_all_subscription_features() -> frozenset[subscription.Features]:
    return frozenset(
        getattr(subscription.Features, f) for f in subscription.Features.__members__
    )


def extract_subscription_marker_features(
    marker: pytest.Mark,
) -> frozenset[subscription.Features]:
    if len(marker.args) == 0:
        return get_all_subscription_features()

    if isinstance(marker.args[0], bool):
        if marker.args[0]:
            return get_all_subscription_features()

        return frozenset(DEFAULT_SUBSCRIPTION_FEATURES)

    for feat in marker.args:
        if not isinstance(feat, subscription.Features):
            raise Exception(  # noqa: TRY002
                "Expected every arguments of `subscription` marker to be an instance of `subscription.Features`"
            )

    return frozenset(DEFAULT_SUBSCRIPTION_FEATURES + marker.args)


@pytest.fixture
async def dashboard(
    monkeypatch: pytest.MonkeyPatch,
    redis_cache: redis_utils.RedisCache,
    request: pytest.FixtureRequest,
    setup_database: None,
) -> DashboardFixture:
    is_functionaltest_class = request.cls is not None
    marker = request.node.get_closest_marker("subscription")
    if marker:
        subscription_features = extract_subscription_marker_features(marker)
    elif is_functionaltest_class and request.cls.SUBSCRIPTION_ACTIVE:
        subscription_features = get_all_subscription_features()
    else:
        subscription_features = frozenset(DEFAULT_SUBSCRIPTION_FEATURES)

    api_key_admin = "a" * 64

    sub = subscription.Subscription(
        redis_cache,
        settings.TESTING_ORGANIZATION_ID,
        "You're not nice",
        subscription_features,
        [
            typing.cast(subscription.FeaturesLiteralT, s.value)
            for s in subscription_features
        ],
    )
    await sub._save_subscription_to_cache()

    async with database.create_session() as session:
        await github_user.GitHubUser.create_or_update(
            session,
            settings.TESTING_ORG_ADMIN_ID,
            github_types.GitHubLogin("mergify-test1"),
            settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
        )
        await github_user.GitHubUser.create_or_update(
            session,
            settings.TESTING_ORG_USER_ID,
            github_types.GitHubLogin("mergify-test4"),
            settings.TESTING_ORG_USER_PERSONAL_TOKEN,
        )
        await github_user.GitHubUser.create_or_update(
            session,
            settings.TESTING_MERGIFY_TEST_2_ID,
            github_types.GitHubLogin("mergify-test2"),
            settings.TESTING_EXTERNAL_USER_PERSONAL_TOKEN,
        )

    real_get_subscription = subscription.Subscription.get_subscription

    async def fake_retrieve_subscription_from_db(
        redis_cache: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> subscription.Subscription:
        if owner_id == settings.TESTING_ORGANIZATION_ID:
            return sub
        return subscription.Subscription(
            redis_cache,
            owner_id,
            "We're just testing",
            frozenset({subscription.Features.PUBLIC_REPOSITORY}),
            ["public_repository"],
        )

    async def fake_subscription(
        redis_cache: redis_utils.RedisCache, owner_id: github_types.GitHubAccountIdType
    ) -> subscription.Subscription:
        if owner_id == settings.TESTING_ORGANIZATION_ID:
            return await real_get_subscription(redis_cache, owner_id)
        return subscription.Subscription(
            redis_cache,
            owner_id,
            "We're just testing",
            frozenset({subscription.Features.PUBLIC_REPOSITORY}),
            ["public_repository"],
        )

    patcher = mock.patch(
        "mergify_engine.subscription.Subscription._retrieve_subscription_from_db",
        side_effect=fake_retrieve_subscription_from_db,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    patcher = mock.patch(
        "mergify_engine.subscription.Subscription.get_subscription",
        side_effect=fake_subscription,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    monkeypatch.setattr(
        settings,
        "APPLICATION_APIKEYS",
        types.ApplicationAPIKeys(
            f"{api_key_admin}:{settings.TESTING_ORGANIZATION_ID}:{settings.TESTING_ORGANIZATION_NAME}",
        ),
    )

    return DashboardFixture(api_key_admin, sub)


def pyvcr_response_filter(
    response: dict[str, typing.Any]
) -> dict[str, typing.Any] | None:
    if (
        response["status_code"] in (403, 429)
        or response["status_code"] == 422
        and "abuse" in response["content"]
    ) and response["headers"].get("X-RateLimit-Remaining") is not None:
        return None

    for h in [
        "CF-Cache-Status",
        "CF-RAY",
        "Expect-CT",
        "Report-To",
        "NEL",
        "cf-request-id",
        "Via",
        "X-GitHub-Request-Id",
        "Date",
        "ETag",
        "X-RateLimit-Reset",
        "X-RateLimit-Used",
        "X-RateLimit-Resource",
        "X-RateLimit-Limit",
        "Via",
        "cookie",
        "Expires",
        "Fastly-Request-ID",
        "X-Timer",
        "X-Served-By",
        "Last-Modified",
        "X-RateLimit-Remaining",
        "X-Runtime-rack",
        "Access-Control-Allow-Origin",
        "Access-Control-Expose-Headers",
        "Cache-Control",
        "Content-Security-Policy",
        "Referrer-Policy",
        "Server",
        "Status",
        "Strict-Transport-Security",
        "Vary",
        "X-Content-Type-Options",
        "X-Frame-Options",
        "X-XSS-Protection",
    ]:
        response["headers"].pop(h, None)
    return response


def pyvcr_request_filter(request: vcr.request.Request) -> vcr.request.Request:
    if request.method == "POST" and request.path.endswith("/access_tokens"):
        return None
    return request


class RecorderFixture(typing.NamedTuple):
    config: RecordConfigType
    vcr: vcr.VCR


def cleanup_github_app_info() -> None:
    github.GitHubAppInfo._bot = None
    github.GitHubAppInfo._app = None


@pytest.fixture(autouse=True)
async def recorder(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
) -> RecorderFixture | None:
    is_unittest_class = request.cls is not None

    marker = request.node.get_closest_marker("recorder")
    if not is_unittest_class and marker is None:
        return None

    if is_unittest_class:
        cassette_library_dir = os.path.join(
            CASSETTE_LIBRARY_DIR_BASE,
            request.cls.__name__,
            request.node.name,
        )
    else:
        cassette_library_dir = os.path.join(
            CASSETTE_LIBRARY_DIR_BASE,
            request.node.module.__name__.replace(
                "mergify_engine.tests.functional.", ""
            ).replace(".", "/"),
            request.node.name,
        )

    # Recording stuffs
    if RECORD:
        if os.path.exists(cassette_library_dir):
            shutil.rmtree(cassette_library_dir)
        os.makedirs(cassette_library_dir)

    # NOTE(Kontrolix): Those are hosts that VCR will ignore. When adding one to the
    # list please add the reason why we ignore it.
    # Reasons:
    # - api.openai.com : We currently use this endpoint only for embedding and there no
    #                    is value to call it for real. It will systematicaly be mocked
    #                    in the test
    ignored_host = ["api.openai.com"]

    recorder = vcr.VCR(
        cassette_library_dir=cassette_library_dir,
        record_mode="all" if RECORD else "none",
        match_on=["method", "uri"],
        ignore_localhost=True,
        ignore_hosts=ignored_host,
        filter_headers=[
            ("Authorization", "<TOKEN>"),
            ("X-Hub-Signature", "<SIGNATURE>"),
            ("User-Agent", None),
            ("Accept-Encoding", None),
            ("Connection", None),
        ],
        before_record_response=pyvcr_response_filter,
        before_record_request=pyvcr_request_filter,
    )

    if RECORD:
        github.CachedToken.STORAGE = {}
    else:
        # Never expire token during replay
        patcher = mock.patch.object(
            github_app, "get_or_create_jwt", return_value="<TOKEN>"
        )
        patcher.start()
        request.addfinalizer(patcher.stop)
        patcher = mock.patch.object(
            github.GitHubAppInstallationAuth,
            "get_access_token",
            return_value="<TOKEN>",
        )
        patcher.start()
        request.addfinalizer(patcher.stop)

    # Let's start recording
    cassette = recorder.use_cassette("http.yaml")
    cassette.__enter__()
    request.addfinalizer(cassette.__exit__)
    record_config_file = os.path.join(cassette_library_dir, "config.json")

    if RECORD:
        mergify_bot = await github.GitHubAppInfo.get_bot(redis_links.cache)
        with open(record_config_file, "w") as f:
            f.write(
                json.dumps(
                    RecordConfigType(
                        {
                            "integration_id": settings.GITHUB_APP_ID,
                            "app_user_id": mergify_bot["id"],
                            "app_user_login": mergify_bot["login"],
                            "organization_id": settings.TESTING_ORGANIZATION_ID,
                            "organization_name": settings.TESTING_ORGANIZATION_NAME,
                            "repository_id": settings.TESTING_REPOSITORY_ID,
                            "repository_name": github_types.GitHubRepositoryName(
                                settings.TESTING_REPOSITORY_NAME
                            ),
                            "branch_prefix": date.utcnow().strftime("%Y%m%d%H%M%S"),
                        }
                    )
                )
            )

    request.addfinalizer(cleanup_github_app_info)
    with open(record_config_file) as f:
        recorder_config = typing.cast(RecordConfigType, json.loads(f.read()))
        monkeypatch.setattr(
            settings, "GITHUB_APP_ID", recorder_config["integration_id"]
        )
        monkeypatch.setattr(
            github.GitHubAppInfo,
            "_bot",
            github_types.GitHubAccount(
                {
                    "id": recorder_config["app_user_id"],
                    "login": recorder_config["app_user_login"],
                    "type": "Bot",
                    "avatar_url": "",
                }
            ),
        )
        monkeypatch.setattr(
            github.GitHubAppInfo,
            "_app",
            github_types.GitHubApp(
                {
                    "id": recorder_config["integration_id"],
                    "name": recorder_config["app_user_login"][:-5].capitalize(),
                    "slug": recorder_config["app_user_login"][:-5],
                    "owner": {
                        "id": github_types.GitHubAccountIdType(1),
                        "login": github_types.GitHubLogin("mergifyio-testing"),
                        "type": "Organization",
                        "avatar_url": "",
                    },
                }
            ),
        )

        return RecorderFixture(recorder_config, recorder)


@pytest.fixture
def unittest_asyncio_glue(
    request: pytest.FixtureRequest,
    event_loop: asyncio.AbstractEventLoop,
) -> None:
    request.cls.pytest_event_loop = event_loop


@pytest.fixture
async def web_client_as_admin(
    web_client: httpx.AsyncClient, dashboard: DashboardFixture
) -> httpx.AsyncClient:
    web_client.headers["Authorization"] = f"bearer {dashboard.api_key_admin}"
    return web_client


@pytest.fixture
def unittest_glue(
    dashboard: DashboardFixture,
    web_client: httpx.AsyncClient,
    web_client_as_admin: httpx.AsyncClient,
    recorder: RecorderFixture,
    request: pytest.FixtureRequest,
) -> None:
    request.cls.api_key_admin = dashboard.api_key_admin
    request.cls.app = web_client
    request.cls.admin_app = web_client_as_admin
    request.cls.RECORD_CONFIG = recorder.config
    request.cls.cassette_library_dir = recorder.vcr.cassette_library_dir
    request.cls.subscription = dashboard.subscription


@dataclasses.dataclass
class RetrySecondaryRateLimit(tenacity.TryAgain):
    ratelimit_reset_timestamp: float


class wait_rate_limit(tenacity.wait.wait_base):
    def __call__(self, retry_state: tenacity.RetryCallState) -> float:
        if retry_state.outcome is None:
            return 0

        exc = retry_state.outcome.exception()
        if exc is None:
            return 0

        if isinstance(exc, exceptions.RateLimited):
            return int(exc.countdown.total_seconds())

        if isinstance(exc, RetrySecondaryRateLimit):
            if retry_state.attempt_number < 4:
                return 10 * (retry_state.attempt_number + 1)

            return (
                date.fromtimestamp(exc.ratelimit_reset_timestamp)
                + datetime.timedelta(seconds=5)
                - date.utcnow_from_clock_realtime()
            ).total_seconds()

        return 0


@contextlib.asynccontextmanager
async def _request_with_secondatary_rate_limit() -> abc.AsyncIterator[None]:
    try:
        yield
    except (
        http.HTTPClientSideError,
        http.HTTPForbidden,
        http.HTTPTooManyRequests,
    ) as exc:
        # A secondary rate limit has no specific headers about its own ratelimit
        # (its ratelimit headers are the one about the normal ratelimit),
        # so we need to check here if the message says it is a secondary ratelimit.
        if "a secondary rate limit" not in exc.response.text:
            raise

        raise RetrySecondaryRateLimit(
            float(exc.response.headers.get("X-RateLimit-Reset"))
        )


@contextlib.asynccontextmanager
async def _request_sync_lock() -> abc.AsyncIterator[None]:
    with REQUESTS_SYNC_FILE_LOCK.acquire(poll_interval=0.01):
        # Get the last modified date of the file, faster than reading the file
        # for the date inside.
        try:
            timestamp = float(os.path.getmtime(REQUESTS_SYNC_FILE_PATH))
        except FileNotFoundError:
            pass
        else:
            timestamp_date = date.fromtimestamp(timestamp)
            milliseconds_diff = (
                date.utcnow_from_clock_realtime() - timestamp_date
            ) / datetime.timedelta(milliseconds=1)

            if milliseconds_diff < 1000.0:
                await asyncio.sleep((1000.0 - milliseconds_diff) / 1000)

        yield

        with open(REQUESTS_SYNC_FILE_PATH, "w") as f:
            f.write(str(date.utcnow_from_clock_realtime().timestamp()))


@pytest.fixture(autouse=True, scope="module")
def mock_asyncgithubclient_requests() -> abc.Generator[None, None, None]:
    # When running tests in parallel, we need to mock the requests function
    # to add a delay between requests creating content to avoid hitting the secondary rate limit.
    # https://docs.github.com/en/rest/guides/best-practices-for-integrators#dealing-with-secondary-rate-limits

    if RECORD:
        real_request = github.AsyncGitHubClient.request

        async def mocked_request(  # type: ignore[no-untyped-def]
            self, method: str, *args: typing.Any, **kwargs: typing.Any
        ) -> httpx.Response:
            async for attempts in tenacity.AsyncRetrying(
                wait=wait_rate_limit(),
                # The stop is here just to avoid infinite loops.
                stop=tenacity.stop_after_attempt(5),
                retry=tenacity.retry_any(
                    tenacity.retry_if_exception_type(
                        (exceptions.RateLimited, RetrySecondaryRateLimit)
                    )
                ),
            ):
                with attempts:
                    async with contextlib.AsyncExitStack() as stack:
                        if method != "GET":
                            await stack.enter_async_context(_request_sync_lock())

                        await stack.enter_async_context(
                            _request_with_secondatary_rate_limit()
                        )

                        response = await real_request(self, method, *args, **kwargs)

            return response

        with mock.patch.object(github.AsyncGitHubClient, "request", mocked_request):
            yield
    else:
        yield


@pytest.fixture(autouse=True)
async def lock_settings_delete_branch_on_merge(
    request: pytest.FixtureRequest,
) -> abc.AsyncIterator[None]:
    if request.node.get_closest_marker("delete_branch_on_merge"):
        request.node.get_closest_marker("delete_branch_on_merge")
        with filelock.FileLock(
            os.path.join(os.path.dirname(__file__), "delete_branch_on_merge.lock")
        ):
            yield
    else:
        yield


@pytest.fixture(autouse=True)
def mock_vcr_stubs_httpx_stubs_to_serialized_response() -> (
    abc.Generator[None, None, None]
):
    """
    TODO(Kontrolix) Remove this fixture when this PR is merged and release
    https://github.com/kevin1024/vcrpy/pull/764
    See MRGFY-2561
    """

    def _to_serialized_response(httpx_response: typing.Any) -> dict[str, typing.Any]:
        try:
            content = httpx_response.content.decode("utf-8")
            base64encoded = False
        except UnicodeDecodeError:
            content = base64.b64encode(httpx_response.content)
            base64encoded = True

        return {
            "status_code": httpx_response.status_code,
            "http_version": httpx_response.http_version,
            "headers": vcr.stubs.httpx_stubs._transform_headers(httpx_response),
            "content": content,
            "base64encoded": base64encoded,
        }

    with mock.patch(
        "vcr.stubs.httpx_stubs._to_serialized_response",
        new=_to_serialized_response,
    ):
        yield


@pytest.fixture(autouse=True)
def mock_vcr_stubs_httpx_stubs_from_serialized_response() -> (
    abc.Generator[None, None, None]
):
    """
    TODO(Kontrolix) Remove this fixture when this PR is merged and release
    https://github.com/kevin1024/vcrpy/pull/764
    See MRGFY-2561
    """

    # NOTE(Kontrolix): Import here to avoid mess up this test
    # `test_command_queue_infinite_loop_bug`, I don'tknow why
    import vcr.stubs.httpx_stubs

    @vcr.stubs.httpx_stubs.patch(  # type: ignore[misc]
        "httpx.Response.close", vcr.stubs.httpx_stubs.MagicMock()
    )
    @vcr.stubs.httpx_stubs.patch(  # type: ignore[misc]
        "httpx.Response.read", vcr.stubs.httpx_stubs.MagicMock()
    )
    def _from_serialized_response(
        request: httpx.Request,
        serialized_response: dict[str, typing.Any],
        history: list[typing.Any] | None = None,
    ) -> httpx.Response:
        if serialized_response.get("base64encoded"):
            content = base64.b64decode(serialized_response["content"])
        else:
            content = serialized_response["content"].encode()

        response = httpx.Response(
            status_code=serialized_response["status_code"],
            request=request,
            headers=vcr.stubs.httpx_stubs._from_serialized_headers(
                serialized_response.get("headers")
            ),
            content=content,
            history=history or [],
        )
        response._content = content
        return response

    with mock.patch(
        "vcr.stubs.httpx_stubs._from_serialized_response",
        new=_from_serialized_response,
    ):
        yield
