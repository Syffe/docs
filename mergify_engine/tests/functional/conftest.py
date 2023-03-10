import asyncio
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

from mergify_engine import config
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import redis_utils
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.clients import http
from mergify_engine.dashboard import application as application_mod
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens as user_tokens_mod
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
    user_tokens: user_tokens_mod.UserTokens


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
            raise Exception(
                "Expected every arguments of `subscription` marker to be an instance of `subscription.Features`"
            )

    return frozenset(DEFAULT_SUBSCRIPTION_FEATURES + marker.args)


@pytest.fixture
async def dashboard(
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
        config.TESTING_ORGANIZATION_ID,
        "You're not nice",
        subscription_features,
    )
    await sub._save_subscription_to_cache()
    user_tokens = user_tokens_mod.UserTokens(
        redis_cache,
        config.TESTING_ORGANIZATION_ID,
        [
            {
                "id": config.ORG_ADMIN_ID,
                "login": github_types.GitHubLogin("mergify-test1"),
                "oauth_access_token": config.ORG_ADMIN_PERSONAL_TOKEN,
                "name": None,
                "email": None,
            },
            {
                "id": config.ORG_USER_ID,
                "login": github_types.GitHubLogin("mergify-test4"),
                "oauth_access_token": config.ORG_USER_PERSONAL_TOKEN,
                "name": None,
                "email": None,
            },
            {
                "id": config.TESTING_MERGIFY_TEST_2_ID,
                "login": github_types.GitHubLogin("mergify-test2"),
                "oauth_access_token": config.EXTERNAL_USER_PERSONAL_TOKEN,
                "name": None,
                "email": None,
            },
        ],
    )
    await typing.cast(user_tokens_mod.UserTokensSaas, user_tokens).save_to_cache()

    async with models.create_session() as session:
        for user_token in user_tokens.users:
            await github_user.GitHubUser.create_or_update(
                session,
                user_token["id"],
                user_token["login"],
                user_token["oauth_access_token"],
            )

    real_get_subscription = subscription.Subscription.get_subscription

    async def fake_retrieve_subscription_from_db(
        redis_cache: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
    ) -> subscription.Subscription:
        if owner_id == config.TESTING_ORGANIZATION_ID:
            return sub
        return subscription.Subscription(
            redis_cache,
            owner_id,
            "We're just testing",
            frozenset({subscription.Features.PUBLIC_REPOSITORY}),
        )

    async def fake_subscription(
        redis_cache: redis_utils.RedisCache, owner_id: github_types.GitHubAccountIdType
    ) -> subscription.Subscription:
        if owner_id == config.TESTING_ORGANIZATION_ID:
            return await real_get_subscription(redis_cache, owner_id)
        return subscription.Subscription(
            redis_cache,
            owner_id,
            "We're just testing",
            frozenset({subscription.Features.PUBLIC_REPOSITORY}),
        )

    patcher = mock.patch(
        "mergify_engine.dashboard.subscription.Subscription._retrieve_subscription_from_db",
        side_effect=fake_retrieve_subscription_from_db,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    patcher = mock.patch(
        "mergify_engine.dashboard.subscription.Subscription.get_subscription",
        side_effect=fake_subscription,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    async def fake_retrieve_user_tokens_from_db(
        redis_cache: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
        filter_tokens: bool,
    ) -> user_tokens_mod.UserTokens:
        if owner_id == config.TESTING_ORGANIZATION_ID:
            return user_tokens
        return user_tokens_mod.UserTokens(redis_cache, owner_id, [])

    real_get_user_tokens = user_tokens_mod.UserTokens.get

    async def fake_user_tokens(
        redis_cache: redis_utils.RedisCache,
        owner_id: github_types.GitHubAccountIdType,
        filter_tokens: bool,
    ) -> user_tokens_mod.UserTokens:
        if owner_id == config.TESTING_ORGANIZATION_ID:
            return await real_get_user_tokens(redis_cache, owner_id, filter_tokens)
        return user_tokens_mod.UserTokens(redis_cache, owner_id, [])

    patcher = mock.patch(
        "mergify_engine.dashboard.user_tokens.UserTokensSaas._retrieve_from_db",
        side_effect=fake_retrieve_user_tokens_from_db,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    patcher = mock.patch(
        "mergify_engine.dashboard.user_tokens.UserTokensSaas.get",
        side_effect=fake_user_tokens,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    async def fake_application_get(
        redis_cache: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> application_mod.Application:
        if (
            api_access_key == api_key_admin[:32]
            and api_secret_key == api_key_admin[32:]
        ):
            return application_mod.Application(
                redis_cache,
                123,
                "testing application",
                api_access_key,
                api_secret_key,
                account_scope={
                    "id": config.TESTING_ORGANIZATION_ID,
                    "login": config.TESTING_ORGANIZATION_NAME,
                },
            )
        raise application_mod.ApplicationUserNotFound()

    patcher = mock.patch(
        "mergify_engine.dashboard.application.ApplicationSaas.get",
        side_effect=fake_application_get,
    )
    patcher.start()
    request.addfinalizer(patcher.stop)

    return DashboardFixture(
        api_key_admin,
        sub,
        user_tokens,
    )


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

    recorder = vcr.VCR(
        cassette_library_dir=cassette_library_dir,
        record_mode="all" if RECORD else "none",
        match_on=["method", "uri"],
        ignore_localhost=True,
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
            github.GithubAppInstallationAuth,
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
                            "integration_id": config.INTEGRATION_ID,
                            "app_user_id": mergify_bot["id"],
                            "app_user_login": mergify_bot["login"],
                            "organization_id": config.TESTING_ORGANIZATION_ID,
                            "organization_name": config.TESTING_ORGANIZATION_NAME,
                            "repository_id": config.TESTING_REPOSITORY_ID,
                            "repository_name": github_types.GitHubRepositoryName(
                                config.TESTING_REPOSITORY_NAME
                            ),
                            "branch_prefix": date.utcnow().strftime("%Y%m%d%H%M%S"),
                        }
                    )
                )
            )

    request.addfinalizer(cleanup_github_app_info)
    with open(record_config_file) as f:
        recorder_config = typing.cast(RecordConfigType, json.loads(f.read()))
        monkeypatch.setattr(config, "INTEGRATION_ID", recorder_config["integration_id"])
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


class wait_secondary_rate_limit(tenacity.wait.wait_base):
    def __call__(self, retry_state: tenacity.RetryCallState) -> float:
        if retry_state.outcome is None:
            return 0

        exc = retry_state.outcome.exception()
        if exc is None or not isinstance(exc, RetrySecondaryRateLimit):
            return 0

        if retry_state.attempt_number < 4:
            return 10 * (retry_state.attempt_number + 1)

        return (
            date.fromtimestamp(exc.ratelimit_reset_timestamp)
            + datetime.timedelta(seconds=5)
            - date.utcnow_from_clock_realtime()
        ).total_seconds()


@tenacity.retry(
    wait=wait_secondary_rate_limit(),
    # The stop is here just to avoid infinite loops.
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(tenacity.TryAgain),
)
async def _request_with_ratelimit_retry(
    request_func: abc.Callable[
        [
            github.AsyncGithubClient,
            str,
            github_types.GitHubApiVersion | None,
            github_types.GitHubOAuthToken | None,
            typing.Any,
        ],
        abc.Coroutine[typing.Any, typing.Any, httpx.Response],
    ],
    *args: typing.Any,
    **kwargs: typing.Any,
) -> httpx.Response:
    try:
        return await request_func(*args, **kwargs)
    except exceptions.RateLimited as exc:
        await asyncio.sleep(int(exc.countdown.total_seconds()))
        raise tenacity.TryAgain
    except (
        http.HTTPClientSideError,
        http.HTTPForbidden,
        http.HTTPTooManyRequests,
    ) as exc:
        try:
            github._check_rate_limit(args[0], exc.response)
        except exceptions.RateLimited as ratelimit_exc:
            await asyncio.sleep(int(ratelimit_exc.countdown.total_seconds()))
            raise tenacity.TryAgain

        # A secondary rate limit has no specific headers about its own ratelimit
        # (its ratelimit headers are the one about the normal ratelimit),
        # so we need to check here if the message says it is a secondary ratelimit.
        if "a secondary rate limit" not in exc.response.text:
            raise

        raise RetrySecondaryRateLimit(
            float(exc.response.headers.get("X-RateLimit-Reset"))
        )


async def _request_with_lock_and_ratelimit_retry(
    request_func: abc.Callable[
        [
            github.AsyncGithubClient,
            str,
            github_types.GitHubApiVersion | None,
            github_types.GitHubOAuthToken | None,
            typing.Any,
        ],
        abc.Coroutine[typing.Any, typing.Any, httpx.Response],
    ],
    *args: typing.Any,
    **kwargs: typing.Any,
) -> httpx.Response:
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

        response = await _request_with_ratelimit_retry(request_func, *args, **kwargs)

        with open(REQUESTS_SYNC_FILE_PATH, "w") as f:
            f.write(str(date.utcnow_from_clock_realtime().timestamp()))

    return response


@pytest.fixture(autouse=True, scope="module")
def mock_asyncgithubclient_requests() -> abc.Generator[None, None, None]:
    # When running tests in parallel, we need to mock the requests function
    # to add a delay between requests creating content to avoid hitting the secondary rate limit.
    # https://docs.github.com/en/rest/guides/best-practices-for-integrators#dealing-with-secondary-rate-limits

    mocks = []

    if RECORD:
        real_post = github.AsyncGithubClient.post

        async def mocked_post(
            *args: typing.Any, **kwargs: typing.Any
        ) -> httpx.Response:
            return await _request_with_lock_and_ratelimit_retry(
                real_post, *args, **kwargs  # type: ignore[arg-type]
            )

        mocks.append(mock.patch.object(github.AsyncGithubClient, "post", mocked_post))

        real_put = github.AsyncGithubClient.put

        async def mocked_put(*args: typing.Any, **kwargs: typing.Any) -> httpx.Response:
            return await _request_with_lock_and_ratelimit_retry(
                real_put, *args, **kwargs  # type: ignore[arg-type]
            )

        mocks.append(mock.patch.object(github.AsyncGithubClient, "put", mocked_put))

        real_patch = github.AsyncGithubClient.patch

        async def mocked_patch(
            *args: typing.Any, **kwargs: typing.Any
        ) -> httpx.Response:
            return await _request_with_lock_and_ratelimit_retry(
                real_patch, *args, **kwargs  # type: ignore[arg-type]
            )

        mocks.append(mock.patch.object(github.AsyncGithubClient, "patch", mocked_patch))

        real_delete = github.AsyncGithubClient.delete

        async def mocked_delete(
            *args: typing.Any, **kwargs: typing.Any
        ) -> httpx.Response:
            return await _request_with_lock_and_ratelimit_retry(
                real_delete, *args, **kwargs  # type: ignore[arg-type]
            )

        mocks.append(
            mock.patch.object(github.AsyncGithubClient, "delete", mocked_delete)
        )

    real_get = github.AsyncGithubClient.get

    async def mocked_get(*args: typing.Any, **kwargs: typing.Any) -> httpx.Response:
        return await _request_with_ratelimit_retry(
            real_get, *args, **kwargs  # type: ignore[arg-type]
        )

    mocks.append(mock.patch.object(github.AsyncGithubClient, "get", mocked_get))

    with contextlib.ExitStack() as es:
        for m in mocks:
            es.enter_context(m)

        yield
