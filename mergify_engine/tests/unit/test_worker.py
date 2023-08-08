import asyncio
import dataclasses
import datetime
import json
import time
import typing
from unittest import mock

from freezegun import freeze_time
import httpx
import msgpack
import pytest
from redis import exceptions as redis_exceptions
import tenacity._asyncio

from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.clients import github_app
from mergify_engine.clients import http
from mergify_engine.worker import dedicated_workers_cache_syncer_service
from mergify_engine.worker import dedicated_workers_spawner_service
from mergify_engine.worker import delayed_refresh_service
from mergify_engine.worker import gitter_service
from mergify_engine.worker import manager
from mergify_engine.worker import shared_workers_spawner_service
from mergify_engine.worker import stream
from mergify_engine.worker import stream_cli
from mergify_engine.worker import stream_lua
from mergify_engine.worker import stream_monitoring_service
from mergify_engine.worker import task


# NOTE(sileht): old version of the worker_pusher.push() method (3.0.0)
# Since we do rolling upgrade of the API and worker_pusher.push is used by the API
# We can't migrate the data on the worker startup.
# So we need to support at least until next major version the old format in Redis database
async def legacy_push(
    redis: redis_utils.RedisStream,
    owner_id: github_types.GitHubAccountIdType,
    owner_login: github_types.GitHubLogin,
    repo_id: github_types.GitHubRepositoryIdType,
    repo_name: github_types.GitHubRepositoryName,
    tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
    pull_number: github_types.GitHubPullRequestNumber | None,
    event_type: github_types.GitHubEventType,
    data: github_types.GitHubEvent,
    score: str | None = None,
) -> None:
    now = date.utcnow()
    event = msgpack.packb(
        {
            "event_type": event_type,
            "data": data,
            "timestamp": now.isoformat(),
        },
        use_bin_type=True,
    )
    scheduled_at = now + datetime.timedelta(
        seconds=worker_pusher.WORKER_PROCESSING_DELAY
    )
    if score is None:
        score = str(date.utcnow().timestamp())
    bucket_org_key = stream_lua.BucketOrgKeyType(f"bucket~{owner_id}~{owner_login}")
    bucket_sources_key = stream_lua.BucketSourcesKeyType(
        f"bucket-sources~{repo_id}~{repo_name}~{pull_number or 0}"
    )
    await stream_lua.push_pull(
        redis,
        bucket_org_key,
        bucket_sources_key,
        tracing_repo_name,
        scheduled_at,
        event,
        score,
    )


async def fake_get_subscription(*args: typing.Any, **kwargs: typing.Any) -> mock.Mock:
    sub = mock.Mock()
    sub.has_feature.return_value = False
    return sub


async def stop_and_wait_worker(self: manager.ServiceManager) -> None:
    self.stop()
    if self._stop_task is not None:
        await self._stop_task


FAKE_INSTALLATION = github_types.GitHubInstallation(
    {
        "id": github_types.GitHubInstallationIdType(12345),
        "account": {
            "id": github_types.GitHubAccountIdType(123),
            "login": github_types.GitHubLogin("owner-123"),
            "type": "Organization",
            "avatar_url": "",
        },
        "target_type": "Organization",
        "permissions": github_app.EXPECTED_MINIMAL_PERMISSIONS["Organization"],
        "suspended_at": None,
    }
)

HTTP_500_EXCEPTION = http.HTTPServerSideError(
    "no way",
    response=httpx.Response(500),
    request=httpx.Request("GET", "http://localhost"),
)


async def just_run_once(
    self: task.TaskRetriedForever,
    name: str,
    func: task.TaskRetriedForeverFuncT,
    sleep_time: float,
) -> None:
    await func()


@pytest.fixture
def stream_processor(redis_links: redis_utils.RedisLinks) -> stream.Processor:
    return stream.Processor(redis_links, "shared-0", None, stream.OwnerLoginsCache())


@pytest.fixture
def fake_installation() -> context.Installation:
    return context.Installation(FAKE_INSTALLATION, {}, None, None, {})  # type: ignore[arg-type]


def fake_get_installation_from_account_id(
    owner_id: github_types.GitHubAccountIdType,
) -> github_types.GitHubInstallation:
    return {
        "id": github_types.GitHubInstallationIdType(12345),
        "account": {
            "id": owner_id,
            "login": github_types.GitHubLogin(f"owner-{owner_id}"),
            "type": "User",
            "avatar_url": "",
        },
        "target_type": "Organization",
        "permissions": github_app.EXPECTED_MINIMAL_PERMISSIONS["Organization"],
        "suspended_at": None,
    }


# NOTE(Syffe): related to this issue: https://github.com/python/cpython/issues/84753
# and this PR: https://github.com/python/cpython/pull/94050
# inspect doesn't assert mock.AsyncMock as a coroutine, so we need to create a fake one
async def fake() -> None:
    pass


WORKER_HAS_WORK_INTERVAL_CHECK = 0.02


async def run_worker(
    test_timeout: float | None = None, **kwargs: typing.Any
) -> manager.ServiceManager:
    w = manager.ServiceManager(
        worker_idle_time=0.01,
        stream_monitoring_idle_time=0.01,
        gitter_idle_time=0.01,
        delayed_refresh_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        gitter_concurrent_jobs=0,
        ci_event_processing_idle_time=0.01,
        log_embedder_idle_time=0.01,
        **kwargs,
    )
    await w.start()

    real_consume_method = stream.Processor.consume
    worker_concurrency_works = [0]

    async def tracked_consume(
        inner_self: stream.Processor,
        bucket_org_key: stream_lua.BucketOrgKeyType,
        owner_id: github_types.GitHubAccountIdType,
        owner_login_for_tracing: github_types.GitHubLoginForTracing,
    ) -> None:
        worker_concurrency_works[0] += 1
        try:
            await real_consume_method(
                inner_self, bucket_org_key, owner_id, owner_login_for_tracing
            )
        finally:
            worker_concurrency_works[0] -= 1

    stream.Processor.consume = tracked_consume  # type: ignore[assignment]

    # run delayed-refresh and monitor task at least once
    await asyncio.sleep(WORKER_HAS_WORK_INTERVAL_CHECK)

    started_at = time.monotonic()
    while (
        (await w._redis_links.stream.zcard("streams")) > 0
        or worker_concurrency_works[0] > 0
    ) and (test_timeout is None or time.monotonic() - started_at < test_timeout):
        await asyncio.sleep(WORKER_HAS_WORK_INTERVAL_CHECK)

    # NOTE(sileht): we just stop tasks to be able to introspect the services' state
    await w._shutdown()
    stream.Processor.consume = real_consume_method  # type: ignore[method-assign]
    return w


@dataclasses.dataclass
class InstallationMatcher:
    owner: str

    def __eq__(self, installation: object) -> bool:
        if not isinstance(installation, context.Installation):
            return NotImplemented
        return self.owner == installation.owner_login


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_worker_legacy_push(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    setup_database: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    buckets = set()
    bucket_sources = set()
    for installation_id in range(8):
        for pull_number in range(1, 3):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                repo_id = installation_id
                owner_id = installation_id
                buckets.add(f"bucket~{owner_id}~{owner}")
                bucket_sources.add(f"bucket-sources~{repo_id}~{repo}~{pull_number}")
                await legacy_push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(owner_id),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(repo_id),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                )

    # Push some with the new format too
    for installation_id in range(8):
        for pull_number in range(1, 3):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                repo_id = installation_id
                owner_id = installation_id
                buckets.add(f"bucket~{owner_id}")
                bucket_sources.add(f"bucket-sources~{repo_id}~{pull_number}")
                await worker_pusher.push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(owner_id),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(repo_id),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                    priority=worker_pusher.Priority.immediate,
                )

    # Check everything we push are in redis
    assert 16 == (await redis_links.stream.zcard("streams"))
    assert 16 == len(await redis_links.stream.keys("bucket~*"))
    assert 32 == len(await redis_links.stream.keys("bucket-sources~*"))
    for bucket in buckets:
        assert 2 == await redis_links.stream.zcard(bucket)
    for bucket_source in bucket_sources:
        assert 3 == await redis_links.stream.xlen(bucket_source)

    w = await run_worker()

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    # Check engine have been run with expect data
    assert 32 == len(run_engine.mock_calls)
    assert (
        mock.call(
            InstallationMatcher(owner="owner-0"),
            0,
            "repo-0",
            1,
            [
                {
                    "event_type": "pull_request",
                    "data": {"payload": 0},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "pull_request",
                    "data": {"payload": 1},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "pull_request",
                    "data": {"payload": 2},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        )
        in run_engine.mock_calls
    )

    serv = w.get_service(shared_workers_spawner_service.SharedStreamService)
    assert serv is not None
    assert serv._owners_cache._mapping == {i: f"owner-{i}" for i in range(0, 8)}


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_worker_with_waiting_tasks(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    setup_database: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    buckets = set()
    bucket_sources = set()
    for installation_id in range(8):
        for pull_number in range(1, 3):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                repo_id = installation_id
                owner_id = installation_id
                buckets.add(f"bucket~{owner_id}")
                bucket_sources.add(f"bucket-sources~{repo_id}~{pull_number}")
                await worker_pusher.push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(owner_id),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(repo_id),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                    priority=worker_pusher.Priority.immediate,
                )

    # Check everything we push are in redis
    assert 8 == (await redis_links.stream.zcard("streams"))
    assert 8 == len(await redis_links.stream.keys("bucket~*"))
    assert 16 == len(await redis_links.stream.keys("bucket-sources~*"))
    for bucket in buckets:
        assert 2 == await redis_links.stream.zcard(bucket)
    for bucket_source in bucket_sources:
        assert 3 == await redis_links.stream.xlen(bucket_source)

    await run_worker()

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    # Check engine have been run with expect data
    assert 16 == len(run_engine.mock_calls)
    assert (
        mock.call(
            InstallationMatcher(owner="owner-0"),
            0,
            "repo-0",
            1,
            [
                {
                    "event_type": "pull_request",
                    "data": {"payload": 0},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "pull_request",
                    "data": {"payload": 1},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "pull_request",
                    "data": {"payload": 2},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        )
        in run_engine.mock_calls
    )


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
@mock.patch("mergify_engine.clients.github.aget_client")
@mock.patch.object(
    pull_request_finder.PullRequestFinder,
    "extract_pull_numbers_from_event",
    mock.AsyncMock(side_effect=[[123, 456, 789], [123, 789]]),
)
async def test_worker_expanded_events(
    aget_client: mock.MagicMock,
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
) -> None:
    logs.setup_logging()
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    client = mock.Mock(
        auth=mock.Mock(
            owner_id=123,
            owner_login="owner-123",
            installation=fake_get_installation_from_account_id(
                github_types.GitHubAccountIdType(123)
            ),
        ),
    )
    client.__aenter__ = mock.AsyncMock(return_value=client)
    client.__aexit__ = mock.AsyncMock()
    client.items.return_value = mock.AsyncMock()

    aget_client.return_value = client

    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        None,
        "push",
        github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        None,
        "check_run",
        github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )

    assert 1 == await redis_links.stream.zcard("streams")
    assert 2 == await redis_links.stream.zcard("bucket~123")
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 2 == await redis_links.stream.xlen("bucket-sources~123~0")
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~123")

    await run_worker()

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    # Check engine have been run with expect data, order is very important
    # push event on pull request with other events must go last
    assert 3 == len(run_engine.mock_calls)
    assert run_engine.mock_calls[0] == mock.call(
        InstallationMatcher(owner="owner-123"),
        123,
        "repo",
        123,
        [
            {
                "event_type": "pull_request",
                "data": {"payload": "whatever"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
            {
                "event_type": "push",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
            {
                "event_type": "check_run",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
        ],
    )
    assert run_engine.mock_calls[1] == mock.call(
        InstallationMatcher(owner="owner-123"),
        123,
        "repo",
        789,
        [
            {
                "event_type": "push",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
            {
                "event_type": "check_run",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
        ],
    )
    assert run_engine.mock_calls[2] == mock.call(
        InstallationMatcher(owner="owner-123"),
        123,
        "repo",
        456,
        [
            {
                "event_type": "push",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
        ],
    )


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_worker_with_one_task(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    setup_database: None,
) -> None:
    get_subscription.side_effect = fake_get_subscription
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "issue_comment",
        github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )

    # Check everything we push are in redis
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 1 == await redis_links.stream.zcard("bucket~123")
    assert 1 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")

    await run_worker()

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    # Check engine have been run with expect data
    assert 1 == len(run_engine.mock_calls)
    assert run_engine.mock_calls[0] == mock.call(
        InstallationMatcher(owner="owner-123"),
        123,
        "repo",
        123,
        [
            {
                "event_type": "pull_request",
                "data": {"payload": "whatever"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
            {
                "event_type": "issue_comment",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
        ],
    )


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_consume_unexisting_stream(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    stream_processor: stream.Processor,
    logger_checker: None,
) -> None:
    get_subscription.side_effect = fake_get_subscription
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id

    await stream_processor.consume(
        stream_lua.BucketOrgKeyType("buckets~2~notexists"),
        github_types.GitHubAccountIdType(2),
        github_types.GitHubLogin("notexists"),
    )
    assert len(run_engine.mock_calls) == 0


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_consume_good_stream(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
    logger_checker: None,
) -> None:
    get_subscription.side_effect = fake_get_subscription
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "issue_comment",
        github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )

    # Check everything we push are in redis
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 1 == await redis_links.stream.zcard("bucket~123")
    assert 1 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")

    await stream_processor.consume(
        stream_lua.BucketOrgKeyType("bucket~123"),
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
    )
    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
        == "owner-123"
    )

    assert len(run_engine.mock_calls) == 1
    assert run_engine.mock_calls[0] == mock.call(
        InstallationMatcher(owner="owner-123"),
        123,
        "repo",
        123,
        [
            {
                "event_type": "pull_request",
                "data": {"payload": "whatever"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
            {
                "event_type": "issue_comment",
                "data": {"payload": "foobar"},
                "timestamp": mock.ANY,
                "initial_score": mock.ANY,
            },
        ],
    )

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch.object(manager, "LOG")
@mock.patch("redis.asyncio.Redis.ping")
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
async def test_worker_start_redis_ping(
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    ping: mock.MagicMock,
    logger: mock.MagicMock,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    logs.setup_logging()

    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    ping.side_effect = [
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        fake(),
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        redis_exceptions.ConnectionError(),
        fake(),
    ]

    w = manager.ServiceManager(enabled_services=set())

    with mock.patch.object(tenacity.wait_exponential, "__call__", return_value=0):
        await w.start()

    request.addfinalizer(lambda: event_loop.run_until_complete(w._shutdown()))

    assert 8 == len(logger.warning.mock_calls)
    assert logger.warning.mock_calls[0].args == (
        "Couldn't connect to Redis %s, retrying in %d seconds...",
        "Stream",
        mock.ANY,
    )
    assert logger.warning.mock_calls[1].args == (
        "Couldn't connect to Redis %s, retrying in %d seconds...",
        "Stream",
        mock.ANY,
    )
    assert logger.warning.mock_calls[4].args == (
        "Couldn't connect to Redis %s, retrying in %d seconds...",
        "Cache",
        mock.ANY,
    )
    assert logger.warning.mock_calls[5].args == (
        "Couldn't connect to Redis %s, retrying in %d seconds...",
        "Cache",
        mock.ANY,
    )


@mock.patch("mergify_engine.worker.stream.daiquiri.getLogger")
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_retrying_pull(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    logger_class: mock.MagicMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
) -> None:
    logs.setup_logging()
    logger = logger_class.return_value
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    # One retries once, the other reaches max_retry
    run_engine.side_effect = [
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        mock.Mock(),
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
        HTTP_500_EXCEPTION,
    ]

    with freeze_time("2020-01-01 22:00:00", tick=True):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(42),
            "issue_comment",
            github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )

    # Check everything we push are in redis
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 2 == await redis_links.stream.zcard("bucket~123")
    assert 2 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~123")
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~42")

    with freeze_time("2020-01-01 22:00:20", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )

    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
        == "owner-123"
    )

    assert len(run_engine.mock_calls) == 2
    assert run_engine.mock_calls == [
        mock.call(
            InstallationMatcher(owner="owner-123"),
            123,
            "repo",
            123,
            [
                {
                    "event_type": "pull_request",
                    "data": {"payload": "whatever"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        ),
        mock.call(
            InstallationMatcher(owner="owner-123"),
            123,
            "repo",
            42,
            [
                {
                    "event_type": "issue_comment",
                    "data": {"payload": "foobar"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        ),
    ]

    # Check stream still there and attempts recorded
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 2 == await redis_links.stream.zcard("bucket~123")
    assert 2 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~123")
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~42")
    assert {
        b"bucket-sources~123~42": b"1",
        b"bucket-sources~123~123": b"1",
    } == await redis_links.stream.hgetall("attempts")

    with freeze_time("2020-01-01 22:00:35", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )

    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 1 == await redis_links.stream.zcard("bucket~123")
    assert 1 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == await redis_links.stream.xlen("bucket-sources~123~123")
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~42")

    assert 1 == len(await redis_links.stream.hgetall("attempts"))
    assert len(run_engine.mock_calls) == 4
    assert {b"bucket-sources~123~42": b"2"} == await redis_links.stream.hgetall(
        "attempts"
    )

    # Check nothing is retried if we replay the steram before 30s
    with freeze_time("2020-01-01 22:00:58", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 1 == await redis_links.stream.zcard("bucket~123")
    assert 1 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == await redis_links.stream.xlen("bucket-sources~123~123")
    assert 1 == await redis_links.stream.xlen("bucket-sources~123~42")

    assert 1 == len(await redis_links.stream.hgetall("attempts"))
    assert len(run_engine.mock_calls) == 4
    assert {b"bucket-sources~123~42": b"2"} == await redis_links.stream.hgetall(
        "attempts"
    )

    when = date.fromisoformat("2020-01-01 22:01:33")
    for _ in range(14):
        when += datetime.timedelta(seconds=30)
        with freeze_time(when, tick=True):
            await stream_processor.consume(
                stream_lua.BucketOrgKeyType("bucket~123"),
                github_types.GitHubAccountIdType(123),
                github_types.GitHubLogin("owner-123"),
            )
    assert len(run_engine.mock_calls) == 17

    # Too many retries, everything is gone
    assert 15 == len(logger.info.mock_calls)
    assert 1 == len(logger.warning.mock_calls)
    assert 0 == len(logger.error.mock_calls)
    assert logger.info.mock_calls[0].args == (
        "failed to process pull request, retrying",
    )
    assert logger.info.mock_calls[1].args == (
        "failed to process pull request, retrying",
    )
    assert logger.warning.mock_calls[0].args == (
        "failed to process pull request, abandoning",
    )
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == await redis_links.stream.zcard("bucket~123")
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch.object(stream, "LOG")
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_retrying_stream_recovered(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    logger: mock.MagicMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
) -> None:
    logs.setup_logging()

    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    response = mock.Mock()
    response.json.return_value = {"message": "boom"}
    response.status_code = 401
    run_engine.side_effect = http.HTTPClientSideError(
        message="foobar", request=response.request, response=response
    )

    with freeze_time("2020-01-01 22:00:00", tick=True):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "issue_comment",
            github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )

        assert 1 == (await redis_links.stream.zcard("streams"))
        assert 1 == len(await redis_links.stream.keys("bucket~*"))
        assert 1 == await redis_links.stream.zcard("bucket~123")
        assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")
        assert 0 == len(await redis_links.stream.hgetall("attempts"))

        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
        assert (
            stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
            == "owner-123"
        )

        assert len(run_engine.mock_calls) == 1
        assert run_engine.mock_calls[0] == mock.call(
            InstallationMatcher(owner="owner-123"),
            123,
            "repo",
            123,
            [
                {
                    "event_type": "pull_request",
                    "data": {"payload": "whatever"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "issue_comment",
                    "data": {"payload": "foobar"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        )

        # Check stream still there and attempts recorded
        assert 1 == (await redis_links.stream.zcard("streams"))
        assert 1 == len(await redis_links.stream.keys("bucket~*"))
        assert 1 == await redis_links.stream.zcard("bucket~123")
        assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")
        assert 1 == len(await redis_links.stream.hgetall("attempts"))

        assert {b"bucket~123": b"1"} == await redis_links.stream.hgetall("attempts")

        run_engine.side_effect = None

    with freeze_time("2020-01-01 22:00:31", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
        assert len(run_engine.mock_calls) == 2
        assert 0 == (await redis_links.stream.zcard("streams"))
        assert 0 == len(await redis_links.stream.keys("bucket~*"))
        assert 0 == await redis_links.stream.zcard("bucket~123")
        assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))

        assert 1 == len(logger.info.mock_calls)
        assert 0 == len(logger.warning.mock_calls)
        assert 0 == len(logger.error.mock_calls)
        assert logger.info.mock_calls[0].args == (
            "failed to process org bucket, retrying",
        )


@mock.patch.object(stream, "LOG")
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_retrying_stream_failure(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    logger: mock.MagicMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
) -> None:
    logs.setup_logging()

    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    response = mock.Mock()
    response.json.return_value = {"message": "boom"}
    response.status_code = 401
    run_engine.side_effect = http.HTTPClientSideError(
        message="foobar", request=response.request, response=response
    )

    with freeze_time("2020-01-01 22:00:00", tick=True):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "issue_comment",
            github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )

        assert 1 == await redis_links.stream.zcard("streams")
        assert 1 == len(await redis_links.stream.keys("bucket~*"))
        assert 1 == await redis_links.stream.zcard("bucket~123")
        assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")
        assert 0 == len(await redis_links.stream.hgetall("attempts"))

    with freeze_time("2020-01-01 22:00:31", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
        assert (
            stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
            == "owner-123"
        )

        assert len(run_engine.mock_calls) == 1
        assert run_engine.mock_calls[0] == mock.call(
            InstallationMatcher(owner="owner-123"),
            123,
            "repo",
            123,
            [
                {
                    "event_type": "pull_request",
                    "data": {"payload": "whatever"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
                {
                    "event_type": "issue_comment",
                    "data": {"payload": "foobar"},
                    "timestamp": mock.ANY,
                    "initial_score": mock.ANY,
                },
            ],
        )

        # Check stream still there and attempts recorded
        assert 1 == (await redis_links.stream.zcard("streams"))
        assert 1 == len(await redis_links.stream.keys("bucket~*"))
        assert 1 == len(await redis_links.stream.hgetall("attempts"))

        assert {b"bucket~123": b"1"} == await redis_links.stream.hgetall("attempts")

    with freeze_time("2020-01-01 22:01:03", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
        assert len(run_engine.mock_calls) == 2
        assert {b"bucket~123": b"2"} == await redis_links.stream.hgetall("attempts")

        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )
        assert len(run_engine.mock_calls) == 3

        # Still there
        assert 3 == len(logger.info.mock_calls)
        assert 0 == len(logger.warning.mock_calls)
        assert 0 == len(logger.error.mock_calls)
        assert logger.info.mock_calls[0].args == (
            "failed to process org bucket, retrying",
        )
        assert logger.info.mock_calls[1].args == (
            "failed to process org bucket, retrying",
        )
        assert logger.info.mock_calls[2].args == (
            "failed to process org bucket, retrying",
        )
        assert 1 == (await redis_links.stream.zcard("streams"))
        assert 1 == len(await redis_links.stream.keys("bucket~*"))
        assert 1 == await redis_links.stream.zcard("bucket~123")
        assert 2 == await redis_links.stream.xlen("bucket-sources~123~123")
        assert 1 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch("mergify_engine.worker.stream.daiquiri.getLogger")
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_pull_unexpected_error(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.AsyncMock,
    get_subscription: mock.AsyncMock,
    logger_class: mock.MagicMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
) -> None:
    logs.setup_logging()
    logger = logger_class.return_value

    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    run_engine.side_effect = Exception

    with freeze_time("2020-01-01 22:00:00", tick=True):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )

        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )

    with freeze_time("2020-01-01 22:00:40", tick=True):
        await stream_processor.consume(
            stream_lua.BucketOrgKeyType("bucket~123"),
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
        )

    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
        == "owner-123"
    )

    # Exception have been logged, redis must be clean
    assert len(run_engine.mock_calls) == 2
    assert len(logger.warning.mock_calls) == 0
    assert len(logger.error.mock_calls) == 2
    assert logger.error.mock_calls[0].args == ("failed to process pull request",)
    assert logger.error.mock_calls[1].args == ("failed to process pull request",)
    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_priority(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.Mock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    async def push_prio(pr: int, prio: worker_pusher.Priority) -> None:
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(234),
            github_types.GitHubLogin("owner-234"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(pr),
            "pull_request",
            {"payload": prio},  # type: ignore[typeddict-item]
            score=str(worker_pusher.get_priority_score(prio)),
        )

    with freeze_time("2020-01-01", tick=True):
        await push_prio(1, worker_pusher.Priority.low)
        await push_prio(2, worker_pusher.Priority.low)
        await push_prio(3, worker_pusher.Priority.high)
        await push_prio(4, worker_pusher.Priority.low)
        await push_prio(5, worker_pusher.Priority.medium)
        await push_prio(6, worker_pusher.Priority.medium)
        await push_prio(7, worker_pusher.Priority.high)
        await push_prio(8, worker_pusher.Priority.low)

    with freeze_time("2020-01-04", tick=True):
        await push_prio(9, worker_pusher.Priority.high)
        await push_prio(10, worker_pusher.Priority.low)
        await push_prio(11, worker_pusher.Priority.medium)
        await push_prio(12, worker_pusher.Priority.medium)
        await push_prio(13, worker_pusher.Priority.low)

    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    received = []

    def fake_engine(
        installation: context.Installation,
        repo_id: github_types.GitHubRepositoryIdType,
        repo: github_types.GitHubRepositoryName,
        pull_number: github_types.GitHubPullRequestNumber,
        sources: list[context.T_PayloadEventSource],
    ) -> None:
        received.append(pull_number)

    run_engine.side_effect = fake_engine
    syncer_service = (
        dedicated_workers_cache_syncer_service.DedicatedWorkersCacheSyncerService(
            redis_links, idle_time=0
        )
    )
    shared_service = shared_workers_spawner_service.SharedStreamService(
        redis_links,
        service_dedicated_workers_cache_syncer=syncer_service,
        process_index=0,
        idle_time=0,
        worker_idle_time=0,
        shared_stream_processes=1,
        shared_stream_tasks_per_process=0,
        retry_handled_exception_forever=False,
    )
    shared_service.shared_stream_tasks_per_process = 1
    request.addfinalizer(
        lambda: event_loop.run_until_complete(
            task.stop_and_wait(syncer_service.tasks + shared_service.tasks)
        )
    )

    with freeze_time("2020-01-14", tick=True):
        await shared_service._stream_worker_task(stream_processor)

    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))
    assert received == [3, 7, 9, 5, 6, 11, 12, 1, 2, 4, 8, 10, 13]


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_date_scheduling(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    stream_processor: stream.Processor,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription
    # Don't process it before 2040
    with freeze_time("2040-01-01"):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        )
        unwanted_owner_id = "owner-123"

    with freeze_time("2020-01-01"):
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(234),
            github_types.GitHubLogin("owner-234"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(321),
            "pull_request",
            github_types.GitHubEvent({"payload": "foobar"}),  # type: ignore[typeddict-item]
        )
        wanted_owner_id = "owner-234"

    assert 2 == (await redis_links.stream.zcard("streams"))
    assert 2 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    syncer_service = (
        dedicated_workers_cache_syncer_service.DedicatedWorkersCacheSyncerService(
            redis_links, idle_time=0
        )
    )
    shared_service = shared_workers_spawner_service.SharedStreamService(
        redis_links,
        service_dedicated_workers_cache_syncer=syncer_service,
        process_index=0,
        idle_time=0,
        worker_idle_time=0,
        shared_stream_processes=1,
        shared_stream_tasks_per_process=0,
        retry_handled_exception_forever=False,
    )
    shared_service.shared_stream_tasks_per_process = 1
    request.addfinalizer(
        lambda: event_loop.run_until_complete(
            task.stop_and_wait(syncer_service.tasks + shared_service.tasks)
        )
    )

    received = []

    def fake_engine(
        installation: context.Installation,
        repo_id: github_types.GitHubRepositoryIdType,
        repo: github_types.GitHubRepositoryName,
        pull_number: github_types.GitHubPullRequestNumber,
        sources: list[context.T_PayloadEventSource],
    ) -> None:
        received.append(installation.owner_login)

    run_engine.side_effect = fake_engine

    with freeze_time("2020-01-14"):
        await shared_service._stream_worker_task(stream_processor)

    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))
    assert received == [wanted_owner_id]

    with freeze_time("2030-01-14"):
        await shared_service._stream_worker_task(stream_processor)

    assert 1 == (await redis_links.stream.zcard("streams"))
    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))
    assert received == [wanted_owner_id]

    # We are in 2041, we have something todo :)
    with freeze_time("2041-01-14"):
        await shared_service._stream_worker_task(stream_processor)

    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))
    assert received == [wanted_owner_id, unwanted_owner_id]

    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
        == "owner-123"
    )
    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(234))
        == "owner-234"
    )


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
async def test_worker_drop_bucket(
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
) -> None:
    get_subscription.side_effect = fake_get_subscription

    buckets = set()
    bucket_sources = set()
    _id = 123
    for pull_number in range(2):
        for data in range(3):
            owner = f"owner-{_id}"
            repo = f"repo-{_id}"
            repo_id = _id
            owner_id = _id
            buckets.add(f"bucket~{owner_id}")
            bucket_sources.add(f"bucket-sources~{repo_id}~{pull_number}")
            await worker_pusher.push(
                redis_links.stream,
                github_types.GitHubAccountIdType(owner_id),
                github_types.GitHubLogin(owner),
                github_types.GitHubRepositoryIdType(repo_id),
                github_types.GitHubRepositoryName(repo),
                github_types.GitHubPullRequestNumber(pull_number),
                "pull_request",
                github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
            )

    assert 1 == len(await redis_links.stream.keys("bucket~*"))
    assert 2 == len(await redis_links.stream.keys("bucket-sources~*"))

    for bucket in buckets:
        assert 2 == await redis_links.stream.zcard(bucket)
    for bucket_source in bucket_sources:
        assert 3 == await redis_links.stream.xlen(bucket_source)

    await stream_lua.drop_bucket(
        redis_links.stream, stream_lua.BucketOrgKeyType("bucket~123")
    )
    assert 0 == len(await redis_links.stream.keys("bucket~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))

    await stream_lua.clean_org_bucket(
        redis_links.stream, stream_lua.BucketOrgKeyType("bucket~123"), date.utcnow()
    )

    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
async def test_worker_debug_report(
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
) -> None:
    get_subscription.side_effect = fake_get_subscription

    for installation_id in range(8):
        for pull_number in range(2):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                await worker_pusher.push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(123),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(123),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                )

    await stream_cli.async_status()


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_retrying_after_read_error(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    fake_installation: context.Installation,
    stream_processor: stream.Processor,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    response = mock.Mock()
    response.json.return_value = {"message": "boom"}
    response.status_code = 503
    run_engine.side_effect = httpx.ReadError(
        "Server disconnected while attempting read",
        request=mock.Mock(),
    )

    with pytest.raises(stream.OrgBucketRetry):
        async with stream_processor._translate_exception_to_retries(
            stream_lua.BucketOrgKeyType("stream~owner~123")
        ):
            await stream.run_engine(
                fake_installation,
                github_types.GitHubRepositoryIdType(123),
                github_types.GitHubRepositoryName("repo"),
                github_types.GitHubPullRequestNumber(1234),
                [],
            )

    assert (
        stream_processor.owners_cache.get(github_types.GitHubAccountIdType(123))
        == "<unknown 123>"
    )


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_ignore_503(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    setup_database: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    response = mock.Mock()
    response.text = "Server Error: Sorry, this diff is taking too long to generate."
    response.status_code = 503
    response.json.side_effect = json.JSONDecodeError("whatever", "", 0)
    response.reason_phrase = "Service Unavailable"
    response.url = "https://api.github.com/repositories/1234/pulls/5/files"

    run_engine.side_effect = lambda *_: http.raise_for_status(response)

    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )

    await run_worker()

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("stream~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_worker_with_multiple_workers(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    buckets = set()
    bucket_sources = set()
    for installation_id in range(100):
        for pull_number in range(1, 3):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                repo_id = installation_id
                owner_id = installation_id
                buckets.add(f"bucket~{owner_id}")
                bucket_sources.add(f"bucket-sources~{repo_id}~{pull_number}")
                await worker_pusher.push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(owner_id),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(repo_id),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                    priority=worker_pusher.Priority.immediate,
                )

    # Check everything we push are in redis
    assert 100 == (await redis_links.stream.zcard("streams"))
    assert 100 == len(await redis_links.stream.keys("bucket~*"))
    assert 200 == len(await redis_links.stream.keys("bucket-sources~*"))
    for bucket in buckets:
        assert 2 == await redis_links.stream.zcard(bucket)
    for bucket_source in bucket_sources:
        assert 3 == await redis_links.stream.xlen(bucket_source)

    shared_stream_processes = 4
    shared_stream_tasks_per_process = 3

    await asyncio.gather(
        *[
            run_worker(
                enabled_services={"shared-workers-spawner"},
                shared_stream_tasks_per_process=shared_stream_tasks_per_process,
                shared_stream_processes=shared_stream_processes,
                process_index=i,
            )
            for i in range(shared_stream_processes)
        ]
    )

    # Check redis is empty
    assert 0 == (await redis_links.stream.zcard("streams"))
    assert 0 == len(await redis_links.stream.keys("buckets~*"))
    assert 0 == len(await redis_links.stream.keys("bucket-sources~*"))
    assert 0 == len(await redis_links.stream.hgetall("attempts"))

    # Check engine have been run with expect data
    assert 200 == len(run_engine.mock_calls)


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_worker_reschedule(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    monkeypatch.setattr("mergify_engine.worker_pusher.WORKER_PROCESSING_DELAY", 3000)
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
    )

    score = (await redis_links.stream.zrange("streams", 0, -1, withscores=True))[0][1]
    planned_for = datetime.datetime.utcfromtimestamp(score)

    monkeypatch.setattr("sys.argv", ["mergify-worker-rescheduler", "other"])
    ret = await stream_cli.async_reschedule_now()
    assert ret == 1

    score_not_rescheduled = (
        await redis_links.stream.zrange("streams", 0, -1, withscores=True)
    )[0][1]
    planned_for_not_rescheduled = datetime.datetime.utcfromtimestamp(
        score_not_rescheduled
    )
    assert planned_for == planned_for_not_rescheduled

    monkeypatch.setattr("sys.argv", ["mergify-worker-rescheduler", "123"])
    ret = await stream_cli.async_reschedule_now()
    assert ret == 0

    score_rescheduled = (
        await redis_links.stream.zrange("streams", 0, -1, withscores=True)
    )[0][1]
    planned_for_rescheduled = datetime.datetime.utcfromtimestamp(score_rescheduled)
    assert planned_for > planned_for_rescheduled


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def _test_worker_stuck_shutdown(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    async def fake_engine(*args: typing.Any, **kwargs: typing.Any) -> None:
        await asyncio.sleep(10000000)

    run_engine.side_effect = fake_engine
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
        priority=worker_pusher.Priority.immediate,
    )
    await run_worker(test_timeout=2)


@pytest.mark.flaky(  # drop me when asyncio_timeout is removed from redispy and python >= 3.11.3
    reruns=5
)
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_dedicated_worker_scaleup_scaledown(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.Mock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id

    w = manager.ServiceManager(
        enabled_services={"shared-workers-spawner", "dedicated-workers-spawner"},
        worker_idle_time=0.01,
        shared_stream_tasks_per_process=3,
        delayed_refresh_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        gitter_concurrent_jobs=1,
    )
    await w.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w._shutdown()))

    tracker = []

    async def track_context(*args: typing.Any, **kwargs: typing.Any) -> None:
        tracker.append(str(logs.WORKER_ID.get(None)))

    run_engine.side_effect = track_context

    async def fake_get_subscription_dedicated(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        # 123 is always shared
        if owner_id == 123:
            sub.has_feature.return_value = False
        else:
            sub.has_feature.return_value = True
        return sub

    async def fake_get_subscription_shared(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        sub.has_feature.return_value = False
        return sub

    async def push_and_wait() -> None:
        # worker hash == 2
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(4446),
            github_types.GitHubLogin("owner-4446"),
            github_types.GitHubRepositoryIdType(4446),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(4446),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 1
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 0
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(1),
            github_types.GitHubLogin("owner-1"),
            github_types.GitHubRepositoryIdType(1),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(1),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        started_at = time.monotonic()
        while (
            (await w._redis_links.stream.zcard("streams")) > 0
        ) and time.monotonic() - started_at < 10:
            await asyncio.sleep(0.1)

    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    assert sorted(tracker) == [
        "dedicated-1",
        "dedicated-4446",
        "shared-1",
    ]
    serv = w.get_service(dedicated_workers_spawner_service.DedicatedStreamService)
    assert serv is not None
    assert set(serv._dedicated_worker_tasks.keys()) == set(
        {
            github_types.GitHubAccountIdType(1),
            github_types.GitHubAccountIdType(4446),
        }
    )

    tracker.clear()

    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    await push_and_wait()
    assert sorted(tracker) == [
        "shared-0",
        "shared-0",
        "shared-1",
        "shared-1",
        "shared-2",
        "shared-2",
    ]
    assert set(serv._dedicated_worker_tasks.keys()) == set()
    tracker.clear()

    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    await push_and_wait()
    assert sorted(tracker) == [
        "dedicated-1",
        "dedicated-1",
        "dedicated-4446",
        "dedicated-4446",
        "shared-1",
        "shared-1",
    ]
    assert set(serv._dedicated_worker_tasks.keys()) == set(
        {
            github_types.GitHubAccountIdType(1),
            github_types.GitHubAccountIdType(4446),
        }
    )
    tracker.clear()

    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    assert sorted(tracker) == [
        "shared-0",
        "shared-1",
        "shared-2",
    ]
    assert set(serv._dedicated_worker_tasks.keys()) == set()


@pytest.mark.flaky(  # drop me when asyncio_timeout is removed from redispy and python >= 3.11.3
    reruns=5
)
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_dedicated_worker_process_scaleup_scaledown(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.Mock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id

    w_dedicated = manager.ServiceManager(
        enabled_services={"dedicated-workers-spawner"},
        delayed_refresh_idle_time=0.01,
        worker_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
    )
    await w_dedicated.start()
    w_shared = manager.ServiceManager(
        enabled_services={"shared-workers-spawner"},
        shared_stream_tasks_per_process=3,
        worker_idle_time=0.01,
        delayed_refresh_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
    )
    await w_shared.start()

    request.addfinalizer(lambda: event_loop.run_until_complete(w_dedicated._shutdown()))
    request.addfinalizer(lambda: event_loop.run_until_complete(w_shared._shutdown()))

    serv_shared = w_shared.get_service(
        shared_workers_spawner_service.SharedStreamService
    )
    serv_dedicated = w_dedicated.get_service(
        dedicated_workers_spawner_service.DedicatedStreamService
    )
    assert serv_shared is not None
    assert serv_dedicated is not None

    tracker = []

    async def track_context(*args: typing.Any, **kwargs: typing.Any) -> None:
        tracker.append(str(logs.WORKER_ID.get(None)))

    run_engine.side_effect = track_context

    async def fake_get_subscription_dedicated(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        # 123 is always shared
        if owner_id == 123:
            sub.has_feature.return_value = False
        else:
            sub.has_feature.return_value = True
        return sub

    async def fake_get_subscription_shared(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        sub.has_feature.return_value = False
        return sub

    async def push_and_wait() -> None:
        # worker hash == 2
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(4446),
            github_types.GitHubLogin("owner-4446"),
            github_types.GitHubRepositoryIdType(4446),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(4446),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 1
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 0
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(1),
            github_types.GitHubLogin("owner-1"),
            github_types.GitHubRepositoryIdType(1),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(1),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        started_at = time.monotonic()
        while (
            (await w_shared._redis_links.stream.zcard("streams")) > 0
            and (await w_shared._redis_links.stream.zcard("streams")) > 0
        ) and time.monotonic() - started_at < 10:
            await asyncio.sleep(0.1)

    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    assert sorted(tracker) == [
        "dedicated-1",
        "dedicated-4446",
        "shared-1",
    ]

    assert serv_shared.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert serv_dedicated.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert set(serv_dedicated._dedicated_worker_tasks.keys()) == {1, 4446}
    tracker.clear()

    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    await push_and_wait()
    assert sorted(tracker) == [
        "shared-0",
        "shared-0",
        "shared-1",
        "shared-1",
        "shared-2",
        "shared-2",
    ]
    assert serv_shared.service_dedicated_workers_cache_syncer.owner_ids == set()
    assert serv_dedicated.service_dedicated_workers_cache_syncer.owner_ids == set()
    assert set(serv_dedicated._dedicated_worker_tasks.keys()) == set()
    tracker.clear()

    # scale up
    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    await push_and_wait()
    assert sorted(tracker) == [
        "dedicated-1",
        "dedicated-1",
        "dedicated-4446",
        "dedicated-4446",
        "shared-1",
        "shared-1",
    ]

    assert serv_shared.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert serv_dedicated.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert set(serv_dedicated._dedicated_worker_tasks.keys()) == {1, 4446}
    tracker.clear()

    # scale down
    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    assert sorted(tracker) == [
        "shared-0",
        "shared-1",
        "shared-2",
    ]
    assert serv_shared.service_dedicated_workers_cache_syncer.owner_ids == set()
    assert serv_dedicated.service_dedicated_workers_cache_syncer.owner_ids == set()
    assert set(serv_dedicated._dedicated_worker_tasks.keys()) == set()
    tracker.clear()

    # scale up
    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    await push_and_wait()
    assert sorted(tracker) == [
        "dedicated-1",
        "dedicated-1",
        "dedicated-4446",
        "dedicated-4446",
        "shared-1",
        "shared-1",
    ]
    assert serv_shared.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert serv_dedicated.service_dedicated_workers_cache_syncer.owner_ids == {1, 4446}
    assert set(serv_dedicated._dedicated_worker_tasks.keys()) == {1, 4446}
    tracker.clear()


@pytest.mark.flaky(  # drop me when asyncio_timeout is removed from redispy and python >= 3.11.3
    reruns=5
)
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_separate_dedicated_worker(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.Mock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id

    shared_w = manager.ServiceManager(
        enabled_services={"shared-workers-spawner"},
        shared_stream_tasks_per_process=3,
        worker_idle_time=0.01,
        delayed_refresh_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
    )
    await shared_w.start()

    dedicated_w = manager.ServiceManager(
        enabled_services={"dedicated-workers-spawner"},
        shared_stream_tasks_per_process=3,
        worker_idle_time=0.01,
        delayed_refresh_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
    )

    tracker = []

    async def track_context(*args: typing.Any, **kwargs: typing.Any) -> None:
        tracker.append(str(logs.WORKER_ID.get(None)))

    run_engine.side_effect = track_context

    async def fake_get_subscription_dedicated(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        # 123 is always shared
        if owner_id == 123:
            sub.has_feature.return_value = False
        else:
            sub.has_feature.return_value = True
        return sub

    async def fake_get_subscription_shared(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        sub.has_feature.return_value = False
        return sub

    async def push_and_wait(blocked_stream: int = 0) -> None:
        # worker hash == 2
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(4446),
            github_types.GitHubLogin("owner-4446"),
            github_types.GitHubRepositoryIdType(4446),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(4446),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 1
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 0
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(1),
            github_types.GitHubLogin("owner-1"),
            github_types.GitHubRepositoryIdType(1),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(1),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        started_at = time.monotonic()
        while (
            (await redis_links.stream.zcard("streams")) > blocked_stream
        ) and time.monotonic() - started_at < 10:
            await asyncio.sleep(0.1)

    get_subscription.side_effect = fake_get_subscription_dedicated

    # Start only shared worker
    await push_and_wait(2)
    # only shared is consumed
    assert sorted(tracker) == ["shared-1"]
    tracker.clear()

    shared_w.stop()
    await shared_w.wait_shutdown_complete()
    await dedicated_w.start()
    await push_and_wait(1)
    # only dedicated are consumed
    assert sorted(tracker) == ["dedicated-1", "dedicated-4446"]
    tracker.clear()

    # Start both
    await shared_w.start()
    await push_and_wait()
    assert sorted(tracker) == ["dedicated-1", "dedicated-4446", "shared-1"]
    tracker.clear()

    shared_w.stop()
    await shared_w.wait_shutdown_complete()
    dedicated_w.stop()
    await dedicated_w.wait_shutdown_complete()


@mock.patch("mergify_engine.worker.manager.ServiceManager.setup_signals")
@mock.patch("mergify_engine.worker.delayed_refresh_service.DelayedRefreshService.work")
@mock.patch(
    "mergify_engine.worker.stream_monitoring_service.MonitoringStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.dedicated_workers_spawner_service.DedicatedStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.shared_workers_spawner_service.SharedStreamService.shared_stream_worker_task"
)
@mock.patch(
    "mergify_engine.worker.manager.ServiceManager.wait_shutdown_complete",
    side_effect=stop_and_wait_worker,
    autospec=True,
)
@mock.patch(
    "mergify_engine.worker.task.TaskRetriedForever.loop_and_sleep_forever",
    autospec=True,
)
def test_worker_start_all_tasks(
    loop_and_sleep_forever: mock.Mock,
    wait_shutdown_complete: mock.Mock,
    shared_stream_worker_task: mock.Mock,
    dedicated_workers_spawner_task: mock.Mock,
    monitoring_task: mock.Mock,
    delayed_refresh_task: mock.Mock,
    setup_signals: mock.Mock,
    database_cleanup: None,
) -> None:
    loop_and_sleep_forever.side_effect = just_run_once

    manager.main([])
    while not wait_shutdown_complete.called:
        time.sleep(0.01)
    assert shared_stream_worker_task.called
    assert dedicated_workers_spawner_task.called
    assert monitoring_task.called
    assert delayed_refresh_task.called


@mock.patch("mergify_engine.worker.manager.ServiceManager.setup_signals")
@mock.patch("mergify_engine.worker.delayed_refresh_service.DelayedRefreshService.work")
@mock.patch(
    "mergify_engine.worker.stream_monitoring_service.MonitoringStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.dedicated_workers_spawner_service.DedicatedStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.shared_workers_spawner_service.SharedStreamService.shared_stream_worker_task"
)
@mock.patch(
    "mergify_engine.worker.manager.ServiceManager.wait_shutdown_complete",
    side_effect=stop_and_wait_worker,
    autospec=True,
)
@mock.patch(
    "mergify_engine.worker.task.TaskRetriedForever.loop_and_sleep_forever",
    autospec=True,
)
def test_worker_start_just_shared(
    loop_and_sleep_forever: mock.Mock,
    wait_shutdown_complete: mock.Mock,
    shared_stream_worker_task: mock.Mock,
    dedicated_workers_spawner_task: mock.Mock,
    monitoring_task: mock.Mock,
    delayed_refresh_task: mock.Mock,
    setup_signals: mock.Mock,
    database_cleanup: None,
) -> None:
    loop_and_sleep_forever.side_effect = just_run_once

    manager.main(["--enabled-services=shared-workers-spawner"])
    while not wait_shutdown_complete.called:
        time.sleep(0.01)
    assert shared_stream_worker_task.called
    assert not dedicated_workers_spawner_task.called
    assert not monitoring_task.called
    assert not delayed_refresh_task.called


@mock.patch("mergify_engine.worker.manager.ServiceManager.setup_signals")
@mock.patch("mergify_engine.worker.delayed_refresh_service.DelayedRefreshService.work")
@mock.patch(
    "mergify_engine.worker.stream_monitoring_service.MonitoringStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.dedicated_workers_spawner_service.DedicatedStreamService.work"
)
@mock.patch(
    "mergify_engine.worker.shared_workers_spawner_service.SharedStreamService.shared_stream_worker_task"
)
@mock.patch(
    "mergify_engine.worker.manager.ServiceManager.wait_shutdown_complete",
    side_effect=stop_and_wait_worker,
    autospec=True,
)
@mock.patch(
    "mergify_engine.worker.task.TaskRetriedForever.loop_and_sleep_forever",
    autospec=True,
)
def test_worker_start_except_shared(
    loop_and_sleep_forever: mock.Mock,
    wait_shutdown_complete: mock.Mock,
    shared_stream_worker_task: mock.Mock,
    dedicated_workers_spawner_task: mock.Mock,
    monitoring_task: mock.Mock,
    delayed_refresh_task: mock.Mock,
    setup_signals: mock.Mock,
    database_cleanup: None,
) -> None:
    loop_and_sleep_forever.side_effect = just_run_once

    manager.main(
        [
            "--enabled-services=dedicated-workers-spawner,stream-monitoring,delayed-refresh"
        ]
    )
    while not wait_shutdown_complete.called:
        time.sleep(0.01)
    assert not shared_stream_worker_task.called
    assert dedicated_workers_spawner_task.called
    assert monitoring_task.called
    assert delayed_refresh_task.called


async def test_get_shared_worker_ids(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    owner_id = github_types.GitHubAccountIdType(132)
    monkeypatch.setattr(manager, "_DYNO", "worker-shared.1")
    assert manager.get_process_index_from_env() == 0
    w1 = manager.ServiceManager(
        enabled_services={"shared-workers-spawner"},
        shared_stream_processes=2,
        shared_stream_tasks_per_process=30,
    )
    await w1.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w1._shutdown()))
    shared_serv1 = w1.get_service(shared_workers_spawner_service.SharedStreamService)
    assert shared_serv1 is not None
    assert shared_serv1.get_shared_worker_ids() == list(range(0, 30))
    assert shared_serv1.global_shared_tasks_count == 60
    s1 = stream.Processor(redis_links, "shared-8", None, shared_serv1._owners_cache)
    assert shared_serv1.should_handle_owner(s1, owner_id)

    monkeypatch.setattr(manager, "_DYNO", "worker-shared.2")
    assert manager.get_process_index_from_env() == 1
    w2 = manager.ServiceManager(
        enabled_services={"shared-workers-spawner"},
        shared_stream_processes=2,
        shared_stream_tasks_per_process=30,
    )
    await w2.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w2._shutdown()))
    shared_serv2 = w2.get_service(shared_workers_spawner_service.SharedStreamService)
    assert shared_serv2 is not None
    assert shared_serv2.get_shared_worker_ids() == list(range(30, 60))
    assert shared_serv2.global_shared_tasks_count == 60
    s2 = stream.Processor(redis_links, "shared-38", None, shared_serv2._owners_cache)
    assert not shared_serv2.should_handle_owner(s2, owner_id)


async def test_get_my_dedicated_worker_ids(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    owners_cache = {
        github_types.GitHubAccountIdType(123),
        github_types.GitHubAccountIdType(124),
        github_types.GitHubAccountIdType(125),
        github_types.GitHubAccountIdType(126),
        github_types.GitHubAccountIdType(127),
    }

    monkeypatch.setattr(manager, "_DYNO", "worker-dedicated.1")
    assert manager.get_process_index_from_env() == 0
    w1 = manager.ServiceManager(
        enabled_services={"shared-workers-spawner", "dedicated-workers-spawner"},
        shared_stream_processes=0,
        dedicated_stream_processes=2,
    )
    await w1.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w1._shutdown()))
    dedicated_serv1 = w1.get_service(
        dedicated_workers_spawner_service.DedicatedStreamService
    )
    assert dedicated_serv1 is not None
    dedicated_serv1.service_dedicated_workers_cache_syncer.owner_ids = owners_cache
    assert dedicated_serv1.get_my_dedicated_worker_ids_from_cache() == {
        github_types.GitHubAccountIdType(123),
        github_types.GitHubAccountIdType(125),
        github_types.GitHubAccountIdType(127),
    }

    monkeypatch.setattr(manager, "_DYNO", "worker-dedicated.2")
    assert manager.get_process_index_from_env() == 1
    w2 = manager.ServiceManager(
        enabled_services={"shared-workers-spawner", "dedicated-workers-spawner"},
        shared_stream_processes=0,
        dedicated_stream_processes=2,
    )
    await w2.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w2._shutdown()))
    dedicated_serv2 = w2.get_service(
        dedicated_workers_spawner_service.DedicatedStreamService
    )
    assert dedicated_serv2 is not None
    dedicated_serv2.service_dedicated_workers_cache_syncer.owner_ids = owners_cache
    assert dedicated_serv2.get_my_dedicated_worker_ids_from_cache() == {
        github_types.GitHubAccountIdType(124),
        github_types.GitHubAccountIdType(126),
    }


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_processor_ignoring_error_422(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    stream_processor: stream.Processor,
    fake_installation: context.Installation,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id
    get_subscription.side_effect = fake_get_subscription

    response = mock.Mock()
    response.status_code = 422
    response.json.return_value = {"message": "No commit found for SHA: xyz"}
    message = "422 Client Error: Unprocessable Entity for url `https://api.github.com/repos/owner/repo/check-runs`"
    error_422 = http.HTTPClientSideError(
        message=message,
        request=response.request,
        response=response,
    )
    run_engine.side_effect = error_422

    with pytest.raises(stream.IgnoredException):
        async with stream_processor._translate_exception_to_retries(
            stream_lua.BucketOrgKeyType("stream~owner~123")
        ):
            await stream.run_engine(
                fake_installation,
                github_types.GitHubRepositoryIdType(123),
                github_types.GitHubRepositoryName("repo"),
                github_types.GitHubPullRequestNumber(1234),
                [],
            )


@pytest.mark.parametrize(
    "priority,timestamp",
    (
        (worker_pusher.Priority.immediate, "2020-01-12T22:42:00"),
        (worker_pusher.Priority.high, "2020-01-14T20:02:03"),
        (worker_pusher.Priority.low, "2070-07-12T02:42:53"),
        (worker_pusher.Priority.medium, "1990-08-24T20:48:26"),
    ),
)
def test_score_priority_helpers(
    priority: worker_pusher.Priority, timestamp: str
) -> None:
    with freeze_time(timestamp) as frozen_time:
        score = float(worker_pusher.get_priority_score(priority))
        assert worker_pusher.get_priority_level_from_score(score) == priority
        assert worker_pusher.get_date_from_score(score) == frozen_time().replace(
            tzinfo=datetime.UTC
        )


@pytest.mark.flaky(  # drop me when asyncio_timeout is removed from redispy and python >= 3.11.3
    reruns=5
)
@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_dedicated_multiple_processes(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.Mock,
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    get_installation_from_account_id.side_effect = fake_get_installation_from_account_id

    w_shared = manager.ServiceManager(
        enabled_services={"shared-workers-spawner"},
        shared_stream_tasks_per_process=3,
        worker_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        dedicated_stream_processes=0,
        process_index=0,
    )
    await w_shared.start()
    w1 = manager.ServiceManager(
        enabled_services={"dedicated-workers-spawner"},
        shared_stream_tasks_per_process=0,
        worker_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        dedicated_stream_processes=2,
        process_index=0,
    )
    await w1.start()

    w2 = manager.ServiceManager(
        enabled_services={"dedicated-workers-spawner"},
        shared_stream_tasks_per_process=0,
        delayed_refresh_idle_time=0.01,
        worker_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        dedicated_stream_processes=2,
        process_index=1,
    )
    await w2.start()

    request.addfinalizer(lambda: event_loop.run_until_complete(w_shared._shutdown()))
    request.addfinalizer(lambda: event_loop.run_until_complete(w1._shutdown()))
    request.addfinalizer(lambda: event_loop.run_until_complete(w2._shutdown()))

    serv_dedicated_of_shared = w_shared.get_service(
        dedicated_workers_spawner_service.DedicatedStreamService
    )
    assert serv_dedicated_of_shared is None
    serv1 = w1.get_service(dedicated_workers_spawner_service.DedicatedStreamService)
    assert serv1 is not None
    serv2 = w2.get_service(dedicated_workers_spawner_service.DedicatedStreamService)
    assert serv2 is not None

    async def fake_get_subscription_dedicated(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        # 123 is always shared
        if owner_id == 123:
            sub.has_feature.return_value = False
        else:
            sub.has_feature.return_value = True
        return sub

    async def fake_get_subscription_shared(
        redis: redis_utils.RedisStream, owner_id: github_types.GitHubAccountIdType
    ) -> mock.Mock:
        sub = mock.Mock()
        sub.has_feature.return_value = False
        return sub

    async def push_and_wait() -> None:
        # worker hash == 2
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(4446),
            github_types.GitHubLogin("owner-4446"),
            github_types.GitHubRepositoryIdType(4446),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(4446),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 1
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(123),
            github_types.GitHubLogin("owner-123"),
            github_types.GitHubRepositoryIdType(123),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(123),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        # worker hash == 0
        await worker_pusher.push(
            redis_links.stream,
            github_types.GitHubAccountIdType(1),
            github_types.GitHubLogin("owner-1"),
            github_types.GitHubRepositoryIdType(1),
            github_types.GitHubRepositoryName("repo"),
            github_types.GitHubPullRequestNumber(1),
            "pull_request",
            github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.immediate,
        )
        started_at = time.monotonic()
        while (
            await w_shared._redis_links.stream.zcard("streams")
        ) > 0 and time.monotonic() - started_at < 10:
            await asyncio.sleep(0.1)

    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    await push_and_wait()
    await push_and_wait()
    assert set(serv1._dedicated_worker_tasks.keys()) == {
        github_types.GitHubAccountIdType(1)
    }
    assert set(serv2._dedicated_worker_tasks.keys()) == {
        github_types.GitHubAccountIdType(4446)
    }

    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    await push_and_wait()
    await push_and_wait()
    assert set(serv1._dedicated_worker_tasks.keys()) == set()
    assert set(serv2._dedicated_worker_tasks.keys()) == set()

    get_subscription.side_effect = fake_get_subscription_dedicated
    await push_and_wait()
    await push_and_wait()
    await push_and_wait()
    assert set(serv1._dedicated_worker_tasks.keys()) == {
        github_types.GitHubAccountIdType(1)
    }
    assert set(serv2._dedicated_worker_tasks.keys()) == {
        github_types.GitHubAccountIdType(4446)
    }

    get_subscription.side_effect = fake_get_subscription_shared
    await push_and_wait()
    await push_and_wait()
    await push_and_wait()
    assert set(serv1._dedicated_worker_tasks.keys()) == set()
    assert set(serv2._dedicated_worker_tasks.keys()) == set()


async def test_start_stop_cycle(
    redis_links: redis_utils.RedisLinks,
    logger_checker: None,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
    setup_database: None,
) -> None:
    w = manager.ServiceManager(
        shared_stream_tasks_per_process=3,
        worker_idle_time=0.01,
        dedicated_workers_spawner_idle_time=0.01,
        dedicated_workers_cache_syncer_idle_time=0.01,
        dedicated_stream_processes=1,
        process_index=0,
        gitter_concurrent_jobs=2,
        ci_event_processing_idle_time=0.01,
        log_embedder_idle_time=0.01,
    )
    assert w._stopped.is_set()
    assert w._stop_task is None

    await w.start()

    # NOTE(sileht): ensure it doesn't crash instantly
    await asyncio.sleep(1)

    assert len(w._services) == 9

    tasks = [a_task for serv in w._services for a_task in serv.tasks]
    assert len(tasks) == 13

    serv_shared = w.get_service(shared_workers_spawner_service.SharedStreamService)
    assert serv_shared is not None
    serv_dedicated = w.get_service(
        dedicated_workers_spawner_service.DedicatedStreamService
    )
    assert serv_dedicated is not None
    assert w.get_service(stream_monitoring_service.MonitoringStreamService) is not None
    assert w.get_service(delayed_refresh_service.DelayedRefreshService) is not None
    serv_gitter = w.get_service(gitter_service.GitterService)
    assert serv_gitter is not None

    assert len(serv_shared._shared_worker_tasks) == 3
    assert len(serv_dedicated._dedicated_worker_tasks) == 0
    assert len(serv_gitter._pools) == 2

    assert not w._stopped.is_set()
    assert w._stop_task is None

    w.stop()

    assert not w._stopped.is_set()
    assert w._stop_task is not None

    # https://github.com/python/mypy/issues/11969
    await w.wait_shutdown_complete()  # type: ignore[unreachable]

    assert len(w._services) == 0
    assert w._stopped.is_set()
    assert w._stop_task is None


async def test_task_unexpected_cancellation(
    logger_checker: pytest.LogCaptureFixture,
) -> None:
    counter = 0

    async def wait() -> None:
        nonlocal counter
        counter += 1
        await asyncio.sleep(1000)

    t = task.TaskRetriedForever("foo", wait, 0)

    # NOTE(sileht): schedule the task
    await asyncio.sleep(0)

    assert counter == 1
    assert [r.message for r in logger_checker.get_records(when="call")] == [
        "foo starting",
    ]
    assert not t.task.done()

    # ignored
    t.task.cancel()
    await asyncio.sleep(0)
    assert not t.task.done()
    assert counter == 2
    assert [r.message for r in logger_checker.get_records(when="call")] == [
        "foo starting",
        "foo task unexpectedly cancelled, ignoring",
    ]

    # ignored
    t.task.cancel()
    await asyncio.sleep(0)
    assert not t.task.done()
    assert counter == 3
    assert [r.message for r in logger_checker.get_records(when="call")] == [
        "foo starting",
        "foo task unexpectedly cancelled, ignoring",
        "foo task unexpectedly cancelled, ignoring",
    ]

    # stopped
    await task.stop_and_wait([t])
    assert t.task.done()
    assert counter == 3
    assert [r.message for r in logger_checker.get_records(when="call")] == [
        "foo starting",
        "foo task unexpectedly cancelled, ignoring",
        "foo task unexpectedly cancelled, ignoring",
        "tasks stopping",
        "foo task exited",
        "tasks stopped",
    ]
