import asyncio
from collections import abc
import copy
import datetime
import itertools
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import typing
import unittest
from unittest import mock
from urllib import parse

import daiquiri
from first import first
import httpx
import pytest

from mergify_engine import branch_updater
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import duplicate_pull
from mergify_engine import github_graphql_types
from mergify_engine import github_types
from mergify_engine import gitter
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.queue import merge_train
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import conftest as func_conftest
from mergify_engine.worker import gitter_service
from mergify_engine.worker import manager
from mergify_engine.worker import stream
from mergify_engine.worker import stream_lua
from mergify_engine.worker import stream_services
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)
RECORD = bool(os.getenv("MERGIFYENGINE_RECORD", False))
RECORDING_IN_PARALLEL = bool(os.getenv("RECORD_PARALLEL", False))
FAKE_DATA = "whatdataisthat"
FAKE_HMAC = utils.compute_hmac(FAKE_DATA.encode("utf8"), config.WEBHOOK_SECRET)


class MergeQueueCarMatcher(typing.NamedTuple):
    user_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    initial_current_base_sha: github_types.SHAType
    checks_type: merge_train.TrainCarChecksType | None
    queue_pull_request_number: github_types.GitHubPullRequestNumber | None


class ForwardedEvent(typing.TypedDict):
    payload: github_types.GitHubEvent
    type: github_types.GitHubEventType
    id: str


class RecordException(typing.TypedDict):
    returncode: int
    output: str


# mypy doesn't understand a key being "optional" in a TypedDict.
# The only workaround is to make a TypedDict with the optional key(s)
# and pass `total=False`, then inherit this class.
class RecordExc(typing.TypedDict, total=False):
    exc: RecordException


class Record(RecordExc):
    args: list[typing.Any]
    kwargs: dict[typing.Any, typing.Any]
    out: str


class GitterRecorder(gitter.Gitter):
    def __init__(
        self,
        logger: "logging.LoggerAdapter[logging.Logger]",
        cassette_library_dir: str,
        suffix: str,
    ) -> None:
        super().__init__(logger)
        self.cassette_path = os.path.join(cassette_library_dir, f"git-{suffix}.json")
        if RECORD:
            self.records: list[Record] = []
        else:
            self.load_records()

    def load_records(self) -> None:
        if not os.path.exists(self.cassette_path):
            raise RuntimeError(f"Cassette {self.cassette_path} not found")
        with open(self.cassette_path, "rb") as f:
            data = f.read().decode("utf8")
            self.records = json.loads(data)

    def save_records(self) -> None:
        with open(self.cassette_path, "wb") as f:
            data = json.dumps(self.records)
            f.write(data.encode("utf8"))

    async def __call__(self, *args: typing.Any, **kwargs: typing.Any) -> str:
        if RECORD:
            try:
                output = await super().__call__(*args, **kwargs)
            except gitter.GitError as e:
                self.records.append(
                    {
                        "args": self.prepare_args(args),
                        "kwargs": self.prepare_kwargs(kwargs),
                        "exc": {
                            "returncode": e.returncode,
                            "output": e.output,
                        },
                        "out": "",
                    }
                )
                raise
            else:
                self.records.append(
                    {
                        "args": self.prepare_args(args),
                        "kwargs": self.prepare_kwargs(kwargs),
                        "out": output,
                    }
                )
            return output
        else:
            r = self.records.pop(0)
            if "exc" in r:
                raise gitter.GitError(
                    returncode=r["exc"]["returncode"],
                    output=r["exc"]["output"],
                )
            else:
                assert r["args"] == self.prepare_args(
                    args
                ), f'{r["args"]} != {self.prepare_args(args)}'
                assert r["kwargs"] == self.prepare_kwargs(
                    kwargs
                ), f'{r["kwargs"]} != {self.prepare_kwargs(kwargs)}'
                return r["out"]

    def prepare_args(self, args: typing.Any) -> list[str]:
        prepared_args = [
            arg.replace(self.tmp, "/tmp/mergify-gitter<random>") for arg in args
        ]
        if "user.signingkey" in prepared_args:
            prepared_args = [
                arg.replace(config.TESTING_ID_GPGKEY_SECRET, "<SECRET>") for arg in args
            ]
        return prepared_args

    @staticmethod
    def prepare_kwargs(kwargs: typing.Any) -> typing.Any:
        if "_input" in kwargs:
            kwargs["_input"] = re.sub(r"://[^@]*@", "://<TOKEN>:@", kwargs["_input"])
        if "_env" in kwargs:
            kwargs["_env"] = {"envtest": "this is a test"}
        return kwargs

    async def cleanup(self) -> None:
        await super().cleanup()
        if RECORD:
            self.save_records()


class MissingEventTimeout(Exception):
    def __init__(
        self, event_type: github_types.GitHubEventType, expected_payload: typing.Any
    ) -> None:
        return super().__init__(
            f"Never got event `{event_type}` with payload `{expected_payload}` (timeout)"
        )


class EventReader:
    def __init__(
        self,
        app: httpx.AsyncClient,
        integration_id: int,
        repository_id: github_types.GitHubRepositoryIdType,
        test_name: str,
    ) -> None:
        self._app = app
        self._session = http.AsyncClient()
        self._handled_events: asyncio.Queue[ForwardedEvent] = asyncio.Queue()
        self._counter = 0

        hostname = parse.urlparse(config.GITHUB_URL).hostname
        self.base_event_forwarder_url = f"{config.TESTING_FORWARDER_ENDPOINT}/events/{hostname}/{integration_id}/{repository_id}/"
        self.test_name = test_name.replace("/", "-")

    def get_events_forwarder_url(self, test_id: str | None = None) -> str:
        if test_id is None:
            test_id = self.test_name

        return f"{self.base_event_forwarder_url}{test_id}"

    async def aclose(self) -> None:
        await self.drain()
        await self._session.aclose()

    async def drain(self) -> None:
        # NOTE(sileht): Drop any pending events still on the server
        r = await self._session.request(
            "DELETE",
            self.get_events_forwarder_url(),
            content=FAKE_DATA,
            headers={"X-Hub-Signature": "sha1=" + FAKE_HMAC},
        )
        r.raise_for_status()

    EVENTS_POLLING_INTERVAL_SECONDS = 0.20

    async def wait_for(
        self,
        event_type: github_types.GitHubEventType,
        expected_payload: typing.Any,
        timeout: float = 15 if RECORD else 2,
        forward_to_engine: bool = True,
        test_id: str | None = None,
    ) -> github_types.GitHubEvent:
        LOG.log(
            42,
            "WAITING FOR %s/%s: %s",
            event_type,
            expected_payload.get("action"),
            expected_payload,
        )

        started_at = time.monotonic()
        while time.monotonic() - started_at < timeout:
            try:
                event = self._handled_events.get_nowait()
                await self._process_event(event, forward_to_engine)
            except asyncio.QueueEmpty:
                for event in await self._get_events(test_id=test_id):
                    await self._handled_events.put(event)
                else:
                    await asyncio.sleep(
                        self.EVENTS_POLLING_INTERVAL_SECONDS if RECORD else 0
                    )
                continue

            if event["type"] == event_type and self._match(
                event["payload"], expected_payload
            ):
                return event["payload"]

        raise MissingEventTimeout(event_type, expected_payload)

    def _match(self, data: github_types.GitHubEvent, expected_data: typing.Any) -> bool:
        if isinstance(expected_data, dict):
            for key, expected in expected_data.items():
                if key not in data:
                    return False
                if not self._match(data[key], expected):  # type: ignore[literal-required]
                    return False
            return True
        else:
            return bool(data == expected_data)

    async def _get_events(self, test_id: str | None = None) -> list[ForwardedEvent]:
        # NOTE(sileht): we use a counter to make each call unique in cassettes
        self._counter += 1
        return typing.cast(
            list[ForwardedEvent],
            (
                await self._session.request(
                    "GET",
                    f"{self.get_events_forwarder_url(test_id)}?counter={self._counter}",
                    content=FAKE_DATA,
                    headers={"X-Hub-Signature": "sha1=" + FAKE_HMAC},
                )
            ).json(),
        )

    async def _process_event(self, event: typing.Any, forward_to_engine: bool) -> None:
        payload = event["payload"]
        if event["type"] in ["check_run", "check_suite"]:
            extra = (
                f"/{payload[event['type']].get('status')}"
                f"/{payload[event['type']].get('conclusion')}"
            )
        elif event["type"] == "status":
            extra = f"/{payload.get('state')}"
        else:
            extra = ""

        LOG.log(
            42,
            "EVENT RECEIVED %s/%s%s: %s",
            event["type"],
            payload.get("action"),
            extra,
            self._remove_useless_links(copy.deepcopy(event)),
        )
        if forward_to_engine:
            await self._app.post(
                "/event",
                headers={
                    "X-GitHub-Event": event["type"],
                    "X-GitHub-Delivery": "123456789",
                    "X-Hub-Signature": "sha1=whatever",
                    "Content-type": "application/json",
                },
                content=json.dumps(payload),
            )

    def _remove_useless_links(self, data: typing.Any) -> typing.Any:
        if isinstance(data, dict):
            data.pop("installation", None)
            data.pop("sender", None)
            data.pop("repository", None)
            data.pop("id", None)
            data.pop("node_id", None)
            data.pop("tree_id", None)
            data.pop("_links", None)
            data.pop("user", None)
            data.pop("body", None)
            data.pop("after", None)
            data.pop("before", None)
            data.pop("app", None)
            data.pop("timestamp", None)
            data.pop("external_id", None)
            if "organization" in data:
                data["organization"].pop("description", None)
            if "check_run" in data:
                data["check_run"].pop("checks_suite", None)
            for key, value in list(data.items()):
                if key.endswith("url"):
                    del data[key]
                elif key.endswith("_at"):
                    del data[key]
                else:
                    data[key] = self._remove_useless_links(value)
            return data
        elif isinstance(data, list):
            return [self._remove_useless_links(elem) for elem in data]
        else:
            return data


@pytest.mark.usefixtures("logger_checker", "unittest_asyncio_glue")
class IsolatedAsyncioTestCaseWithPytestAsyncioGlue(unittest.IsolatedAsyncioTestCase):
    def _setupAsyncioRunner(self) -> None:
        # NOTE(sileht): py311 unittest internal interface
        # We reuse the event loop created by pytest-asyncio
        self._asyncioRunner = asyncio.Runner(
            debug=True,
            loop_factory=lambda: self.pytest_event_loop,  # type: ignore[attr-defined,no-any-return]
        )

    def _tearDownAsyncioRunner(self) -> None:
        # NOTE(sileht): py311 unittest internal interface
        # We don't run the loop.close() of unittest and let pytest doing it
        pass


@pytest.mark.usefixtures("logger_checker", "unittest_glue")
class FunctionalTestBase(IsolatedAsyncioTestCaseWithPytestAsyncioGlue):
    # Compat mypy/pytest fixtures
    app: httpx.AsyncClient
    RECORD_CONFIG: func_conftest.RecordConfigType
    subscription: subscription.Subscription
    cassette_library_dir: str
    api_key_admin: str

    # NOTE(sileht): The repository have been manually created in mergifyio-testing
    # organization and then forked in mergify-test2 user account
    FORK_PERSONAL_TOKEN = config.EXTERNAL_USER_PERSONAL_TOKEN
    SUBSCRIPTION_ACTIVE = False

    # To run tests on private repository, you can use:
    # FORK_PERSONAL_TOKEN = config.ORG_USER_PERSONAL_TOKEN
    # SUBSCRIPTION_ACTIVE = True

    WAIT_TIME_BEFORE_TEARDOWN = 0.20
    WORKER_IDLE_SLEEP_TIME = 0.20

    # NOTE(Syffe): If too low (previously 0.02), this value can cause some tests using
    # delayed-refreshes to be flaky
    WORKER_HAS_WORK_INTERVAL_CHECK = 0.04

    def register_mock(self, mock_obj: typing.Any) -> None:
        mock_obj.start()
        self.addCleanup(mock_obj.stop)

    async def asyncSetUp(self) -> None:
        super().setUp()

        # NOTE(sileht): don't preempted bucket consumption
        # Otherwise preemption doesn't occur at the same moment during record
        # and replay. Making some tests working during record and failing
        # during replay.
        config.BUCKET_PROCESSING_MAX_SECONDS = 100000

        config.API_ENABLE = True

        self.register_mock(
            mock.patch.object(
                constants,
                "NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST",
                datetime.timedelta(seconds=0),
            )
        )
        self.register_mock(
            mock.patch.object(
                constants,
                "MIN_DELAY_BETWEEN_SAME_PULL_REQUEST",
                datetime.timedelta(seconds=0),
            )
        )

        self.created_branches: set[github_types.GitHubRefType] = set()
        self.existing_labels: list[str] = []
        self.pr_counter: int = 0
        self.git_counter: int = 0

        self.register_mock(mock.patch.object(branch_updater.gitter, "Gitter", self.get_gitter))  # type: ignore[attr-defined]
        self.register_mock(mock.patch.object(duplicate_pull.gitter, "Gitter", self.get_gitter))  # type: ignore[attr-defined]

        self.main_branch_name = github_types.GitHubRefType(
            self.get_full_branch_name("main")
        )
        # ########## MOCK MERGE_QUEUE_BRANCH_PREFIX
        # To easily delete all branches created by any queue-related
        # stuff.

        self.mock_merge_queue_branch_prefix = mock.patch.object(
            constants,
            "MERGE_QUEUE_BRANCH_PREFIX",
            f"{self._testMethodName[:50]}/mq/",
        )
        self.register_mock(self.mock_merge_queue_branch_prefix)
        # ##############################

        # ########## MOCK EXTRACT DEFAULT BRANCH
        def mock_extract_default_branch(
            repository: github_types.GitHubRepository,
        ) -> github_types.GitHubRefType:
            if repository["id"] == self.RECORD_CONFIG["repository_id"]:
                return self.main_branch_name

            return repository["default_branch"]

        self.register_mock(
            mock.patch.object(
                utils,
                "extract_default_branch",
                mock_extract_default_branch,
            )
        )
        # ##############################

        # Web authentification always pass
        self.register_mock(mock.patch("hmac.compare_digest", return_value=True))

        signals.register()
        self.addCleanup(signals.unregister)

        self.git = self.get_gitter(LOG)
        await self.git.init()
        self.addAsyncCleanup(self.git.cleanup)

        self.redis_links = redis_utils.RedisLinks(name="functional-fixture")
        await self.redis_links.flushall()

        installation_json = await github.get_installation_from_account_id(
            config.TESTING_ORGANIZATION_ID
        )

        self.client_integration = github.aget_client(installation_json)
        self.client_admin = github.AsyncGithubInstallationClient(
            auth=github.GithubTokenAuth(
                token=config.ORG_ADMIN_PERSONAL_TOKEN,
            )
        )
        self.client_fork = github.AsyncGithubInstallationClient(
            auth=github.GithubTokenAuth(
                token=self.FORK_PERSONAL_TOKEN,
            )
        )
        self.addAsyncCleanup(self.client_integration.aclose)
        self.addAsyncCleanup(self.client_admin.aclose)
        self.addAsyncCleanup(self.client_fork.aclose)

        await self.client_admin.item("/user")
        await self.client_fork.item("/user")

        self.url_origin = (
            f"/repos/mergifyio-testing/{self.RECORD_CONFIG['repository_name']}"
        )
        self.url_fork = f"/repos/mergify-test2/{self.RECORD_CONFIG['repository_name']}"
        self.git_origin = f"{config.GITHUB_URL}/mergifyio-testing/{self.RECORD_CONFIG['repository_name']}"
        self.git_fork = (
            f"{config.GITHUB_URL}/mergify-test2/{self.RECORD_CONFIG['repository_name']}"
        )

        self.installation_ctxt = context.Installation(
            installation_json,
            self.subscription,
            self.client_integration,
            self.redis_links,
        )
        self.repository_ctxt = await self.installation_ctxt.get_repository_by_id(
            github_types.GitHubRepositoryIdType(self.RECORD_CONFIG["repository_id"])
        )

        # NOTE(sileht): We mock this method because when we replay test, the
        # timing maybe not the same as when we record it, making the formatted
        # elapsed time different in the merge queue summary.
        def fake_pretty_datetime(dt: datetime.datetime) -> str:
            return "<fake_pretty_datetime()>"

        self.register_mock(
            mock.patch(
                "mergify_engine.date.pretty_datetime",
                side_effect=fake_pretty_datetime,
            )
        )

        self._event_reader = EventReader(
            self.app,
            self.RECORD_CONFIG["integration_id"],
            self.RECORD_CONFIG["repository_id"],
            self.get_full_branch_name(),
        )
        await self._event_reader.drain()

        # Track when worker work
        real_consume_method = stream.Processor.consume

        self.worker_concurrency_works = 0

        async def tracked_consume(
            inner_self: stream.Processor,
            bucket_org_key: stream_lua.BucketOrgKeyType,
            owner_id: github_types.GitHubAccountIdType,
            owner_login_for_tracing: github_types.GitHubLoginForTracing,
        ) -> None:
            self.worker_concurrency_works += 1
            try:
                await real_consume_method(
                    inner_self, bucket_org_key, owner_id, owner_login_for_tracing
                )
            finally:
                self.worker_concurrency_works -= 1

        stream.Processor.consume = tracked_consume  # type: ignore[assignment]

        def cleanup_consume() -> None:
            stream.Processor.consume = real_consume_method  # type: ignore[assignment]

        self.addCleanup(cleanup_consume)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()

        # NOTE(sileht): Wait a bit to ensure all remaining events arrive.
        if RECORD:
            await asyncio.sleep(self.WAIT_TIME_BEFORE_TEARDOWN)

            current_test_branches = [
                github_types.GitHubRefType(ref["ref"].replace("refs/heads/", ""))
                async for ref in self.find_git_refs(
                    self.url_origin, [self.main_branch_name]
                )
            ]
            for branch_name in self.created_branches.union(current_test_branches):
                try:
                    branch = await self.get_branch(branch_name)
                except http.HTTPNotFound:
                    continue

                if branch["protected"]:
                    await self.branch_protection_unprotect(branch["name"])

                try:
                    await self.client_integration.delete(
                        f"{self.url_origin}/git/refs/heads/{parse.quote(branch['name'])}"
                    )
                except http.HTTPNotFound:
                    continue

        await self.app.aclose()

        await self._event_reader.aclose()
        await self.redis_links.flushall()
        await self.redis_links.shutdown_all()

    async def wait_for(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> github_types.GitHubEvent:
        if RECORDING_IN_PARALLEL:
            # Since in parallel we might create way more content than
            # in non-record, some events might be missed if we do not wait
            # enough time.
            kwargs.setdefault("timeout", 180)

        return await self._event_reader.wait_for(*args, **kwargs)

    async def wait_for_pull_request(
        self,
        action: github_types.GitHubEventPullRequestActionType,
        pr_number: github_types.GitHubPullRequestNumber | None = None,
    ) -> github_types.GitHubEventPullRequest:
        wait_for_payload: dict[
            str,
            github_types.GitHubEventPullRequestActionType
            | github_types.GitHubPullRequestNumber,
        ] = {"action": action}
        if pr_number is not None:
            wait_for_payload["number"] = pr_number

        return typing.cast(
            github_types.GitHubEventPullRequest,
            await self.wait_for("pull_request", wait_for_payload),
        )

    async def wait_for_issue_comment(
        self,
        test_id: str,
        action: github_types.GitHubEventIssueCommentActionType,
    ) -> github_types.GitHubEventIssueComment:
        return typing.cast(
            github_types.GitHubEventIssueComment,
            await self.wait_for("issue_comment", {"action": action}, test_id=test_id),
        )

    async def wait_for_check_run(
        self,
        action: github_types.GitHubCheckRunActionType | None = None,
        status: github_types.GitHubCheckRunStatus | None = None,
        conclusion: github_types.GitHubCheckRunConclusion | None = None,
        name: str | None = None,
    ) -> github_types.GitHubEventCheckRun:
        if not action and not status and not conclusion and not name:
            raise RuntimeError(
                "Need at least one of `action`, `status`, `conclusion` or `name` when waiting for `check_run` event"
            )

        wait_for_payload: dict[str, typing.Any] = {}
        if action:
            wait_for_payload["action"] = action
        if status or conclusion or name:
            wait_for_payload["check_run"] = {}
            if status:
                wait_for_payload["check_run"]["status"] = status
            if conclusion:
                wait_for_payload["check_run"]["conclusion"] = conclusion
            if name:
                wait_for_payload["check_run"]["name"] = name

        return typing.cast(
            github_types.GitHubEventCheckRun,
            await self.wait_for(
                "check_run",
                wait_for_payload,
            ),
        )

    async def wait_for_pull_request_review(
        self, state: github_types.GitHubEventReviewStateType
    ) -> github_types.GitHubEventPullRequestReview:
        return typing.cast(
            github_types.GitHubEventPullRequestReview,
            await self.wait_for("pull_request_review", {"review": {"state": state}}),
        )

    async def run_full_engine(self) -> None:
        LOG.log(42, "RUNNING FULL ENGINE")
        w = manager.ServiceManager(
            idle_sleep_time=self.WORKER_IDLE_SLEEP_TIME if RECORD else 0.01,
            enabled_services={
                "shared-stream",
                "dedicated-stream",
                "delayed-refresh",
                "gitter",
            },
            delayed_refresh_idle_time=0.01,
            dedicated_workers_spawner_idle_time=0.01,
            dedicated_workers_syncer_idle_time=0.01,
            retry_handled_exception_forever=False,
            gitter_concurrent_jobs=1,
            shutdown_timeout=0,
        )
        await w.start()
        gitter_serv = w.get_service(gitter_service.GitterService)
        assert gitter_serv is not None

        # Ensure delayed_refresh and monitoring task run at least once
        await asyncio.sleep(self.WORKER_HAS_WORK_INTERVAL_CHECK)

        while (
            (await w._redis_links.stream.zcard("streams")) > 0
            or self.worker_concurrency_works > 0
            or len(gitter_serv._jobs) > 0
        ):
            await asyncio.sleep(self.WORKER_HAS_WORK_INTERVAL_CHECK)

        w.stop()
        await w.wait_shutdown_complete()

    async def run_engine(self) -> None:
        LOG.log(42, "RUNNING ENGINE")

        gitter_serv = gitter_service.GitterService(
            concurrent_jobs=0,
            idle_sleep_time=0.01,
            monitoring_idle_time=60,
        )
        syncer_service = stream_services.DedicatedWorkersCacheSyncerService(
            self.redis_links, dedicated_workers_syncer_idle_time=0.01
        )
        shared_service = stream_services.SharedStreamService(
            self.redis_links,
            dedicated_workers_cache_syncer=syncer_service,
            process_index=0,
            idle_sleep_time=0,
            shared_stream_processes=1,
            shared_stream_tasks_per_process=0,
            retry_handled_exception_forever=False,
        )
        dedicated_service = stream_services.DedicatedStreamService(
            self.redis_links,
            dedicated_workers_cache_syncer=syncer_service,
            process_index=0,
            idle_sleep_time=0,
            dedicated_workers_shutdown_timeout=0,
            dedicated_stream_processes=0,
            dedicated_workers_spawner_idle_time=0,
            retry_handled_exception_forever=False,
        )

        shared_service.shared_stream_tasks_per_process = 1

        while (await self.redis_links.stream.zcard("streams")) > 0:
            await shared_service.shared_stream_worker_task(0)
            await dedicated_service.dedicated_stream_worker_task(
                config.TESTING_ORGANIZATION_ID
            )
            while not gitter_serv._queue.empty():
                await gitter_serv._gitter_worker(0)

        await task.stop_wait_and_kill(
            syncer_service.tasks
            + dedicated_service.tasks
            + shared_service.tasks
            + gitter_serv.tasks,
            timeout=0,
        )

    def get_gitter(
        self, logger: "logging.LoggerAdapter[logging.Logger]"
    ) -> GitterRecorder:
        self.git_counter += 1
        return GitterRecorder(logger, self.cassette_library_dir, str(self.git_counter))

    async def setup_repo(
        self,
        mergify_config: str | None = None,
        test_branches: abc.Iterable[str] | None = None,
        files: dict[str, str] | None = None,
        forward_to_engine: bool = False,
    ) -> None:
        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        if test_branches is None:
            test_branches = []
        if files is None:
            files = {}

        self.mergify_bot = await github.GitHubAppInfo.get_bot(
            self.redis_links.cache_bytes
        )

        await self.git.configure(self.redis_links.cache_bytes)
        await self.git.add_cred(
            config.ORG_ADMIN_PERSONAL_TOKEN,
            "",
            f"mergifyio-testing/{self.RECORD_CONFIG['repository_name']}",
        )
        await self.git.add_cred(
            self.FORK_PERSONAL_TOKEN,
            "",
            f"mergify-test2/{self.RECORD_CONFIG['repository_name']}",
        )
        await self.git("remote", "add", "origin", self.git_origin)
        await self.git("remote", "add", "fork", self.git_fork)

        if mergify_config is None:
            with open(self.git.repository + "/.gitkeep", "w") as f:
                f.write("repo must not be empty")
            await self.git("add", ".gitkeep")
        else:
            with open(self.git.repository + "/.mergify.yml", "w") as f:
                f.write(mergify_config)
            await self.git("add", ".mergify.yml")

        if files:
            await self._git_create_files(files)

        await self.git("commit", "--no-edit", "-m", "initial commit")
        await self.git("branch", "-M", self.main_branch_name)

        branches_to_push = [str(self.main_branch_name)]
        self.created_branches.add(self.main_branch_name)
        for test_branch in test_branches:
            await self.git("branch", test_branch, self.main_branch_name)
            branches_to_push.append(test_branch)
            self.created_branches.add(github_types.GitHubRefType(test_branch))

        await self.git("push", "--quiet", "origin", *branches_to_push)
        for _ in branches_to_push:
            await self.wait_for("push", {}, forward_to_engine=forward_to_engine)

    def get_full_branch_name(self, name: str | None = None) -> str:
        if name is not None:
            return (
                f"{self.RECORD_CONFIG['branch_prefix']}/{self._testMethodName}/{name}"
            )

        return f"{self.RECORD_CONFIG['branch_prefix']}/{self._testMethodName}"

    async def create_pr(
        self,
        base: str | None = None,
        files: dict[str, str] | None = None,
        two_commits: bool = False,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
        branch: str | None = None,
        message: str | None = None,
        draft: bool = False,
        git_tree_ready: bool = False,
        verified: bool = False,
        commit_headline: str | None = None,
        commit_body: str | None = None,
        commit_date: datetime.datetime | None = None,
    ) -> github_types.GitHubPullRequest:
        self.pr_counter += 1

        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        if as_ == "fork":
            remote = "fork"
        else:
            remote = "origin"

        if base is None:
            base = self.main_branch_name

        if files is None:
            files = {f"test{self.pr_counter}": ""}

        if not branch:
            branch = f"{as_}/pr{self.pr_counter}"
            branch = self.get_full_branch_name(branch)

        title = f"{self._testMethodName}: pull request n{self.pr_counter} from {as_}"
        if not commit_headline:
            commit_headline = title

        if git_tree_ready:
            await self.git("branch", "-M", branch)
        else:
            await self.git("checkout", "--quiet", f"origin/{base}", "-b", branch)

        await self._git_create_files(files)

        args_commit = ["commit", "--no-edit", "-m", commit_headline]
        if commit_body is not None:
            args_commit += ["-m", commit_body]

        tmp_kwargs = {}
        if commit_date is not None:
            tmp_kwargs["_env"] = {"GIT_COMMITTER_DATE": commit_date.isoformat()}

        if verified:
            temporary_folder = tempfile.mkdtemp()
            tmp_env = {"GNUPGHOME": temporary_folder}
            self.addCleanup(shutil.rmtree, temporary_folder)
            subprocess.run(
                ["gpg", "--import"],
                input=config.TESTING_GPGKEY_SECRET,
                env=self.git.prepare_safe_env(tmp_env),
            )
            await self.git("config", "user.signingkey", config.TESTING_ID_GPGKEY_SECRET)
            await self.git(
                "config", "user.email", "engineering+mergify-test@mergify.io"
            )
            args_commit.append("-S")
            tmp_kwargs.setdefault("_env", {})
            tmp_kwargs["_env"].update(tmp_env)

        await self.git(*args_commit, **tmp_kwargs)

        if two_commits:
            await self.git(
                "mv", f"test{self.pr_counter}", f"test{self.pr_counter}-moved"
            )
            args_second_commit = [
                "commit",
                "--no-edit",
                "-m",
                f"{commit_headline}, moved",
            ]
            if commit_body is not None:
                args_second_commit += ["-m", commit_body]
            if verified:
                args_second_commit.append("-S")

            await self.git(*args_second_commit, **tmp_kwargs)

        await self.git("push", "--quiet", remote, branch)
        if as_ != "fork":
            await self.wait_for(
                "push",
                {"ref": f"refs/heads/{branch}"},
            )

        if as_ == "admin":
            client = self.client_admin
            login = github_types.GitHubLogin("mergifyio-testing")
        elif as_ == "fork":
            client = self.client_fork
            login = github_types.GitHubLogin("mergify-test2")
        else:
            client = self.client_integration
            login = github_types.GitHubLogin("mergifyio-testing")

        resp = await client.post(
            f"{self.url_origin}/pulls",
            json={
                "base": base,
                "head": f"{login}:{branch}",
                "title": title,
                "body": title if message is None else message,
                "draft": draft,
            },
        )
        await self.wait_for("pull_request", {"action": "opened"})

        self.created_branches.add(github_types.GitHubRefType(branch))

        pr = typing.cast(github_types.GitHubPullRequest, resp.json())

        return pr

    async def _git_create_files(self, files: dict[str, str]) -> None:
        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        for name, content in files.items():
            path = self.git.repository + "/" + name
            directory_path = os.path.dirname(path)
            os.makedirs(directory_path, exist_ok=True)
            with open(path, "w") as f:
                f.write(content)
            await self.git("add", name)

    async def create_conflicting_prs(self) -> list[github_types.GitHubPullRequest]:
        pr1 = await self.create_pr(
            files={"testconflict": "testconflict"},
        )
        branch = pr1["head"]["ref"]
        branch_2 = f"{branch}-conflict"

        await self.git("checkout", "--quiet", branch)
        await self.git("checkout", "--quiet", "-b", branch_2)
        await self.git("push", "--quiet", "origin", branch_2)

        self.pr_counter += 1
        login = github_types.GitHubLogin("mergifyio-testing")

        title = (
            f"{self._testMethodName}: pull request n{self.pr_counter} from integration"
        )
        resp = await self.client_integration.post(
            f"{self.url_origin}/pulls",
            json={
                "base": self.main_branch_name,
                "head": f"{login}:{branch_2}",
                "title": title,
                "body": title,
            },
        )
        await self.wait_for("pull_request", {"action": "opened"})
        pr2 = typing.cast(github_types.GitHubPullRequest, resp.json())

        return [pr1, pr2]

    async def create_pr_with_autosquash_commit(
        self,
        commit_type: typing.Literal["fixup", "squash", "fixup=amend", "fixup=reword"],
        commit_body: str | None = None,
        autosquash_commit_body: str | None = None,
    ) -> github_types.GitHubPullRequest:
        # if autosquash_commit_body is not None and commit_type in ("fixup=amend", "fixup=reword"):
        #     raise RuntimeError("Git doesn't allow `-m` with `--fixup=amend` and `--fixup=reword`")

        pr = await self.create_pr(commit_body=commit_body)

        with open(self.git.repository + f"/testfixup{self.pr_counter}", "w") as f:
            f.write("fixup")

        await self.git("add", f"testfixup{self.pr_counter}")

        args = ["commit", "--no-edit"]
        if commit_type in ("fixup=amend", "fixup=reword"):
            args += [f"--{commit_type}:{pr['head']['sha']}"]
        else:
            args += [f"--{commit_type}", pr["head"]["sha"]]

        if autosquash_commit_body is not None and commit_type not in (
            "fixup=amend",
            "fixup=reword",
        ):
            args += ["-m", autosquash_commit_body]

        await self.git(*args)

        await self.git("push", "--quiet", "origin", pr["head"]["ref"])

        await self.wait_for("push", {"ref": f"refs/heads/{pr['head']['ref']}"})
        commits = await self.get_commits(pr["number"])
        assert len(commits) == 2

        # We can't use `-m` with those (git doesn't  allow it), so we
        # need to trick a bit to have the same behavior.
        if autosquash_commit_body is not None and commit_type in (
            "fixup=amend",
            "fixup=reword",
        ):
            amend_commit_headline = commits[1]["commit"]["message"].split("\n")[0]
            await self.git(
                "commit",
                "--amend",
                "-m",
                amend_commit_headline,
                "-m",
                commits[0]["commit"]["message"],
                "-m",
                autosquash_commit_body,
            )
            await self.git("push", "-f", "--quiet", "origin", pr["head"]["ref"])

        return await self.get_pull(pr["number"])

    async def create_status(
        self,
        pull: github_types.GitHubPullRequest,
        context: str = "continuous-integration/fake-ci",
        state: github_types.GitHubStatusState = "success",
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/statuses/{pull['head']['sha']}",
            json={
                "state": state,
                "description": f"The CI is {state}",
                "context": context,
            },
        )
        # There is no way to retrieve the current test name in a "status" event,
        # so we need to retrieve it based on the head sha of the PR
        # (the test-events-forwarder stores the event like that).
        await self.wait_for("status", {"state": state}, test_id=pull["head"]["sha"])

    async def get_check_runs(
        self,
        pull: github_types.GitHubPullRequest,
    ) -> list[github_types.GitHubCheckRun]:
        req = await self.client_integration.get(
            f"{self.url_origin}/commits/{pull['head']['ref']}/check-runs"
        )
        assert req.status_code == 200
        return [
            typing.cast(github_types.GitHubCheckRun, check_run)
            for check_run in req.json()["check_runs"]
        ]

    async def create_check_run(
        self,
        pull: github_types.GitHubPullRequest,
        context: str = "continuous-integration/fake-ci",
        conclusion: str = "success",
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/check-runs",
            json={
                "name": context,
                "head_sha": pull["head"]["sha"],
                "conclusion": conclusion,
            },
        )
        await self.wait_for(
            "check_run",
            {"check_run": {"conclusion": conclusion, "status": "completed"}},
        )

    async def create_review(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        event: typing.Literal[
            "APPROVE", "REQUEST_CHANGES", "COMMENT", "PENDING"
        ] = "APPROVE",
        oauth_token: github_types.GitHubOAuthToken | None = None,
    ) -> None:
        await self.client_admin.post(
            f"{self.url_origin}/pulls/{pull_number}/reviews",
            json={"event": event, "body": f"event: {event}"},
            oauth_token=oauth_token,
        )
        await self.wait_for("pull_request_review", {"action": "submitted"})

    async def get_review_requests(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> github_types.GitHubRequestedReviewers:
        return typing.cast(
            github_types.GitHubRequestedReviewers,
            await self.client_integration.item(
                f"{self.url_origin}/pulls/{pull_number}/requested_reviewers",
            ),
        )

    async def create_review_request(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        reviewers: list[str],
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/pulls/{pull_number}/requested_reviewers",
            json={"reviewers": reviewers},
        )
        await self.wait_for("pull_request", {"action": "review_requested"})

    async def create_comment(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
    ) -> int:
        if as_ == "admin":
            client = self.client_admin
        elif as_ == "fork":
            client = self.client_fork
        else:
            client = self.client_integration

        response = await client.post(
            f"{self.url_origin}/issues/{pull_number}/comments", json={"body": message}
        )
        await self.wait_for("issue_comment", {"action": "created"}, test_id=pull_number)
        return typing.cast(int, response.json()["id"])

    async def create_comment_as_fork(
        self, pull_number: github_types.GitHubPullRequestNumber, message: str
    ) -> int:
        return await self.create_comment(pull_number, message, as_="fork")

    async def create_comment_as_admin(
        self, pull_number: github_types.GitHubPullRequestNumber, message: str
    ) -> int:
        return await self.create_comment(pull_number, message, as_="admin")

    async def create_command(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        command: str,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
    ) -> int:
        comment_id = await self.create_comment(pull_number, command, as_=as_)
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"}, test_id=pull_number)
        await self.run_engine()
        return comment_id

    async def get_gql_id_of_comment_to_hide(
        self, pr_number: int, comment_number: int
    ) -> str | None:
        query = f"""
        query {{
            repository(owner: "{self.repository_ctxt.repo["owner"]["login"]}", name: "{self.repository_ctxt.repo["name"]}") {{
                pullRequest(number: {pr_number}) {{
                    comments(first: 100) {{
                    nodes {{
                        id
                        databaseId
                    }}
                    }}
                }}
            }}
        }}
        """
        response = await self.client_integration.graphql_post(query)
        data = typing.cast(
            github_graphql_types.GraphqlHidingCommentsQuery, response["data"]
        )

        for comment in data["repository"]["pullRequest"]["comments"]["nodes"]:
            if comment["databaseId"] == comment_number:
                return comment["id"]
        return None

    async def hide_comment(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        comment_number: int,
        hide_reason: None
        | (github_graphql_types.ReportedContentClassifiers) = "OUTDATED",
    ) -> bool:
        gql_comment_id = await self.get_gql_id_of_comment_to_hide(
            pull_number, comment_number
        )
        mutation = f"""
        mutation {{
            minimizeComment(input: {{ classifier: {hide_reason}, subjectId: "{gql_comment_id}" }}) {{
                minimizedComment {{
                    isMinimized
                    minimizedReason
                    viewerCanMinimize
                }}
            }}
        }}
        """

        response = await self.client_integration.graphql_post(mutation)
        data = typing.cast(
            github_graphql_types.GraphqlMinimizedCommentResponse, response["data"]
        )
        return data["minimizeComment"]["minimizedComment"]["isMinimized"]

    async def delete_comment(self, comment_number: int) -> None:
        resp = await self.client_integration.delete(
            f"/repos/{self.repository_ctxt.repo['owner']['login']}/{self.repository_ctxt.repo['name']}/issues/comments/{comment_number}"
        )
        assert resp.status_code == 204

    async def create_review_thread(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
        line: int | None = 1,
        path: str | None = "test1",
    ) -> int:
        commits = await self.get_commits(pull_number=pull_number)
        response = await self.client_integration.post(
            f"{self.url_origin}/pulls/{pull_number}/comments",
            json={
                "body": message,
                "path": path,
                "commit_id": commits[-1]["sha"],
                "line": line,
            },
        )
        await self.wait_for("pull_request_review_comment", {"action": "created"})
        return typing.cast(int, response.json()["id"])

    async def get_review_threads(
        self, number: int
    ) -> github_graphql_types.GraphqlReviewThreadsQuery:
        query = f"""
        query {{
            repository(owner: "{self.repository_ctxt.repo["owner"]["login"]}", name: "{self.repository_ctxt.repo["name"]}") {{
                pullRequest(number: {number}) {{
                    reviewThreads(first: 100) {{
                    edges {{
                        node {{
                            isResolved
                            id
                            comments(first: 1) {{
                                edges {{
                                    node {{
                                        body
                                    }}
                                }}
                            }}
                        }}
                    }}
                    }}
                }}
            }}
        }}
        """
        data = typing.cast(
            github_graphql_types.GraphqlReviewThreadsQuery,
            (await self.client_integration.graphql_post(query))["data"],
        )
        return data

    async def reply_to_review_comment(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
        comment_id: int,
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/pulls/{pull_number}/comments",
            json={"body": message, "in_reply_to": comment_id},
        )
        await self.wait_for("pull_request_review_comment", {"action": "created"})

    async def resolve_review_thread(
        self,
        thread_id: str,
    ) -> bool:
        mutation = f"""
        mutation {{
            resolveReviewThread(input:{{clientMutationId: "{self.mergify_bot['id']}", threadId: "{thread_id}"}}) {{
                thread {{
                    isResolved
                }}
            }}
        }}
        """
        response = await self.client_integration.graphql_post(mutation)
        data = typing.cast(
            github_graphql_types.GraphqlResolveThreadMutationResponse, response["data"]
        )
        return data["resolveReviewThread"]["thread"]["isResolved"]

    async def create_issue(self, title: str, body: str) -> github_types.GitHubIssue:
        resp = await self.client_integration.post(
            f"{self.url_origin}/issues", json={"body": body, "title": title}
        )
        # NOTE(sileht):Our GitHubApp doesn't subscribe to issues event
        # because we don't request the permissions for it.
        # await self.wait_for("issues", {"action": "created"})
        return typing.cast(github_types.GitHubIssue, resp.json())

    async def add_assignee(
        self, pull_number: github_types.GitHubPullRequestNumber, assignee: str
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/issues/{pull_number}/assignees",
            json={"assignees": [assignee]},
        )
        await self.wait_for("pull_request", {"action": "assigned"})

    async def send_refresh(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> None:
        await refresher.send_pull_refresh(
            redis_stream=self.redis_links.stream,
            repository=self.repository_ctxt.repo,
            action="internal",
            pull_request_number=pull_number,
            source="test",
        )

    async def add_label(
        self, pull_number: github_types.GitHubPullRequestNumber, label: str
    ) -> github_types.GitHubEventPullRequest:
        if label not in self.existing_labels:
            try:
                await self.client_integration.post(
                    f"{self.url_origin}/labels", json={"name": label, "color": "000000"}
                )
            except http.HTTPClientSideError as e:
                if e.status_code != 422:
                    raise

            self.existing_labels.append(label)

        await self.client_integration.post(
            f"{self.url_origin}/issues/{pull_number}/labels", json={"labels": [label]}
        )
        return await self.wait_for_pull_request("labeled", pr_number=pull_number)

    async def remove_label(
        self, pull_number: github_types.GitHubPullRequestNumber, label: str
    ) -> None:
        await self.client_integration.delete(
            f"{self.url_origin}/issues/{pull_number}/labels/{label}"
        )
        await self.wait_for("pull_request", {"action": "unlabeled"})

    async def branch_protection_unprotect(self, branch: str) -> None:
        await self.client_admin.delete(
            f"{self.url_origin}/branches/{branch}/protection",
            headers={"Accept": "application/vnd.github.luke-cage-preview+json"},
        )

    async def branch_protection_protect(
        self, branch: str, protection: dict[str, typing.Any]
    ) -> None:
        if protection["required_pull_request_reviews"]:
            protection = copy.deepcopy(protection)
            protection["required_pull_request_reviews"]["dismissal_restrictions"] = {}

        await self.client_admin.put(
            f"{self.url_origin}/branches/{branch}/protection",
            json=protection,
            headers={"Accept": "application/vnd.github.luke-cage-preview+json"},
        )

    async def get_branches(
        self, filter_on_name: bool = True
    ) -> list[github_types.GitHubBranch]:
        branch_name_prefix = self.get_full_branch_name()
        return [
            b
            async for b in self.client_integration.items(
                f"{self.url_origin}/branches", resource_name="branches", page_limit=10
            )
            if not filter_on_name or b["name"].startswith(branch_name_prefix)
        ]

    async def get_branch(self, name: str) -> github_types.GitHubBranch:
        escaped_branch_name = parse.quote(name, safe="")
        return typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{escaped_branch_name}"
            ),
        )

    async def get_commits(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[github_types.GitHubBranchCommit]:
        return [
            c
            async for c in typing.cast(
                abc.AsyncGenerator[github_types.GitHubBranchCommit, None],
                self.client_integration.items(
                    f"{self.url_origin}/pulls/{pull_number}/commits",
                    resource_name="commits",
                    page_limit=10,
                ),
            )
        ]

    async def get_commit(
        self, sha: github_types.SHAType
    ) -> github_types.GitHubBranchCommit:
        return typing.cast(
            github_types.GitHubBranchCommit,
            await self.client_integration.item(f"{self.url_origin}/commits/{sha}"),
        )

    async def get_head_commit(self) -> github_types.GitHubBranchCommit:
        return typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}"
            ),
        )["commit"]

    async def get_issue_comments(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[github_types.GitHubComment]:
        return [
            comment
            async for comment in typing.cast(
                abc.AsyncGenerator[github_types.GitHubComment, None],
                self.client_integration.items(
                    f"{self.url_origin}/issues/{pull_number}/comments",
                    resource_name="issue comments",
                    page_limit=10,
                ),
            )
        ]

    async def get_reviews(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[github_types.GitHubReview]:
        return [
            review
            async for review in typing.cast(
                abc.AsyncGenerator[github_types.GitHubReview, None],
                self.client_integration.items(
                    f"{self.url_origin}/pulls/{pull_number}/reviews",
                    resource_name="reviews",
                    page_limit=10,
                ),
            )
        ]

    async def get_review_comments(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[github_types.GitHubReview]:
        return [
            review
            async for review in typing.cast(
                abc.AsyncGenerator[github_types.GitHubReview, None],
                self.client_integration.items(
                    f"{self.url_origin}/pulls/{pull_number}/comments",
                    resource_name="review comments",
                    page_limit=10,
                ),
            )
        ]

    async def get_pull(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> github_types.GitHubPullRequest:
        return typing.cast(
            github_types.GitHubPullRequest,
            await self.client_integration.item(
                f"{self.url_origin}/pulls/{pull_number}"
            ),
        )

    async def get_pulls(
        self, **kwargs: typing.Any
    ) -> list[github_types.GitHubPullRequest]:
        params = kwargs.pop("params", {})
        params.setdefault("base", self.main_branch_name)

        return [
            p
            async for p in self.client_integration.items(
                f"{self.url_origin}/pulls",
                resource_name="pulls",
                page_limit=5,
                params=params,
                **kwargs,
            )
        ]

    async def edit_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        **payload: typing.Any,
    ) -> github_types.GitHubPullRequest:
        return typing.cast(
            github_types.GitHubPullRequest,
            (
                await self.client_integration.patch(
                    f"{self.url_origin}/pulls/{pull_number}", json=payload
                )
            ).json(),
        )

    async def close_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        r = await self.client_integration.patch(
            f"{self.url_origin}/pulls/{pull_number}", json={"state": "closed"}
        )
        assert r.status_code == 200
        assert r.json()["state"] == "closed"

    async def is_pull_merged(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> bool:
        try:
            await self.client_integration.get(
                f"{self.url_origin}/pulls/{pull_number}/merge"
            )
        except http.HTTPNotFound:
            return False
        else:
            return True

    async def merge_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        await self.client_integration.put(
            f"{self.url_origin}/pulls/{pull_number}/merge"
        )

    async def merge_pull_as_admin(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        await self.client_admin.put(f"{self.url_origin}/pulls/{pull_number}/merge")

    async def get_labels(self) -> list[github_types.GitHubLabel]:
        return [
            label
            async for label in typing.cast(
                abc.AsyncGenerator[github_types.GitHubLabel, None],
                self.client_integration.items(
                    f"{self.url_origin}/labels", resource_name="labels", page_limit=3
                ),
            )
        ]

    async def find_git_refs(
        self, url: str, matches: list[str]
    ) -> abc.AsyncGenerator[github_types.GitHubGitRef, None]:
        for match in matches:
            async for matchedBranch in typing.cast(
                abc.AsyncGenerator[github_types.GitHubGitRef, None],
                self.client_integration.items(
                    f"{url}/git/matching-refs/heads/{match}",
                    resource_name="branches",
                    page_limit=5,
                ),
            ):
                yield matchedBranch

    async def get_teams(self) -> list[github_types.GitHubTeam]:
        return [
            t
            async for t in typing.cast(
                abc.AsyncGenerator[github_types.GitHubTeam, None],
                self.client_integration.items(
                    "/orgs/mergifyio-testing/teams", resource_name="teams", page_limit=5
                ),
            )
        ]

    async def get_queue_rule(self, name: str) -> qr_config.QueueRule:
        config = await self.repository_ctxt.get_mergify_config()
        return config["queue_rules"][qr_config.QueueName(name)]

    async def get_queue_rules(self) -> qr_config.QueueRules:
        config = await self.repository_ctxt.get_mergify_config()
        return config["queue_rules"]

    async def get_train(self) -> merge_train.Train:
        q = merge_train.Train(
            repository=self.repository_ctxt,
            queue_rules=await self.get_queue_rules(),
            ref=self.main_branch_name,
        )
        await q.load()
        return q

    @staticmethod
    def _assert_merge_queue_car(
        car: merge_train.TrainCar, expected_car: MergeQueueCarMatcher
    ) -> None:
        for i, ep in enumerate(car.still_queued_embarked_pulls):
            assert (
                ep.user_pull_request_number == expected_car.user_pull_request_numbers[i]
            )
        assert (
            car.parent_pull_request_numbers == expected_car.parent_pull_request_numbers
        )
        assert car.initial_current_base_sha == expected_car.initial_current_base_sha
        assert car.train_car_state.checks_type == expected_car.checks_type
        assert car.queue_pull_request_number == expected_car.queue_pull_request_number

    async def assert_merge_queue_contents(
        self,
        q: merge_train.Train,
        expected_base_sha: github_types.SHAType | None,
        expected_cars: list[MergeQueueCarMatcher],
        expected_waiting_pulls: None
        | (list[github_types.GitHubPullRequestNumber]) = None,
    ) -> None:
        if expected_waiting_pulls is None:
            expected_waiting_pulls = []

        await q.load()
        self.assertEqual(q._current_base_sha, expected_base_sha)

        pulls_in_queue = await q.get_pulls()
        assert (
            pulls_in_queue
            == list(
                itertools.chain.from_iterable(
                    [p.user_pull_request_numbers for p in expected_cars]
                )
            )
            + expected_waiting_pulls
        )

        assert len(q._cars) == len(expected_cars)
        for i, expected_car in enumerate(expected_cars):
            car = q._cars[i]
            self._assert_merge_queue_car(car, expected_car)

        assert len(q._waiting_pulls) == len(expected_waiting_pulls)
        for i, expected_waiting_pull in enumerate(expected_waiting_pulls):
            wp = q._waiting_pulls[i]
            assert wp.user_pull_request_number == expected_waiting_pull

    def get_statistic_redis_key(
        self,
        stat_name: queue_statistics.AvailableStatsKeyT,
    ) -> str:
        return queue_statistics.get_statistic_redis_key(
            self.subscription.owner_id, self.RECORD_CONFIG["repository_id"], stat_name
        )

    async def push_file(
        self,
        filename: str = "random_file.txt",
        content: str = "",
        target_branch: str | None = None,
    ) -> github_types.SHAType:
        if target_branch is None:
            target_branch = self.main_branch_name
        await self.git("fetch", "origin", target_branch)
        await self.git("checkout", "-b", "random", f"origin/{target_branch}")
        with open(self.git.repository + f"/{filename}", "w") as f:
            f.write(content)
        await self.git("add", filename)
        await self.git("commit", "--no-edit", "-m", "random update")
        head_sha = github_types.SHAType(
            (await self.git("log", "-1", "--format=%H")).strip()
        )
        await self.git("push", "--quiet", "origin", f"random:{target_branch}")
        await self.wait_for("push", {"ref": f"refs/heads/{target_branch}"})
        return head_sha

    @staticmethod
    async def assert_check_run(
        ctxt: context.Context,
        check_name: str,
        expected_conclusion: str,
        expected_title: str,
        expected_summary: str,
    ) -> None:
        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == check_name,
        )
        assert check is not None
        assert check["conclusion"] == expected_conclusion
        assert check["output"]["title"] == expected_title
        assert check["output"]["summary"] == expected_summary
