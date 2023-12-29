import asyncio
from collections import abc
import contextlib
import copy
import dataclasses
import datetime
import itertools
import json
import os
import re
import shutil
import subprocess
import tempfile
import typing
import unittest
from unittest import mock
from urllib import parse
import uuid

import daiquiri
from first import first
import httpx
import pytest
import respx

from mergify_engine import branch_updater
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import delayed_refresh
from mergify_engine import duplicate_pull
from mergify_engine import github_graphql_types
from mergify_engine import github_types
from mergify_engine import gitter
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.actions import backport
from mergify_engine.actions import copy as copy_action
from mergify_engine.actions import merge_base
from mergify_engine.ci import event_processing
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.github_in_postgres import process_events as ghinpg_process_events
from mergify_engine.log_embedder import github_action
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import conftest as func_conftest
from mergify_engine.tests.functional import event_reader
from mergify_engine.tests.functional import utils as tests_utils
from mergify_engine.worker import gitter_service
from mergify_engine.worker import log_embedder_service
from mergify_engine.worker import manager
from mergify_engine.worker import stream


if typing.TYPE_CHECKING:
    import logging


LOG = daiquiri.getLogger(__name__)

real_consume_method = stream.Processor.consume
real_store_redis_events_in_pg = ghinpg_process_events.store_redis_events_in_pg
real_process_failed_jobs = github_action.process_failed_jobs
real_process_workflow_run_stream = event_processing.process_workflow_run_stream
real_process_workflow_job_stream = event_processing.process_workflow_job_stream


class MergeQueueCarMatcher(typing.NamedTuple):
    user_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    initial_current_base_sha: github_types.SHAType
    checks_type: merge_train.TrainCarChecksType | None
    queue_pull_request_number: github_types.GitHubPullRequestNumber | None


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


@dataclasses.dataclass
class GitterRecorder(gitter.Gitter):
    cassette_library_dir: dataclasses.InitVar[str]
    cassette_library_dir_suffix: dataclasses.InitVar[str]
    cassette_path: str = dataclasses.field(init=False)
    records: list[Record] = dataclasses.field(init=False)

    def __post_init__(
        self,
        cassette_library_dir: str,
        cassette_library_dir_suffix: str,
    ) -> None:
        super().__post_init__()
        self.cassette_path = os.path.join(
            cassette_library_dir,
            f"git-{cassette_library_dir_suffix}.json",
        )
        if settings.TESTING_RECORD:
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
        if settings.TESTING_RECORD:
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
                    },
                )
                raise
            else:
                self.records.append(
                    {
                        "args": self.prepare_args(args),
                        "kwargs": self.prepare_kwargs(kwargs),
                        "out": output,
                    },
                )
            return output

        r = self.records.pop(0)
        if "exc" in r:
            raise self._create_git_exception(r["exc"]["returncode"], r["exc"]["output"])

        assert r["args"] == self.prepare_args(
            args,
        ), f'{r["args"]} != {self.prepare_args(args)}'
        assert r["kwargs"] == self.prepare_kwargs(
            kwargs,
        ), f'{r["kwargs"]} != {self.prepare_kwargs(kwargs)}'
        return r["out"]

    def prepare_args(self, args: typing.Any) -> list[str]:
        prepared_args = [
            arg.replace(self._temporary_directory, "/tmp/mergify-gitter<random>")
            for arg in args
        ]
        if "user.signingkey" in prepared_args:
            prepared_args = [
                arg.replace(settings.TESTING_GPG_SECRET_KEY, "<SECRET>") for arg in args
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
        if settings.TESTING_RECORD:
            self.save_records()


@pytest.mark.usefixtures("_unittest_asyncio_glue")
class IsolatedAsyncioTestCaseWithPytestAsyncioGlue(unittest.IsolatedAsyncioTestCase):
    def _setupAsyncioRunner(self) -> None:
        # NOTE(sileht): py311 unittest internal interface
        # We reuse the event loop created by pytest-asyncio
        self._asyncioRunner = asyncio.Runner(
            debug=True,
            loop_factory=lambda: self.pytest_event_loop,  # type: ignore[attr-defined]
        )

    def _tearDownAsyncioRunner(self) -> None:
        # NOTE(sileht): py311 unittest internal interface
        # We don't run the loop.close() of unittest and let pytest doing it
        pass


@pytest.mark.usefixtures("_unittest_glue")
class FunctionalTestBase(IsolatedAsyncioTestCaseWithPytestAsyncioGlue):
    # Compat mypy/pytest fixtures
    app: httpx.AsyncClient
    admin_app: httpx.AsyncClient
    RECORD_CONFIG: func_conftest.RecordConfigType
    subscription: subscription.Subscription
    cassette_library_dir: str
    api_key_admin: str

    # NOTE(sileht): The repository have been manually created in mergifyio-testing
    # organization and then forked in mergify-test2 user account
    FORK_PERSONAL_TOKEN = settings.TESTING_EXTERNAL_USER_PERSONAL_TOKEN
    SUBSCRIPTION_ACTIVE = False

    # To run tests on private repository, you can use:
    # FORK_PERSONAL_TOKEN = config.ORG_USER_PERSONAL_TOKEN
    # SUBSCRIPTION_ACTIVE = True

    WAIT_TIME_BEFORE_TEARDOWN = 0.20
    WORKER_IDLE_TIME = 0.20 if settings.TESTING_RECORD else 0.01

    WORKER_HAS_WORK_INTERVAL_CHECK = 0.02

    def register_mock(self, mock_obj: typing.Any) -> None:
        mock_obj.start()
        self.addCleanup(mock_obj.stop)

    async def asyncSetUp(self) -> None:
        parent_class = type(self).mro()[1]
        if parent_class.__name__ != FunctionalTestBase.__name__:
            # It means that the current test class has inherited from
            # another test class.
            # So in order to not have the same _testMethodName twice when
            # recording in parallel, we just add the uppercase letters
            # of the current class name to the _testMethodName.
            # We only add uppercase letters, because, to construct the branch name
            # we only keep 50 letters, so adding the whole class name
            # might lead to other issues with method names.
            self._testMethodName = (
                re.sub(r"[a-z]", "", type(self).__name__) + self._testMethodName
            )

        super().setUp()

        # NOTE(sileht): don't preempted bucket consumption
        # Otherwise preemption doesn't occur at the same moment during record
        # and replay. Making some tests working during record and failing
        # during replay.
        settings.BUCKET_PROCESSING_MAX_SECONDS = 100000

        settings.API_ENABLE = True

        self.register_mock(
            mock.patch.object(
                constants,
                "NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST",
                datetime.timedelta(seconds=0),
            ),
        )
        self.register_mock(
            mock.patch.object(
                constants,
                "MIN_DELAY_BETWEEN_SAME_PULL_REQUEST",
                datetime.timedelta(seconds=0),
            ),
        )

        self._graphql_repository_id: str | None = None
        self.created_branch_protection_rule_ids: set[str] = set()
        self.created_branches: set[github_types.GitHubRefType] = set()
        self.existing_labels: list[str] = []
        self.pr_counter: int = 0
        self.git_counter: int = 0

        self.register_mock(
            mock.patch.object(branch_updater.gitter, "Gitter", self.get_gitter),  # type: ignore[attr-defined]
        )
        self.register_mock(
            mock.patch.object(duplicate_pull.gitter, "Gitter", self.get_gitter),  # type: ignore[attr-defined]
        )

        self.main_branch_name = github_types.GitHubRefType(
            self.get_full_branch_name("main"),
        )
        self.escaped_main_branch_name = parse.quote(self.main_branch_name, safe="")
        # ########## MOCK MERGE_QUEUE_BRANCH_PREFIX
        # To easily delete all branches created by any queue-related
        # stuff.

        self.mocked_merge_queue_branch_prefix = f"{self._testMethodName[:50]}/mq/"
        self.mock_merge_queue_branch_prefix = mock.patch.object(
            constants,
            "MERGE_QUEUE_BRANCH_PREFIX",
            self.mocked_merge_queue_branch_prefix,
        )
        self.register_mock(self.mock_merge_queue_branch_prefix)
        # ##############################

        # ########## MOCK BACKPORT AND COPY ACTIONS BRANCH PREFIX
        # This way we can clean up only the branches created by the backport/copy
        # of the current test.
        uuidvalue = uuid.uuid5(uuid.NAMESPACE_OID, self._testMethodName)
        self.mocked_backport_branch_prefix = f"bp/{uuidvalue}"
        self.mocked_copy_branch_prefix = f"copy/{uuidvalue}"

        self.register_mock(
            mock.patch.object(
                backport.BackportExecutor,
                "BRANCH_PREFIX",
                self.mocked_backport_branch_prefix,
            ),
        )
        self.register_mock(
            mock.patch.object(
                copy_action.CopyExecutor,
                "BRANCH_PREFIX",
                self.mocked_copy_branch_prefix,
            ),
        )
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
            ),
        )
        # ##############################

        # ########## MOCK context.Repository.get_pulls
        # Need to mock the `get_pulls` call to force `base=self.main_branch_name`,
        # otherwise we would query all the PR of all the tests
        real_repository_get_pulls = context.Repository.get_pulls

        async def mocked_get_pulls(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs["base"] = self.main_branch_name
            return await real_repository_get_pulls(*args, **kwargs)

        self.register_mock(
            mock.patch.object(context.Repository, "get_pulls", mocked_get_pulls),
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
            settings.TESTING_ORGANIZATION_ID,
        )

        self.client_integration = github.aget_client(installation_json)
        self.client_admin = github.AsyncGitHubInstallationClient(
            auth=github.GitHubTokenAuth(
                token=settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
            ),
        )
        self.client_fork = github.AsyncGitHubInstallationClient(
            auth=github.GitHubTokenAuth(
                token=self.FORK_PERSONAL_TOKEN,
            ),
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
        self.git_origin = f"{settings.GITHUB_URL}/mergifyio-testing/{self.RECORD_CONFIG['repository_name']}"
        self.git_fork = f"{settings.GITHUB_URL}/mergify-test2/{self.RECORD_CONFIG['repository_name']}"

        self.installation_ctxt = context.Installation(
            installation_json,
            self.subscription,
            self.client_integration,
            self.redis_links,
        )
        self.repository_ctxt = await self.installation_ctxt.get_repository_by_id(
            github_types.GitHubRepositoryIdType(self.RECORD_CONFIG["repository_id"]),
        )

        # NOTE(sileht): We mock this method because when we replay test, the
        # timing maybe not the same as when we record it, making the formatted
        # elapsed time different in the merge queue summary.
        def fake_pretty_datetime(_dt: datetime.datetime) -> str:
            return "<fake_pretty_datetime()>"

        self.register_mock(
            mock.patch(
                "mergify_engine.date.pretty_datetime",
                side_effect=fake_pretty_datetime,
            ),
        )

        self._event_reader = event_reader.EventReader(
            self.app,
            self.RECORD_CONFIG["integration_id"],
            self.RECORD_CONFIG["repository_id"],
            self.get_full_branch_name(),
        )
        await self._event_reader.drain()

        # Track when worker work
        self.worker_concurrency_works = 0

        await self.update_delete_branch_on_merge()

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()

        await self.update_delete_branch_on_merge(teardown=True)

        # NOTE(sileht): Wait a bit to ensure all remaining events arrive.
        if settings.TESTING_RECORD:
            await asyncio.sleep(self.WAIT_TIME_BEFORE_TEARDOWN)

            for rule_id in self.created_branch_protection_rule_ids:
                await self.delete_graphql_branch_protection_rule(rule_id)

            current_test_branches = [
                github_types.GitHubRefType(ref["ref"].replace("refs/heads/", ""))
                async for ref in self.find_git_refs(
                    self.url_origin,
                    [
                        self.main_branch_name,
                        self.mocked_merge_queue_branch_prefix,
                        f"mergify/{self.mocked_backport_branch_prefix}",
                        f"mergify/{self.mocked_copy_branch_prefix}",
                    ],
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
                        f"{self.url_origin}/git/refs/heads/{parse.quote(branch['name'])}",
                    )
                except http.HTTPNotFound:
                    continue

        await self.app.aclose()

        await self._event_reader.aclose()
        await self.redis_links.flushall()
        await self.redis_links.shutdown_all()

    async def reload_repository_ctxt_configuration(self) -> None:
        self.repository_ctxt._caches.mergify_config_file.delete()
        self.repository_ctxt._caches.mergify_config.delete()
        await self.repository_ctxt.load_mergify_config()

    async def update_delete_branch_on_merge(self, teardown: bool = False) -> None:
        for marker in self.pytestmark:  # type: ignore[attr-defined]
            if marker.name == "delete_branch_on_merge":
                if teardown:
                    await self._set_delete_branch_on_merge(False)
                else:
                    await self._set_delete_branch_on_merge(*marker.args)

                break

    async def _set_delete_branch_on_merge(self, value: bool) -> None:
        r = await self.repository_ctxt.installation.client.patch(
            self.repository_ctxt.base_url,
            api_version="antiope",
            json={"delete_branch_on_merge": value},
            oauth_token=settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
        )

        assert r.json()["delete_branch_on_merge"] is value

    def clear_repository_ctxt_caches(self) -> None:
        self.repository_ctxt.clear_caches()

    async def wait_for(
        self,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> github_types.GitHubEvent:
        return await self._event_reader.wait_for(*args, **kwargs)

    async def wait_for_all(
        self,
        expected_events: list[event_reader.WaitForAllEvent],
        sort_received_events: bool = False,
        **kwargs: typing.Any,
    ) -> list[event_reader.EventReceived]:
        events = await self._event_reader.wait_for_all(expected_events, **kwargs)
        if sort_received_events and len(events) > 1:
            return tests_utils.sort_events(expected_events, events)

        return events

    async def wait_for_push(
        self,
        ref: str | None = None,
        branch_name: str | None = None,
    ) -> github_types.GitHubEventPush:
        payload = tests_utils.get_push_event_payload(ref=ref, branch_name=branch_name)
        return typing.cast(
            github_types.GitHubEventPush,
            await self.wait_for(
                "push",
                payload,
            ),
        )

    async def wait_for_pull_request(
        self,
        action: github_types.GitHubEventPullRequestActionType | None = None,
        pr_number: github_types.GitHubPullRequestNumber | None = None,
        merged: bool | None = None,
        forward_to_engine: bool = True,
    ) -> github_types.GitHubEventPullRequest:
        wait_for_payload = tests_utils.get_pull_request_event_payload(
            action=action,
            pr_number=pr_number,
            merged=merged,
        )

        return typing.cast(
            github_types.GitHubEventPullRequest,
            await self.wait_for(
                "pull_request",
                wait_for_payload,
                forward_to_engine=forward_to_engine,
            ),
        )

    async def wait_for_issue_comment(
        self,
        test_id: str,
        action: github_types.GitHubEventIssueCommentActionType,
    ) -> github_types.GitHubEventIssueComment:
        wait_for_payload = tests_utils.get_issue_comment_event_payload(action)
        return typing.cast(
            github_types.GitHubEventIssueComment,
            await self.wait_for("issue_comment", wait_for_payload, test_id=test_id),
        )

    async def wait_for_check_run(
        self,
        action: github_types.GitHubCheckRunActionType | None = None,
        status: github_types.GitHubCheckRunStatus | None = None,
        conclusion: github_types.GitHubCheckRunConclusion | None = None,
        name: str | None = None,
        pr_number: github_types.GitHubPullRequestNumber | None = None,
    ) -> github_types.GitHubEventCheckRun:
        wait_for_payload = tests_utils.get_check_run_event_payload(
            action=action,
            status=status,
            conclusion=conclusion,
            name=name,
            pr_number=pr_number,
        )

        return typing.cast(
            github_types.GitHubEventCheckRun,
            await self.wait_for(
                "check_run",
                wait_for_payload,
            ),
        )

    async def wait_for_pull_request_review(
        self,
        state: github_types.GitHubEventReviewStateType,
    ) -> github_types.GitHubEventPullRequestReview:
        wait_for_payload = tests_utils.get_pull_request_review_event_payload(
            state=state,
        )

        return typing.cast(
            github_types.GitHubEventPullRequestReview,
            await self.wait_for("pull_request_review", wait_for_payload),
        )

    async def run_engine(
        self,
        additionnal_services: manager.ServiceNamesT | None = None,
    ) -> None:
        LOG.log(42, "RUNNING ENGINE")

        async def mocked_tracked_consume(*args, **kwargs):  # type: ignore[no-untyped-def]
            self.worker_concurrency_works += 1
            try:
                await real_consume_method(*args, **kwargs)
            finally:
                self.worker_concurrency_works -= 1

        async def mocked_store_redis_events_in_pg(*args, **kwargs):  # type: ignore[no-untyped-def]
            self.worker_concurrency_works += 1
            try:
                return await real_store_redis_events_in_pg(*args, **kwargs)
            finally:
                self.worker_concurrency_works -= 1

        async def mocked_process_failed_jobs(*args, **kwargs):  # type: ignore[no-untyped-def]
            self.worker_concurrency_works += 1
            try:
                return await real_process_failed_jobs(*args, **kwargs)
            finally:
                self.worker_concurrency_works -= 1

        async def mocked_process_workflow_run_stream(*args, **kwargs):  # type: ignore[no-untyped-def]
            self.worker_concurrency_works += 1
            try:
                return await real_process_workflow_run_stream(*args, **kwargs)
            finally:
                self.worker_concurrency_works -= 1

        async def mocked_process_workflow_job_stream(*args, **kwargs):  # type: ignore[no-untyped-def]
            self.worker_concurrency_works += 1
            try:
                return await real_process_workflow_job_stream(*args, **kwargs)
            finally:
                self.worker_concurrency_works -= 1

        services: manager.ServiceNamesT = {
            "shared-workers-spawner",
            "dedicated-workers-spawner",
            "gitter",
        }

        if additionnal_services:
            services.update(additionnal_services)

        with mock.patch.object(
            stream.Processor,
            "consume",
            mocked_tracked_consume,
        ), mock.patch.object(
            ghinpg_process_events,
            "store_redis_events_in_pg",
            mocked_store_redis_events_in_pg,
        ), mock.patch.object(
            github_action,
            "process_failed_jobs",
            mocked_process_failed_jobs,
        ), mock.patch.object(
            event_processing,
            "process_workflow_run_stream",
            mocked_process_workflow_run_stream,
        ), mock.patch.object(
            event_processing,
            "process_workflow_job_stream",
            mocked_process_workflow_job_stream,
        ):
            await self._run_workers(services)

        LOG.log(42, "END RUNNING ENGINE")

    async def _run_workers(self, services: manager.ServiceNamesT) -> None:
        w = manager.ServiceManager(
            worker_idle_time=self.WORKER_IDLE_TIME,
            enabled_services=services,
            delayed_refresh_idle_time=0.01,
            dedicated_workers_spawner_idle_time=0.01,
            dedicated_workers_cache_syncer_idle_time=0.01,
            log_embedder_idle_time=0.01,
            github_in_postgres_idle_time=0.01,
            ci_event_processing_idle_time=0.01,
            shared_stream_processes=1,
            shared_stream_tasks_per_process=1,
            gitter_idle_time=0.5,
            gitter_concurrent_jobs=1,
            gitter_worker_idle_time=0.01,
            retry_handled_exception_forever=False,
        )
        try:
            await w.start()

            gitter_serv = w.get_service(gitter_service.GitterService)
            assert gitter_serv is not None

            log_embedder_serv = None
            if services and "log-embedder" in services:
                log_embedder_serv = w.get_service(
                    log_embedder_service.LogEmbedderService,
                )
                assert log_embedder_serv is not None

            did_something = False

            async def keep_going() -> bool:
                return bool(
                    (await w._redis_links.stream.zcard("streams")) > 0
                    or self.worker_concurrency_works > 0
                    or len(gitter_serv._jobs) > 0
                    or (
                        "delayed-refresh" in services
                        and len(
                            await delayed_refresh.get_list_of_refresh_to_send(
                                self.redis_links,
                            ),
                        )
                    )
                    or (
                        "github-in-postgres" in services
                        and await self.redis_links.stream.xlen("github_in_postgres")
                    )
                    or (
                        "ci-event-processing" in services
                        and await self.redis_links.stream.xlen(
                            event_processing.GHA_WORKFLOW_RUN_REDIS_KEY,
                        )
                        and await self.redis_links.stream.xlen(
                            event_processing.GHA_WORKFLOW_JOB_REDIS_KEY,
                        )
                    )
                    or (
                        "log-embedder" in services
                        # Just to please mypy, but it's impossible to have this case
                        # because we assert that the service is not `None` if `log-embedder` is
                        # in `services`.
                        and log_embedder_serv is not None
                        and log_embedder_serv.has_pending_work
                    ),
                )

            while await keep_going():
                did_something = True
                await asyncio.sleep(self.WORKER_HAS_WORK_INTERVAL_CHECK)

            if not did_something:
                raise RuntimeError("A `run_engine` did not do anything.")
        finally:
            w.stop()
            await w.wait_shutdown_complete()
            self.worker_concurrency_works = 0

    def get_gitter(
        self,
        logger: "logging.LoggerAdapter[logging.Logger]",
    ) -> GitterRecorder:
        self.git_counter += 1
        return GitterRecorder(logger, self.cassette_library_dir, str(self.git_counter))

    async def setup_repo(
        self,
        mergify_config: str | None = None,
        test_branches: abc.Iterable[str] | None = None,
        files: dict[str, str] | None = None,
        forward_to_engine: bool = False,
        preload_configuration: bool = False,
    ) -> None:
        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        if test_branches is None:
            test_branches = []
        if files is None:
            files = {}

        self.mergify_bot = await github.GitHubAppInfo.get_bot(self.redis_links.cache)

        await self.git.configure(self.redis_links.cache)
        await self.git.add_cred(
            settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
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

        if preload_configuration:
            await self.reload_repository_ctxt_configuration()

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
        commit_author: str | None = None,
        forward_event_to_engine: bool = True,
    ) -> github_types.GitHubPullRequest:
        self.pr_counter += 1

        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        remote = "fork" if as_ == "fork" else "origin"

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
        if commit_author is not None:
            args_commit += ["--author", commit_author]

        tmp_kwargs = {}
        if commit_date is not None:
            tmp_kwargs["_env"] = {"GIT_COMMITTER_DATE": commit_date.isoformat()}

        if verified:
            temporary_folder = tempfile.mkdtemp()
            tmp_env = {"GNUPGHOME": temporary_folder}
            self.addCleanup(shutil.rmtree, temporary_folder)
            subprocess.run(
                ["gpg", "--import"],
                input=settings.TESTING_GPG_SECRET_KEY.encode(),
                env=self.git.prepare_safe_env(tmp_env),
            )
            await self.git(
                "config",
                "user.signingkey",
                settings.TESTING_GPG_SECRET_KEY_ID,
            )
            await self.git(
                "config",
                "user.email",
                "engineering+mergify-test@mergify.com",
            )
            args_commit.append("-S")
            tmp_kwargs.setdefault("_env", {})
            tmp_kwargs["_env"].update(tmp_env)

        await self.git(*args_commit, **tmp_kwargs)

        if two_commits:
            if not files:
                filename = f"test{self.pr_counter}"
            else:
                first_file = first(files.keys())
                assert isinstance(first_file, str)
                filename = first_file

            await self.git("mv", filename, f"{filename}-moved")

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

        await client.post(
            f"{self.url_origin}/pulls",
            json={
                "base": base,
                "head": f"{login}:{branch}",
                "title": title,
                "body": title if message is None else message,
                "draft": draft,
            },
        )

        pr_opened_event = await self.wait_for_pull_request(
            "opened",
            forward_to_engine=forward_event_to_engine,
        )

        self.created_branches.add(github_types.GitHubRefType(branch))

        return pr_opened_event["pull_request"]

    async def create_pr_with_specific_commits(
        self,
        commits_headline: list[str],
        files: list[dict[str, str]],
        base: str | None = None,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
        branch: str | None = None,
        message: str | None = None,
        draft: bool = False,
        verified: bool = False,
        commits_body: list[str] | None = None,
        commits_author: list[str] | None = None,
        forward_event_to_engine: bool = True,
    ) -> github_types.GitHubPullRequest:
        self.pr_counter += 1

        assert len(commits_headline) > 0, "Cannot have empty commits_headline"
        assert len(files) == len(
            commits_headline,
        ), "The list of `files` must be the same length as `commits_headline`"

        if commits_body is not None:
            assert (
                len(commits_body) == len(commits_headline)
            ), "If `commits_body` is specified, it must have the same length as `commits_headline`"
        if commits_author is not None:
            assert (
                len(commits_author) == len(commits_headline)
            ), "If `commits_author` is specified, it must have the same length as `commits_headline`"

        if self.git.repository is None:
            raise RuntimeError("self.git.init() not called, tmp dir empty")

        remote = "fork" if as_ == "fork" else "origin"

        if base is None:
            base = self.main_branch_name

        if not branch:
            branch = f"{as_}/pr{self.pr_counter}"
            branch = self.get_full_branch_name(branch)

        await self.git("checkout", "--quiet", f"origin/{base}", "-b", branch)

        tmp_kwargs: dict[str, typing.Any] = {}
        if verified:
            temporary_folder = tempfile.mkdtemp()
            tmp_env = {"GNUPGHOME": temporary_folder}
            self.addCleanup(shutil.rmtree, temporary_folder)
            subprocess.run(
                ["gpg", "--import"],
                input=settings.TESTING_GPG_SECRET_KEY.encode(),
                env=self.git.prepare_safe_env(tmp_env),
            )
            await self.git(
                "config",
                "user.signingkey",
                settings.TESTING_GPG_SECRET_KEY_ID,
            )
            await self.git(
                "config",
                "user.email",
                "engineering+mergify-test@mergify.com",
            )
            tmp_kwargs.setdefault("_env", {})
            tmp_kwargs["_env"].update(tmp_env)

        title = f"{self._testMethodName}: pull request n{self.pr_counter} from {as_}"

        for idx, commit_headline in enumerate(commits_headline):
            await self._git_create_files(files[idx])

            args_commit = ["commit", "--no-edit", "-m", commit_headline]
            if commits_body is not None:
                args_commit += ["-m", commits_body[idx]]
            if commits_author is not None:
                args_commit += ["--author", commits_author[idx]]

            if verified:
                args_commit.append("-S")

            await self.git(*args_commit, **tmp_kwargs)

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
        await self.wait_for(
            "pull_request",
            {"action": "opened"},
            forward_to_engine=forward_event_to_engine,
        )

        self.created_branches.add(github_types.GitHubRefType(branch))

        return typing.cast(github_types.GitHubPullRequest, resp.json())

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

    async def create_prs_with_same_head_sha(
        self,
    ) -> tuple[github_types.GitHubPullRequest, github_types.GitHubPullRequest]:
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
        await self.wait_for_pull_request("opened")
        pr2 = typing.cast(github_types.GitHubPullRequest, resp.json())

        return pr1, pr2

    async def create_prs_with_conflicts(
        self,
    ) -> tuple[github_types.GitHubPullRequest, github_types.GitHubPullRequest]:
        pr1 = await self.create_pr(
            files={"testprsconflict": "welikeconflicts"},
        )

        pr2 = await self.create_pr(
            files={"testprsconflict": "bigconflicts"},
        )
        return pr1, pr2

    async def create_pr_with_autosquash_commit(
        self,
        commit_type: typing.Literal["fixup", "squash", "fixup=amend", "fixup=reword"],
        commit_body: str | None = None,
        autosquash_commit_body: str | None = None,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
    ) -> github_types.GitHubPullRequest:
        # if autosquash_commit_body is not None and commit_type in ("fixup=amend", "fixup=reword"):
        #     raise RuntimeError("Git doesn't allow `-m` with `--fixup=amend` and `--fixup=reword`")

        pr = await self.create_pr(commit_body=commit_body, as_=as_)

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
        await self.wait_for_push(branch_name=pr["head"]["ref"])
        await self.wait_for_pull_request("synchronize", pr["number"])

        commits = await self.get_commits(pr["number"])
        assert len(commits) == 2

        # We can't use `-m` with those (git doesn't allow it), so we
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
            f"{self.url_origin}/commits/{pull['head']['ref']}/check-runs",
        )
        assert req.status_code == 200
        return [
            typing.cast(github_types.GitHubCheckRun, check_run)
            for check_run in req.json()["check_runs"]
        ]

    async def create_check_run(
        self,
        pull: github_types.GitHubPullRequest,
        name: str = "continuous-integration/fake-ci",
        conclusion: github_types.GitHubCheckRunConclusion = "success",
        external_id: str | None = check_api.USER_CREATED_CHECKS,
    ) -> github_types.GitHubEventCheckRun:
        http_payload: dict[str, typing.Any] = {
            "name": name,
            "head_sha": pull["head"]["sha"],
        }

        if conclusion is None:
            wait_payload = tests_utils.get_check_run_event_payload(
                name=name,
                status="in_progress",
            )
            http_payload["status"] = "in_progress"
        else:
            wait_payload = tests_utils.get_check_run_event_payload(
                name=name,
                conclusion=conclusion,
                status="completed",
            )
            http_payload["conclusion"] = conclusion

        if external_id:
            http_payload["external_id"] = external_id

        await self.client_integration.post(
            f"{self.url_origin}/check-runs",
            json=http_payload,
        )

        return typing.cast(
            github_types.GitHubEventCheckRun,
            await self.wait_for(
                "check_run",
                wait_payload,
                test_id=self._extract_test_id_from_pull_request_for_check_run(pull),
            ),
        )

    @staticmethod
    def _extract_test_id_from_pull_request_for_check_run(
        pull: github_types.GitHubPullRequest,
    ) -> str:
        branch = pull["head"]["ref"]
        # eg: refs/heads/20221003073120/test_retrieve_unresolved_threads/integration/pr1
        tmp = branch.replace("refs/heads/", "")
        # eg: 20221003073120/test_retrieve_unresolved_threads/integration/pr1
        tmp_split = tmp.split("/")
        # eg: 20221003073120/test_retrieve_unresolved_threads
        tmp = "/".join((tmp_split[0], tmp_split[1]))

        # NOTE(Kontrolix): This test let us know if this pull request is a draft merge
        # queue PR. If so, in the creation process of the pr, we create a branch with
        # the QUEUE_BRANCH_PREFIX then merge code on this branch and finally rename it
        # without the prefix. But check-suite linked to our check run was created
        # before renaming so we have to look for branch name with the QUEUE_BRANCH_PREFIX
        if pull["draft"] and tmp == constants.MERGE_QUEUE_BRANCH_PREFIX.strip("/"):
            tmp = f"{merge_train.TrainCar.QUEUE_BRANCH_PREFIX}{tmp}"

        return tmp.replace("/", "-")

    async def update_check_run(
        self,
        pull: github_types.GitHubPullRequest,
        check_id: int,
        conclusion: github_types.GitHubCheckRunConclusion = "success",
    ) -> github_types.GitHubEventCheckRun:
        payload: dict[str, typing.Any] = {"head_sha": pull["head"]["sha"]}

        if conclusion is None:
            wait_payload = tests_utils.get_check_run_event_payload(
                check_id=check_id,
                status="in_progress",
            )
            payload["status"] = "in_progress"
        else:
            wait_payload = tests_utils.get_check_run_event_payload(
                check_id=check_id,
                status="completed",
                conclusion=conclusion,
            )
            payload["conclusion"] = conclusion

        await self.client_integration.patch(
            f"{self.url_origin}/check-runs/{check_id}",
            json=payload,
        )

        return typing.cast(
            github_types.GitHubEventCheckRun,
            await self.wait_for(
                "check_run",
                wait_payload,
            ),
        )

    async def create_review(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        event: typing.Literal[
            "APPROVE",
            "REQUEST_CHANGES",
            "COMMENT",
            "PENDING",
        ] = "APPROVE",
        oauth_token: github_types.GitHubOAuthToken | None = None,
    ) -> None:
        await self.client_admin.post(
            f"{self.url_origin}/pulls/{pull_number}/reviews",
            json={"event": event, "body": f"event: {event}"},
            oauth_token=oauth_token,
        )
        await self.wait_for(
            "pull_request_review",
            tests_utils.get_pull_request_review_event_payload(action="submitted"),
        )

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
    ) -> github_types.GitHubEventPullRequest:
        await self.client_integration.post(
            f"{self.url_origin}/pulls/{pull_number}/requested_reviewers",
            json={"reviewers": reviewers},
        )
        return await self.wait_for_pull_request("review_requested")

    async def get_comment(self, comment_id: int) -> github_types.GitHubComment:
        r = await self.client_integration.get(
            f"{self.url_origin}/issues/comments/{comment_id}",
        )
        return typing.cast(github_types.GitHubComment, r.json())

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
            f"{self.url_origin}/issues/{pull_number}/comments",
            json={"body": message},
        )
        await self.wait_for_issue_comment(str(pull_number), "created")
        return typing.cast(int, response.json()["id"])

    async def create_comment_as_fork(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
    ) -> int:
        return await self.create_comment(pull_number, message, as_="fork")

    async def create_comment_as_admin(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
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
        await self.wait_for_issue_comment(str(pull_number), "created")
        return comment_id

    async def get_gql_id_of_comment_to_hide(
        self,
        pr_number: int,
        comment_number: int,
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
            github_graphql_types.GraphqlHidingCommentsQuery,
            response["data"],
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
            pull_number,
            comment_number,
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
            github_graphql_types.GraphqlMinimizedCommentResponse,
            response["data"],
        )
        return data["minimizeComment"]["minimizedComment"]["isMinimized"]

    async def delete_comment(self, comment_number: int) -> None:
        resp = await self.client_integration.delete(
            f"/repos/{self.repository_ctxt.repo['owner']['login']}/{self.repository_ctxt.repo['name']}/issues/comments/{comment_number}",
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
        self,
        number: int,
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

        return typing.cast(
            github_graphql_types.GraphqlReviewThreadsQuery,
            (await self.client_integration.graphql_post(query))["data"],
        )

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
            github_graphql_types.GraphqlResolveThreadMutationResponse,
            response["data"],
        )
        return data["resolveReviewThread"]["thread"]["isResolved"]

    async def create_issue(self, title: str, body: str) -> github_types.GitHubIssue:
        resp = await self.client_integration.post(
            f"{self.url_origin}/issues",
            json={"body": body, "title": title},
        )
        # NOTE(sileht):Our GitHubApp doesn't subscribe to issues event
        # because we don't request the permissions for it.
        # await self.wait_for("issues", {"action": "created"})
        return typing.cast(github_types.GitHubIssue, resp.json())

    async def add_assignee(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        assignee: str,
    ) -> None:
        await self.client_integration.post(
            f"{self.url_origin}/issues/{pull_number}/assignees",
            json={"assignees": [assignee]},
        )
        await self.wait_for_pull_request("assigned")

    async def send_refresh(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        await refresher.send_pull_refresh(
            redis_stream=self.redis_links.stream,
            repository=self.repository_ctxt.repo,
            action="internal",
            pull_request_number=pull_number,
            source="test",
        )

    async def add_label(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        label: str,
    ) -> github_types.GitHubEventPullRequest:
        if label not in self.existing_labels:
            try:
                await self.client_integration.post(
                    f"{self.url_origin}/labels",
                    json={"name": label, "color": "000000"},
                )
            except http.HTTPClientSideError as e:
                if e.status_code != 422:
                    raise

            self.existing_labels.append(label)

        await self.client_integration.post(
            f"{self.url_origin}/issues/{pull_number}/labels",
            json={"labels": [label]},
        )
        return await self.wait_for_pull_request("labeled", pr_number=pull_number)

    async def remove_label(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        label: str,
    ) -> None:
        await self.client_integration.delete(
            f"{self.url_origin}/issues/{pull_number}/labels/{label}",
        )
        await self.wait_for_pull_request("unlabeled")

    async def get_graphql_repository_id(
        self,
        repository_ctxt: context.Repository,
    ) -> str:
        query = f"""
        query {{
            repository(owner: "{repository_ctxt.repo["owner"]["login"]}", name: "{repository_ctxt.repo["name"]}") {{
                id
            }}
        }}
        """
        response = await self.client_integration.graphql_post(query)
        return typing.cast(str, response["data"]["repository"]["id"])

    @property
    async def graphql_repository_id(self) -> str:
        if self._graphql_repository_id is None:
            self._graphql_repository_id = await self.get_graphql_repository_id(
                self.repository_ctxt,
            )
        return self._graphql_repository_id

    async def delete_graphql_branch_protection_rule(
        self,
        branch_protection_rule_id: str,
    ) -> None:
        query = f"""
        mutation {{
            deleteBranchProtectionRule(
                input: {{
                    branchProtectionRuleId: "{branch_protection_rule_id}",
                    clientMutationId: "{self.mergify_bot['id']}"
                }}
            ) {{
                clientMutationId
            }}
        }}
        """
        await self.client_admin.graphql_post(query)

    @staticmethod
    def _get_graphql_query_from_dict(
        _dict: github_graphql_types.CreateGraphqlBranchProtectionRule,
    ) -> str:
        def _get_value_as_graphql_str(val: typing.Any) -> typing.Any:
            if isinstance(val, bool):
                return str(val).lower()
            if isinstance(val, int | float):
                return str(val)
            if isinstance(val, str):
                return f'"{val}"'

            return val

        return ",\n".join(
            [f"{k}: {_get_value_as_graphql_str(v)}" for k, v in _dict.items()],
        )

    async def create_branch_protection_rule(
        self,
        branch_protection_rule: github_graphql_types.CreateGraphqlBranchProtectionRule,
    ) -> github_graphql_types.GraphqlBranchProtectionRule:
        query = f"""
        mutation {{
            createBranchProtectionRule(
                input: {{
                    {self._get_graphql_query_from_dict(branch_protection_rule)},
                    repositoryId: "{await self.graphql_repository_id}"
                }}
            ) {{
                branchProtectionRule {{
                    allowsDeletions
                    allowsForcePushes
                    dismissesStaleReviews
                    id
                    isAdminEnforced
                    pattern
                    requiredApprovingReviewCount
                    requiredStatusCheckContexts
                    requiresApprovingReviews
                    requiresCodeOwnerReviews
                    requiresCommitSignatures
                    requiresConversationResolution
                    requiresLinearHistory
                    requiresStatusChecks
                    requiresStrictStatusChecks
                    restrictsPushes
                    restrictsReviewDismissals
                }}
            }}
        }}
        """
        response = await self.client_admin.graphql_post(query)

        node_id = response["data"]["createBranchProtectionRule"][
            "branchProtectionRule"
        ].pop("id")
        self.created_branch_protection_rule_ids.add(node_id)

        return typing.cast(
            github_graphql_types.GraphqlBranchProtectionRule,
            response["data"]["createBranchProtectionRule"]["branchProtectionRule"],
        )

    async def branch_protection_unprotect(self, branch: str) -> None:
        try:
            await self.client_admin.delete(
                f"{self.url_origin}/branches/{branch}/protection",
                headers={"Accept": "application/vnd.github.luke-cage-preview+json"},
            )
        except http.HTTPNotFound as exc:
            if "Branch not protected" not in exc.message:
                raise

    async def branch_protection_protect(
        self,
        branch: str,
        protection: dict[str, typing.Any],
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
        self,
        filter_on_name: bool = True,
    ) -> list[github_types.GitHubBranch]:
        branch_name_prefix = self.get_full_branch_name()
        return [
            b
            async for b in self.client_integration.items(
                f"{self.url_origin}/branches",
                resource_name="branches",
                page_limit=10,
            )
            if not filter_on_name or b["name"].startswith(branch_name_prefix)
        ]

    async def get_branch(self, name: str) -> github_types.GitHubBranch:
        escaped_branch_name = parse.quote(name, safe="")
        return typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{escaped_branch_name}",
            ),
        )

    async def get_commits(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
        self,
        sha: github_types.SHAType,
    ) -> github_types.GitHubBranchCommit:
        return typing.cast(
            github_types.GitHubBranchCommit,
            await self.client_integration.item(f"{self.url_origin}/commits/{sha}"),
        )

    async def get_head_commit(self) -> github_types.GitHubBranchCommit:
        return typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}",
            ),
        )["commit"]

    async def get_issue_comments(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> github_types.GitHubPullRequest:
        return typing.cast(
            github_types.GitHubPullRequest,
            await self.client_integration.item(
                f"{self.url_origin}/pulls/{pull_number}",
            ),
        )

    async def get_pulls(
        self,
        **kwargs: typing.Any,
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
                    f"{self.url_origin}/pulls/{pull_number}",
                    json=payload,
                )
            ).json(),
        )

    async def close_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        r = await self.client_integration.patch(
            f"{self.url_origin}/pulls/{pull_number}",
            json={"state": "closed"},
        )
        assert r.status_code == 200
        assert r.json()["state"] == "closed"

    async def is_pull_merged(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> bool:
        try:
            await self.client_integration.get(
                f"{self.url_origin}/pulls/{pull_number}/merge",
            )
        except http.HTTPNotFound:
            return False
        else:
            return True

    async def merge_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        merge_method: typing.Literal["merge", "rebase", "squash"] = "merge",
    ) -> github_types.GitHubEventPullRequest:
        await self.client_integration.put(
            f"{self.url_origin}/pulls/{pull_number}/merge",
            json={"merge_method": merge_method},
        )
        return await self.wait_for_pull_request("closed", pull_number, merged=True)

    async def merge_pull_as_admin(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> github_types.GitHubEventPullRequest:
        await self.client_admin.put(f"{self.url_origin}/pulls/{pull_number}/merge")
        return await self.wait_for_pull_request("closed", pull_number, merged=True)

    async def get_labels(self) -> list[github_types.GitHubLabel]:
        return [
            label
            async for label in typing.cast(
                abc.AsyncGenerator[github_types.GitHubLabel, None],
                self.client_integration.items(
                    f"{self.url_origin}/labels",
                    resource_name="labels",
                    page_limit=3,
                ),
            )
        ]

    async def find_git_refs(
        self,
        url: str,
        matches: list[str],
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
                    "/orgs/mergifyio-testing/teams",
                    resource_name="teams",
                    page_limit=5,
                ),
            )
        ]

    def get_queue_rule(self, name: str) -> qr_config.QueueRule:
        return self.repository_ctxt.mergify_config["queue_rules"][
            qr_config.QueueName(name)
        ]

    def get_queue_rules(self) -> qr_config.QueueRules:
        return self.repository_ctxt.mergify_config["queue_rules"]

    def get_partition_rule(self, name: str) -> partr_config.PartitionRule:
        return self.repository_ctxt.mergify_config["partition_rules"][
            partr_config.PartitionRuleName(name)
        ]

    def get_partition_rules(self) -> partr_config.PartitionRules:
        return self.repository_ctxt.mergify_config["partition_rules"]

    async def get_train(
        self,
        partition_name: partr_config.PartitionRuleName = partr_config.DEFAULT_PARTITION_NAME,
    ) -> merge_train.Train:
        convoy = merge_train.Convoy(
            repository=self.repository_ctxt,
            ref=self.main_branch_name,
        )
        train = merge_train.Train(convoy=convoy, partition_name=partition_name)
        await train.test_helper_load_from_redis()
        return train

    async def get_convoy(self) -> merge_train.Convoy:
        convoy = merge_train.Convoy(
            repository=self.repository_ctxt,
            ref=self.main_branch_name,
        )
        await convoy.load_from_redis()
        return convoy

    @staticmethod
    def _assert_merge_queue_car(
        car: merge_train.TrainCar,
        expected_car: MergeQueueCarMatcher,
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

    @staticmethod
    def order_expected_waiting_pulls_by_priority(
        q: merge_train.Train,
        expected_waiting_pulls: list[github_types.GitHubPullRequestNumber] | None,
    ) -> list[github_types.GitHubPullRequestNumber]:
        if not expected_waiting_pulls:
            return []

        waiting_pulls = [
            embarked_pull.embarked_pull
            for embarked_pull in q._iter_embarked_pulls()
            if embarked_pull.embarked_pull.user_pull_request_number
            in expected_waiting_pulls
        ]
        return [
            wp.user_pull_request_number
            for wp in sorted(
                waiting_pulls,
                key=q._waiting_pulls_sorter,
            )
        ]

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

        await q.test_helper_load_from_redis()
        assert q._current_base_sha == expected_base_sha

        pulls_in_queue = await q.get_pulls()
        expected_waiting_pulls_ordered_by_priority = (
            self.order_expected_waiting_pulls_by_priority(q, expected_waiting_pulls)
        )
        assert len(expected_waiting_pulls_ordered_by_priority) == len(
            expected_waiting_pulls,
        )

        expected_pulls_in_queue = (
            list(
                itertools.chain.from_iterable(
                    [p.user_pull_request_numbers for p in expected_cars],
                ),
            )
            + expected_waiting_pulls_ordered_by_priority
        )
        assert pulls_in_queue == expected_pulls_in_queue

        assert len(q._cars) == len(expected_cars)
        for i, expected_car in enumerate(expected_cars):
            car = q._cars[i]
            self._assert_merge_queue_car(car, expected_car)

        waiting_pulls = [wp.user_pull_request_number for wp in q._waiting_pulls]
        assert waiting_pulls == expected_waiting_pulls

    async def push_file(
        self,
        filename: str = "random_file.txt",
        content: str = "",
        destination_branch: str | None = None,
    ) -> github_types.SHAType:
        if destination_branch is None:
            destination_branch = self.main_branch_name
        await self.git("fetch", "origin", destination_branch)
        await self.git("checkout", "-b", "random", f"origin/{destination_branch}")
        with open(self.git.repository + f"/{filename}", "w") as f:
            f.write(content)
        await self.git("add", filename)
        await self.git("commit", "--no-edit", "-m", "random update")
        head_sha = github_types.SHAType(
            (await self.git("log", "-1", "--format=%H")).strip(),
        )
        await self.git("push", "--quiet", "origin", f"random:{destination_branch}")
        await self.wait_for_push(branch_name=destination_branch)
        return head_sha

    async def change_pull_request_commit_sha(
        self,
        pull_request: github_types.GitHubPullRequest,
        commit_sha: str,
    ) -> github_types.GitHubEventPullRequest:
        await self.git("fetch", "origin", pull_request["head"]["ref"])
        await self.git(
            "checkout",
            "-b",
            "hellothere",
            f"origin/{pull_request['head']['ref']}",
        )
        await self.git("commit", "--no-edit", f"--fixup=reword:{commit_sha}")
        await self.git(
            "rebase",
            "--interactive",
            "--autosquash",
            self.main_branch_name,
            _env={"GIT_SEQUENCE_EDITOR": ":", "EDITOR": ":"},
        )
        await self.git(
            "push",
            "--force",
            "--quiet",
            "origin",
            f"hellothere:{pull_request['head']['ref']}",
        )
        await self.wait_for(
            "push",
            {"ref": f"refs/heads/{pull_request['head']['ref']}"},
        )
        return await self.wait_for_pull_request("synchronize", pull_request["number"])

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

    async def _create_queue_freeze(
        self,
        queue_name: str,
        freeze_payload: dict[str, typing.Any] | None,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
            json=freeze_payload,
        )
        assert r.status_code == expected_status_code, r.json()
        return r

    async def _delete_queue_freeze(
        self,
        queue_name: str,
        expected_status_code: int = 204,
    ) -> httpx.Response:
        r = await self.admin_app.delete(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
        )
        assert r.status_code == expected_status_code
        return r

    async def _get_queue_freeze(
        self,
        queue_name: str,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
        )
        assert r.status_code == expected_status_code
        return r

    async def _get_all_queue_freeze(
        self,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/freezes",
        )
        assert r.status_code == expected_status_code
        return r

    @contextlib.asynccontextmanager
    async def allow_merge_methods(
        self,
        repository_url: str,
        pr_number: github_types.GitHubPullRequestNumber,
        methods: tuple[typing.Literal["merge", "squash", "rebase"], ...] = (),
    ) -> typing.AsyncGenerator[None, None]:
        repository_data = await self.installation_ctxt.client.item(repository_url)

        with respx.mock(assert_all_called=False) as respx_mock:
            if "merge" not in methods:
                respx_mock.put(
                    f"{repository_url}/pulls/{pr_number}/merge",
                    json__merge_method="merge",
                ).respond(
                    405,
                    json={"message": merge_base.FORBIDDEN_MERGE_COMMITS_MSG},
                )
            if "squash" not in methods:
                respx_mock.put(
                    f"{repository_url}/pulls/{pr_number}/merge",
                    json__merge_method="squash",
                ).respond(
                    405,
                    json={"message": merge_base.FORBIDDEN_SQUASH_MERGE_MSG},
                )
            if "rebase" not in methods:
                respx_mock.put(
                    f"{repository_url}/pulls/{pr_number}/merge",
                    json__merge_method="rebase",
                ).respond(
                    405,
                    json={"message": merge_base.FORBIDDEN_REBASE_MERGE_MSG},
                )

            respx_mock.get(repository_url).respond(
                200,
                json=repository_data
                | {
                    "allow_merge_commit": "merge" in methods,
                    "allow_squash_merge": "squash" in methods,
                    "allow_rebase_merge": "rebase" in methods,
                },
            )

            respx_mock.route(host=settings.GITHUB_REST_API_HOST).pass_through()
            respx_mock.route(
                url__startswith=settings.TESTING_FORWARDER_ENDPOINT,
            ).pass_through()

            yield
