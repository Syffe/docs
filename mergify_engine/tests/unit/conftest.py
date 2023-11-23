from collections import abc
import dataclasses
import functools
import json
import os
import typing
from unittest import mock

import jinja2
import jinja2.sandbox
import pytest
import respx

from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine import subscription
from mergify_engine.clients import github
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import queue_rules as qr_config


MERGIFY_CONFIG = rules.UserConfigurationSchema(
    rules.YamlSchema(
        """
queue_rules:
  - name: default
    merge_conditions: []

partition_rules:
  - name: projectA
    conditions:
      - label=projectA

  - name: projectB
    conditions:
      - label=projectB
""",
    ),
)
QUEUE_RULES = MERGIFY_CONFIG["queue_rules"]
PARTITION_RULES = MERGIFY_CONFIG["partition_rules"]


def get_pull_queue_config(
    queue_rules: qr_config.QueueRules,
    queue_name: str,
    priority: int = 100,
) -> queue.PullQueueConfig:
    effective_priority = (
        priority
        + queue_rules[qr_config.QueueName(queue_name)].config["priority"]
        * queue.QUEUE_PRIORITY_OFFSET
    )

    return queue.PullQueueConfig(
        name=qr_config.QueueName(queue_name),
        update_method="merge",
        priority=priority,
        effective_priority=effective_priority,
        bot_account=None,
        update_bot_account=None,
        autosquash=True,
    )


@pytest.fixture
def fake_subscription(
    redis_cache: redis_utils.RedisCache,
    request: pytest.FixtureRequest,
) -> subscription.Subscription:
    marker = request.node.get_closest_marker("subscription")
    subscribed = marker is not None

    if subscribed:
        features = frozenset(
            getattr(subscription.Features, f) for f in subscription.Features.__members__
        )
        all_features = [
            typing.cast(subscription.FeaturesLiteralT, f.value)
            for f in subscription.Features
        ]
    else:
        features = frozenset([subscription.Features.PUBLIC_REPOSITORY])
        all_features = ["public_repository"]

    return subscription.Subscription(
        redis_cache,
        github_types.GitHubAccountIdType(123),
        "sub or not to sub",
        features,
        all_features,
    )


@pytest.fixture
def fake_repository(
    redis_links: redis_utils.RedisLinks,
    fake_subscription: subscription.Subscription,
) -> context.Repository:
    gh_owner = github_types.GitHubAccount(
        {
            "login": github_types.GitHubLogin("Mergifyio"),
            "id": github_types.GitHubAccountIdType(0),
            "type": "User",
            "avatar_url": "https://avatars.githubusercontent.com/u/0?v=4",
        },
    )

    gh_repo = github_types.GitHubRepository(
        {
            "full_name": "Mergifyio/mergify-engine",
            "name": github_types.GitHubRepositoryName("mergify-engine"),
            "private": False,
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType("main"),
        },
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
            "suspended_at": None,
        },
    )

    fake_client = github.AsyncGitHubInstallationClient(
        auth=github.GitHubTokenAuth("fake"),
    )
    installation = context.Installation(
        installation_json,
        fake_subscription,
        fake_client,
        redis_links,
    )
    return context.Repository(installation, gh_repo)


@pytest.fixture
def fake_convoy(fake_repository: context.Repository) -> merge_train.Convoy:
    return merge_train.Convoy(
        fake_repository,
        QUEUE_RULES,
        PARTITION_RULES,
        github_types.GitHubRefType("main"),
    )


async def build_fake_context(
    number: github_types.GitHubPullRequestNumber,
    *,
    repository: context.Repository,
    **kwargs: dict[str, typing.Any],
) -> context.Context:
    pull_request_author = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(123),
            "type": "User",
            "login": github_types.GitHubLogin("contributor"),
            "avatar_url": "",
        },
    )

    pull: github_types.GitHubPullRequest = {
        "node_id": "42",
        "locked": False,
        "assignees": [],
        "requested_reviewers": [
            {
                "id": github_types.GitHubAccountIdType(123),
                "type": "User",
                "login": github_types.GitHubLogin("jd"),
                "avatar_url": "",
            },
            {
                "id": github_types.GitHubAccountIdType(456),
                "type": "User",
                "login": github_types.GitHubLogin("sileht"),
                "avatar_url": "",
            },
        ],
        "requested_teams": [
            {"slug": github_types.GitHubTeamSlug("foobar")},
            {"slug": github_types.GitHubTeamSlug("foobaz")},
        ],
        "milestone": None,
        "title": "awesome",
        "body": "",
        "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
        "closed_at": None,
        "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
        "id": github_types.GitHubPullRequestId(123),
        "maintainer_can_modify": True,
        "user": pull_request_author,
        "labels": [],
        "rebaseable": True,
        "draft": False,
        "merge_commit_sha": None,
        "number": number,
        "commits": 1,
        "mergeable_state": "clean",
        "mergeable": True,
        "state": "open",
        "changed_files": 1,
        "head": {
            "sha": github_types.SHAType("the-head-sha"),
            "label": github_types.GitHubHeadBranchLabel(
                f"{pull_request_author['login']}:feature-branch",
            ),
            "ref": github_types.GitHubRefType("feature-branch"),
            "repo": {
                "id": github_types.GitHubRepositoryIdType(123),
                "default_branch": github_types.GitHubRefType("main"),
                "name": github_types.GitHubRepositoryName("mergify-engine"),
                "full_name": "contributor/mergify-engine",
                "archived": False,
                "private": False,
                "owner": pull_request_author,
                "url": "https://api.github.com/repos/contributor/mergify-engine",
                "html_url": "https://github.com/contributor/mergify-engine",
            },
            "user": pull_request_author,
        },
        "merged": False,
        "merged_by": None,
        "merged_at": None,
        "html_url": "https://...",
        "issue_url": "",
        "base": {
            "label": github_types.GitHubBaseBranchLabel("mergify_engine:main"),
            "ref": github_types.GitHubRefType("main"),
            "repo": repository.repo,  # type: ignore [typeddict-item]
            "sha": github_types.SHAType("the-base-sha"),
            "user": repository.repo["owner"],
        },
    }
    pull.update(kwargs)  # type: ignore
    return context.Context(repository, pull)


ContextGetterFixture = abc.Callable[
    ...,
    abc.Coroutine[typing.Any, typing.Any, context.Context],
]


@pytest.fixture
def context_getter(fake_repository: context.Repository) -> ContextGetterFixture:
    fake_repository._caches.mergify_config.set(MERGIFY_CONFIG)
    return functools.partial(build_fake_context, repository=fake_repository)


@pytest.fixture
async def jinja_environment() -> jinja2.sandbox.SandboxedEnvironment:
    return jinja2.sandbox.SandboxedEnvironment(
        undefined=jinja2.StrictUndefined,
        enable_async=True,
    )


@dataclasses.dataclass
class FakePullRequest(condition_value_querier.BasePullRequest):
    attrs: dict[str, condition_value_querier.PullRequestAttributeType]

    async def __getattr__(
        self,
        name: str,
    ) -> condition_value_querier.PullRequestAttributeType:
        fancy_name = name.replace("_", "-")
        try:
            return self.attrs[fancy_name]
        except KeyError:
            raise condition_value_querier.PullRequestAttributeError(name=fancy_name)

    def sync_checks(self) -> None:
        self.attrs["check-success-or-neutral"] = (
            self.attrs.get("check-success", [])  # type: ignore
            + self.attrs.get("check-neutral", [])
            + self.attrs.get("check-pending", [])
        )
        self.attrs["check-success-or-neutral-or-pending"] = (
            self.attrs.get("check-success", [])  # type: ignore
            + self.attrs.get("check-neutral", [])
            + self.attrs.get("check-pending", [])
        )
        self.attrs["check"] = (
            self.attrs.get("check-success", [])  # type: ignore
            + self.attrs.get("check-neutral", [])
            + self.attrs.get("check-pending", [])
            + self.attrs.get("check-failure", [])
            + self.attrs.get("check-timed-out", [])
            + self.attrs.get("check-skipped", [])
        )

        self.attrs["status-success"] = self.attrs.get("check-success", [])
        self.attrs["status-neutral"] = self.attrs.get("check-neutral", [])
        self.attrs["status-failure"] = self.attrs.get("check-failure", [])


@pytest.fixture(autouse=True)
def respx_configuration(
    respx_mock: respx.MockRouter,
) -> abc.Generator[None, None, None]:
    # This ensures 0 real httpx called is done
    yield
    # And that all respx mocked endpoint are used
    respx_mock.assert_all_called()


@pytest.fixture(autouse=True)
def fake_github_app_info() -> abc.Generator[None, None, None]:
    app = github_types.GitHubApp(
        {
            "id": 0,
            "name": "Mergify-test",
            "slug": "mergify-test",
            "owner": {
                "id": github_types.GitHubAccountIdType(1),
                "login": github_types.GitHubLogin("Mergifyio"),
                "type": "Organization",
                "avatar_url": "",
            },
        },
    )

    bot = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(0),
            "login": github_types.GitHubLogin("Mergify-test[bot]"),
            "type": "Bot",
            "avatar_url": "",
        },
    )

    with mock.patch.object(github.GitHubAppInfo, "_app", app), mock.patch.object(
        github.GitHubAppInfo,
        "_bot",
        bot,
    ):
        yield


@pytest.fixture
async def fake_mergify_bot(
    redis_links: redis_utils.RedisLinks,
) -> github_types.GitHubAccount:
    return await github.GitHubAppInfo.get_bot(redis_links.cache)


@pytest.fixture
def sample_events() -> dict[str, tuple[github_types.GitHubEventType, typing.Any]]:
    events = {}
    event_dir = os.path.join(os.path.dirname(__file__), "events")

    for filename in os.listdir(event_dir):
        event_type = typing.cast(github_types.GitHubEventType, filename.split(".")[0])
        with open(os.path.join(event_dir, filename)) as event:
            events[filename] = (event_type, json.load(event))

    return events


@pytest.fixture
def sample_ci_events_to_process(
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> dict[str, github_events.CIEventToProcess]:
    ci_events = {}

    for filename, (event_type, event) in sample_events.items():
        if event_type in ("workflow_run", "workflow_job"):
            ci_events[filename] = github_events.CIEventToProcess(event_type, "", event)

    return ci_events


@pytest.fixture(autouse=True)
async def clear_redis_database_between_tests(
    redis_links: redis_utils.RedisLinks,
) -> abc.AsyncGenerator[None, None]:
    # No need to do anything else, the code in `redis_links` fixture
    # already cleans everything
    yield


@pytest.fixture
def a_pull_request() -> github_types.GitHubPullRequest:
    gh_owner = github_types.GitHubAccount(
        {
            "login": github_types.GitHubLogin("user"),
            "id": github_types.GitHubAccountIdType(0),
            "type": "User",
            "avatar_url": "",
        },
    )

    gh_repo = github_types.GitHubRepository(
        {
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "id": github_types.GitHubRepositoryIdType(456),
            "full_name": "user/repo",
            "name": github_types.GitHubRepositoryName("repo"),
            "private": False,
            "owner": gh_owner,
        },
    )

    return github_types.GitHubPullRequest(
        {
            "node_id": "42",
            "locked": False,
            "assignees": [],
            "requested_reviewers": [],
            "requested_teams": [],
            "milestone": None,
            "title": "",
            "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
            "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
            "closed_at": None,
            "id": github_types.GitHubPullRequestId(0),
            "maintainer_can_modify": False,
            "rebaseable": False,
            "draft": False,
            "merge_commit_sha": None,
            "labels": [],
            "number": github_types.GitHubPullRequestNumber(6),
            "commits": 1,
            "merged": True,
            "state": "closed",
            "changed_files": 1,
            "html_url": "<html_url>",
            "issue_url": "",
            "base": {
                "label": github_types.GitHubBaseBranchLabel(""),
                "sha": github_types.SHAType("sha"),
                "user": {
                    "login": github_types.GitHubLogin("user"),
                    "id": github_types.GitHubAccountIdType(0),
                    "type": "User",
                    "avatar_url": "",
                },
                "ref": github_types.GitHubRefType("ref"),
                "repo": gh_repo,
            },
            "head": {
                "label": github_types.GitHubHeadBranchLabel(""),
                "sha": github_types.SHAType("old-sha-one"),
                "ref": github_types.GitHubRefType("fork"),
                "user": {
                    "login": github_types.GitHubLogin("user"),
                    "id": github_types.GitHubAccountIdType(0),
                    "type": "User",
                    "avatar_url": "",
                },
                "repo": {
                    "archived": False,
                    "url": "",
                    "html_url": "",
                    "default_branch": github_types.GitHubRefType(""),
                    "id": github_types.GitHubRepositoryIdType(123),
                    "full_name": "fork/other",
                    "name": github_types.GitHubRepositoryName("other"),
                    "private": False,
                    "owner": {
                        "login": github_types.GitHubLogin("user"),
                        "id": github_types.GitHubAccountIdType(0),
                        "type": "User",
                        "avatar_url": "",
                    },
                },
            },
            "user": {
                "login": github_types.GitHubLogin("user"),
                "id": github_types.GitHubAccountIdType(0),
                "type": "User",
                "avatar_url": "",
            },
            "merged_by": None,
            "merged_at": None,
            "mergeable_state": "clean",
            "mergeable": True,
            "body": None,
        },
    )
