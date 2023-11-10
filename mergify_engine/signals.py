import abc
import collections
import datetime
import importlib.metadata
import logging
import typing

import daiquiri
import typing_extensions

from mergify_engine import github_types
from mergify_engine.models import enumerations
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import checks as merge_train_checks
from mergify_engine.rules.config import partition_rules as partr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context


LOG = daiquiri.getLogger(__name__)

EventName = typing.Literal[
    "action.assign",
    "action.backport",
    "action.close",
    "action.comment",
    "action.copy",
    "action.delete_head_branch",
    "action.dismiss_reviews",
    "action.edit",
    "action.label",
    "action.merge",
    "action.post_check",
    "action.github_actions",
    "action.queue.enter",
    "action.queue.change",
    "action.queue.checks_start",
    "action.queue.checks_end",
    "action.queue.leave",
    "action.queue.merged",
    "action.rebase",
    "action.refresh",
    "action.request_reviewers",  # MRGFY-2461
    "action.request_reviews",
    "action.requeue",
    "action.review",
    "action.squash",
    "action.unqueue",
    "action.update",
    "queue.freeze.create",
    "queue.freeze.update",
    "queue.freeze.delete",
    "queue.pause.create",
    "queue.pause.update",
    "queue.pause.delete",
]


class EventMetadata(typing_extensions.TypedDict):
    pass


class EventCommentMetadata(EventMetadata, total=False):
    message: str


class EventCloseMetadata(EventMetadata, total=False):
    message: str


class EventReviewMetadata(EventMetadata, total=False):
    review_type: str
    reviewer: str | None
    message: str | None


class EventCopyMetadata(EventMetadata, total=False):
    to: str
    pull_request_number: int
    conflicts: bool


class EventAssignMetadata(EventMetadata, total=False):
    added: list[str]
    removed: list[str]


class EventEditMetadata(EventMetadata, total=False):
    draft: bool


class EventLabelMetadata(EventMetadata, total=False):
    added: list[str]
    removed: list[str]


class EventPostCheckMetadata(EventMetadata, total=False):
    conclusion: str
    title: str
    summary: str


class EventRequestReviewsMetadata(EventMetadata, total=False):
    reviewers: list[str]
    team_reviewers: list[str]


class EventDismissReviewsMetadata(EventMetadata, total=False):
    users: list[str]


class EventGithubActionsMetadata(EventMetadata, total=False):
    workflow: str
    inputs: dict[str, str | int | bool]


class EventQueueEnterMetadata(EventMetadata, total=False):
    queue_name: str
    branch: str
    position: int
    queued_at: datetime.datetime
    partition_name: partr_config.PartitionRuleName | None


class EventQueueLeaveMetadata(EventMetadata, total=False):
    reason: str
    merged: bool
    queue_name: str
    branch: str
    position: int
    partition_name: partr_config.PartitionRuleName
    queued_at: datetime.datetime
    seconds_waiting_for_schedule: int
    seconds_waiting_for_freeze: int


class EventQueueChangeMetadata(EventMetadata, total=False):
    queue_name: str
    partition_name: partr_config.PartitionRuleName
    size: int
    running_checks: int


class EventDeleteHeadBranchMetadata(EventMetadata, total=False):
    branch: str


# Like GitHubCheckRunConclusion but with "pending" instead of None
ChecksConclusion = typing.Literal[
    "success",
    "failure",
    "error",
    "cancelled",
    "skipped",
    "action_required",
    "timed_out",
    "neutral",
    "stale",
    "pending",
]


class SpeculativeCheckPullRequest(typing_extensions.TypedDict, total=False):
    number: int
    in_place: bool
    checks_timed_out: bool
    checks_conclusion: ChecksConclusion | enumerations.CheckConclusionWithStatuses
    checks_started_at: datetime.datetime | None
    checks_ended_at: datetime.datetime | None
    unsuccessful_checks: list[merge_train_checks.QueueCheck.Serialized]


class EventMergeMetadata(EventMetadata, total=False):
    branch: str


class EventQueueMergedMetadata(EventMetadata, total=False):
    branch: str
    partition_names: list[partr_config.PartitionRuleName]
    queue_name: str
    queued_at: datetime.datetime


class EventQueueChecksEndMetadata(EventMetadata, total=False):
    aborted: bool
    abort_code: queue_utils.AbortCodeT | enumerations.QueueChecksAbortCode | None
    abort_reason: str | None
    abort_status: typing.Literal[
        "DEFINITIVE", "REEMBARKED"
    ] | enumerations.QueueChecksAbortStatus
    branch: str
    partition_name: partr_config.PartitionRuleName
    position: int | None
    queue_name: str
    queued_at: datetime.datetime
    speculative_check_pull_request: SpeculativeCheckPullRequest


class EventQueueChecksStartMetadata(EventMetadata, total=False):
    branch: str
    partition_name: partr_config.PartitionRuleName | None
    position: int
    queue_name: str
    queued_at: datetime.datetime
    start_reason: str
    speculative_check_pull_request: SpeculativeCheckPullRequest


class Actor(typing_extensions.TypedDict):
    type: typing.Literal[
        "user", "application"
    ] | enumerations.GithubAuthenticatedActorType
    id: int
    name: str


class EventQueueFreezeCreateMetadata(EventMetadata, total=False):
    queue_name: str
    reason: str
    cascading: bool
    created_by: Actor


class EventQueueFreezeUpdateMetadata(EventMetadata, total=False):
    queue_name: str
    reason: str
    cascading: bool
    updated_by: Actor


class EventQueueFreezeDeleteMetadata(EventMetadata, total=False):
    queue_name: str
    deleted_by: Actor


class EventQueuePauseCreateMetadata(EventMetadata, total=False):
    reason: str
    created_by: Actor


class EventQueuePauseUpdateMetadata(EventMetadata, total=False):
    reason: str
    updated_by: Actor


class EventQueuePauseDeleteMetadata(EventMetadata, total=False):
    deleted_by: Actor


class EventNoMetadata(EventMetadata):
    pass


SignalT = collections.abc.Callable[
    [
        "context.Repository",
        github_types.GitHubPullRequestNumber | None,
        github_types.GitHubRefType | None,
        EventName,
        EventMetadata | None,
        str,
    ],
    collections.abc.Coroutine[None, None, None],
]


class SignalBase(abc.ABC):
    @abc.abstractmethod
    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event: EventName,
        metadata: EventMetadata,
        trigger: str,
    ) -> None:
        pass


class NoopSignal(SignalBase):
    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event: EventName,
        metadata: EventMetadata,
        trigger: str,
    ) -> None:
        pass


global SIGNALS
SIGNALS: dict[str, SignalT] = {}


def unregister() -> None:
    global SIGNALS
    SIGNALS = {}


def register() -> None:
    global SIGNALS
    for ep in importlib.metadata.entry_points(group="mergify_signals"):
        try:
            # NOTE(sileht): literal import is safe here, we control installed signal packages
            # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
            SIGNALS[ep.name] = ep.load()()
        except ImportError:
            LOG.error("failed to load signal: %s", ep.name, exc_info=True)


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal[
        "action.squash",
        "action.rebase",
        "action.refresh",
        "action.requeue",
        "action.unqueue",
        "action.update",
    ],
    metadata: EventNoMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.merge"],
    metadata: EventMergeMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.delete_head_branch"],
    metadata: EventDeleteHeadBranchMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.close"],
    metadata: EventCloseMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.comment"],
    metadata: EventCommentMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.review"],
    metadata: EventReviewMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.assign"],
    metadata: EventAssignMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.edit"],
    metadata: EventEditMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.label"],
    metadata: EventLabelMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.post_check"],
    metadata: EventPostCheckMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.dismiss_reviews"],
    metadata: EventDismissReviewsMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.request_reviews"],
    metadata: EventRequestReviewsMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.backport", "action.copy"],
    metadata: EventCopyMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.github_actions"],
    metadata: EventGithubActionsMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.enter"],
    metadata: EventQueueEnterMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.change"],
    metadata: EventQueueChangeMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.leave"],
    metadata: EventQueueLeaveMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.checks_end"],
    metadata: EventQueueChecksEndMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.checks_start"],
    metadata: EventQueueChecksStartMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber,
    base_ref: github_types.GitHubRefType,
    event: typing.Literal["action.queue.merged"],
    metadata: EventQueueMergedMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.freeze.create"],
    metadata: EventQueueFreezeCreateMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.freeze.update"],
    metadata: EventQueueFreezeUpdateMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.freeze.delete"],
    metadata: EventQueueFreezeDeleteMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.pause.create"],
    metadata: EventQueuePauseCreateMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.pause.update"],
    metadata: EventQueuePauseUpdateMetadata,
    trigger: str,
) -> None:
    ...


@typing.overload
async def send(
    repository: "context.Repository",
    pull_request: None,
    base_ref: None,
    event: typing.Literal["queue.pause.delete"],
    metadata: EventQueuePauseDeleteMetadata,
    trigger: str,
) -> None:
    ...


async def send(
    repository: "context.Repository",
    pull_request: github_types.GitHubPullRequestNumber | None,
    base_ref: github_types.GitHubRefType | None,
    event: EventName,
    metadata: EventMetadata,
    trigger: str,
) -> None:
    # circular import
    from mergify_engine import exceptions

    for name, signal in SIGNALS.items():
        try:
            await signal(
                repository,
                pull_request,
                base_ref,
                event,
                metadata,
                trigger,
            )
        except Exception as e:
            if exceptions.need_retry(e) is not None or exceptions.should_be_ignored(e):
                level = logging.WARNING
            else:
                level = logging.ERROR
            LOG.log(level, "failed to run signal: %s", name, exc_info=True)
