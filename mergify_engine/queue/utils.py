import dataclasses
import math
import string
import typing

from mergify_engine import constants
from mergify_engine import github_types
from mergify_engine import utils


# FIXME(sileht): Rework statistics/signal to handle PR_MERGED and we can drop AbortCodeT
AbortCodeT = typing.Literal[
    "PR_DEQUEUED",
    "PR_AHEAD_DEQUEUED",
    "PR_AHEAD_FAILED_TO_MERGE",
    "PR_WITH_HIGHER_PRIORITY_QUEUED",
    "PR_QUEUED_TWICE",
    "SPECULATIVE_CHECK_NUMBER_REDUCED",
    "CHECKS_TIMEOUT",
    "CHECKS_FAILED",
    "QUEUE_RULE_MISSING",
    "UNEXPECTED_QUEUE_CHANGE",  # retrocompatibility <= 8.0.0
    "PR_FROZEN_NO_CASCADING",
    "BASE_BRANCH_MISSING",
    "BASE_BRANCH_CHANGED",
    "PR_UNEXPECTEDLY_FAILED_TO_MERGE",
    "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS",
    "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE",
    "CONFLICT_WITH_BASE_BRANCH",
    "CONFLICT_WITH_PULL_AHEAD",
    "BRANCH_UPDATE_FAILED",
    "DRAFT_PULL_REQUEST_CHANGED",
    "PULL_REQUEST_UPDATED",
    "MERGE_QUEUE_RESET",
]

DequeueCodeT = typing.Literal["PR_MERGED"] | AbortCodeT


@dataclasses.dataclass
class BaseQueueCancelReason:
    """Base class for a check cancel happening in the queue."""

    message: typing.ClassVar[str]
    dequeue_code: typing.ClassVar[DequeueCodeT]

    __registry__: typing.ClassVar[
        dict[DequeueCodeT, type["BaseQueueCancelReason"]]
    ] = {}

    def __str__(self) -> str:
        f = string.Formatter()
        fields = {
            field_name
            for _, field_name, _, _ in f.parse(self.message)
            if field_name is not None
        }
        values = {k: getattr(self, k) for k in fields}
        return self.message.format(**values)

    def __init_subclass__(cls, **kwargs: typing.Any):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "dequeue_code"):
            cls.__registry__[cls.dequeue_code] = cls

    Serialized = dict[str, typing.Any]

    def serialized(self) -> Serialized:
        data = dataclasses.asdict(self)
        data["dequeue_code"] = self.dequeue_code
        return data

    @classmethod
    def deserialized(cls, data: Serialized) -> "BaseQueueCancelReason":
        dequeue_code = data.pop("dequeue_code")
        cls = BaseQueueCancelReason.__registry__[dequeue_code]
        return cls(**data)


class BaseDequeueReason(BaseQueueCancelReason):
    """Base class for a check cancel followed by a PR removed from the queue."""


@dataclasses.dataclass
class PrDequeued(BaseDequeueReason):
    message = "Pull request #{pr_number} has been dequeued{details}"
    dequeue_code: typing.ClassVar[typing.Literal["PR_DEQUEUED"]] = "PR_DEQUEUED"
    pr_number: int
    details: str

    def str_without_details(self) -> str:
        return self.message.format(pr_number=self.pr_number, details="")

    # FIXME(sileht): Should it be a dedicated class, PrManuallyDequeued?
    DEQUEUE_COMMAND_MESSAGE: typing.ClassVar[str] = " by a `dequeue` command"

    def is_dequeue_command(self) -> bool:
        return self.details == self.DEQUEUE_COMMAND_MESSAGE

    @classmethod
    def create_dequeue_command(
        cls,
        pr_number: github_types.GitHubPullRequestNumber,
    ) -> "PrDequeued":
        return cls(pr_number, cls.DEQUEUE_COMMAND_MESSAGE)


@dataclasses.dataclass
class PrMerged(BaseDequeueReason):
    message = "Pull request #{pr_number} has been merged automatically at *{sha}*"
    dequeue_code: typing.ClassVar[typing.Literal["PR_MERGED"]] = "PR_MERGED"
    pr_number: int
    sha: github_types.SHAType


@dataclasses.dataclass
class PrAheadDequeued(BaseQueueCancelReason):
    message = "Pull request #{pr_number} which was ahead in the queue has been dequeued"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_DEQUEUED"]
    ] = "PR_AHEAD_DEQUEUED"
    pr_number: int


@dataclasses.dataclass
class PrAheadFailedToMerge(BaseQueueCancelReason):
    message = "Pull requests combination ({_formated_pr_numbers}) which was ahead in the queue failed to get merged"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_FAILED_TO_MERGE"]
    ] = "PR_AHEAD_FAILED_TO_MERGE"
    pr_numbers: list[int]

    @property
    def _formated_pr_numbers(self) -> str:
        return ", ".join(f"#{pr_number}" for pr_number in self.pr_numbers)


@dataclasses.dataclass
class PrUnexpectedlyFailedToMerge(BaseDequeueReason):
    message = "Pull request unexpectedly failed to get merged"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_UNEXPECTEDLY_FAILED_TO_MERGE"]
    ] = "PR_UNEXPECTEDLY_FAILED_TO_MERGE"


# FIXME(sileht): should be something like PRQueuePriorityChanged
@dataclasses.dataclass
class PrWithHigherPriorityQueued(BaseQueueCancelReason):
    message = "Pull request #{pr_number} with higher priority has been queued"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_WITH_HIGHER_PRIORITY_QUEUED"]
    ] = "PR_WITH_HIGHER_PRIORITY_QUEUED"
    pr_number: int


@dataclasses.dataclass
class ChecksStoppedBecauseMergeQueuePause(BaseQueueCancelReason):
    message = "The checks have been interrupted because the merge queue is paused on this repository"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE"]
    ] = "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE"


@dataclasses.dataclass
class PrFrozenNoCascading(BaseQueueCancelReason):
    message = "The pull request was frozen by a freeze with cascading effect disabled"
    dequeue_code: typing.ClassVar[
        typing.Literal["PR_FROZEN_NO_CASCADING"]
    ] = "PR_FROZEN_NO_CASCADING"


@dataclasses.dataclass
class PrQueuedTwice(BaseQueueCancelReason):
    message = "The pull request has been queued twice"
    dequeue_code: typing.ClassVar[typing.Literal["PR_QUEUED_TWICE"]] = "PR_QUEUED_TWICE"


@dataclasses.dataclass
class SpeculativeCheckNumberReduced(BaseQueueCancelReason):
    message = "The number of speculative checks has been reduced"
    dequeue_code: typing.ClassVar[
        typing.Literal["SPECULATIVE_CHECK_NUMBER_REDUCED"]
    ] = "SPECULATIVE_CHECK_NUMBER_REDUCED"


@dataclasses.dataclass
class ChecksTimeout(BaseDequeueReason):
    message = "The queue conditions cannot be satisfied due to checks timeout"
    dequeue_code: typing.ClassVar[typing.Literal["CHECKS_TIMEOUT"]] = "CHECKS_TIMEOUT"


@dataclasses.dataclass
class ChecksFailed(BaseDequeueReason):
    message = "The queue conditions cannot be satisfied due to failing checks"
    dequeue_code: typing.ClassVar[typing.Literal["CHECKS_FAILED"]] = "CHECKS_FAILED"


@dataclasses.dataclass
class QueueRuleMissing(BaseDequeueReason):
    message = "The associated queue rule does not exist anymore"
    dequeue_code: typing.ClassVar[
        typing.Literal["QUEUE_RULE_MISSING"]
    ] = "QUEUE_RULE_MISSING"


@dataclasses.dataclass
class BaseBranchMissing(BaseDequeueReason):
    message = "The base branch does not exist anymore `{ref}`"
    dequeue_code: typing.ClassVar[
        typing.Literal["BASE_BRANCH_MISSING"]
    ] = "BASE_BRANCH_MISSING"
    ref: str


@dataclasses.dataclass
class BaseBranchChanged(BaseDequeueReason):
    message = "The pull request base branch has changed"
    dequeue_code: typing.ClassVar[
        typing.Literal["BASE_BRANCH_CHANGED"]
    ] = "BASE_BRANCH_CHANGED"


# Kept for retrocompatibility <= 8.0.0
@dataclasses.dataclass
class UnexpectedQueueChange(BaseDequeueReason):
    message = "Unexpected queue change: {change}"
    dequeue_code: typing.ClassVar[
        typing.Literal["UNEXPECTED_QUEUE_CHANGE"]
    ] = "UNEXPECTED_QUEUE_CHANGE"
    change: str


@dataclasses.dataclass
class MaximumBatchFailureResolutionAttemptsReached(BaseDequeueReason):
    message = "The maximum batch failure resolution attempts has been reached"
    dequeue_code: typing.ClassVar[
        typing.Literal["BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS"]
    ] = "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS"


@dataclasses.dataclass
class ConflictWithBaseBranch(BaseDequeueReason):
    message = "The pull request conflicts with the base branch"
    dequeue_code: typing.ClassVar[
        typing.Literal["CONFLICT_WITH_BASE_BRANCH"]
    ] = "CONFLICT_WITH_BASE_BRANCH"


@dataclasses.dataclass
class ConflictWithPullAhead(BaseDequeueReason):
    message = "The pull request conflicts with at least one pull request ahead in queue"
    dequeue_code: typing.ClassVar[
        typing.Literal["CONFLICT_WITH_PULL_AHEAD"]
    ] = "CONFLICT_WITH_PULL_AHEAD"


@dataclasses.dataclass
class BranchUpdateFailed(BaseDequeueReason):
    message = "The pull request can't be updated"
    dequeue_code: typing.ClassVar[
        typing.Literal["BRANCH_UPDATE_FAILED"]
    ] = "BRANCH_UPDATE_FAILED"


@dataclasses.dataclass
class DraftPullRequestChanged(BaseDequeueReason):
    message = "The draft pull request has been unexpectedly changed"
    dequeue_code: typing.ClassVar[
        typing.Literal["DRAFT_PULL_REQUEST_CHANGED"]
    ] = "DRAFT_PULL_REQUEST_CHANGED"


@dataclasses.dataclass
class PullRequestUpdated(BaseDequeueReason):
    message = "The pull request #{pr_number} has been manually updated"
    dequeue_code: typing.ClassVar[
        typing.Literal["PULL_REQUEST_UPDATED"]
    ] = "PULL_REQUEST_UPDATED"
    pr_number: int


@dataclasses.dataclass
class QueueReset(BaseQueueCancelReason):
    message = "Merge queue reset: {reason}"
    dequeue_code: typing.ClassVar[
        typing.Literal["MERGE_QUEUE_RESET"]
    ] = "MERGE_QUEUE_RESET"
    reason: str


_HiddenQueuePullRequestTag = typing.TypedDict(
    "_HiddenQueuePullRequestTag",
    {"merge-queue-pr": bool},
    total=False,
)


class HiddenQueuePullRequestTag(utils.MergifyHiddenPayload, _HiddenQueuePullRequestTag):
    pass


def is_merge_queue_pr(pull: github_types.GitHubPullRequest) -> bool:
    return (
        pull["title"].startswith("merge queue:")
        # FIXME(jd): drop me in version >= 9.0.0
        or pull["title"].startswith("merge-queue:")
    ) and (
        # NOTE(greesb): For retrocompatibility, to remove once there are
        # no more PR using this.
        pull["head"]["ref"].startswith(constants.MERGE_QUEUE_BRANCH_PREFIX)
        or is_pr_body_a_merge_queue_pr(pull["body"])
    )


def is_pr_body_a_merge_queue_pr(pull_request_body: str | None) -> bool:
    if pull_request_body is None:
        return False

    try:
        payload = typing.cast(
            HiddenQueuePullRequestTag,
            utils.deserialize_hidden_payload(
                pull_request_body,
            ),
        )
    except utils.MergifyHiddenPayloadNotFound:
        return False

    return payload.get("merge-queue-pr", False)


def is_same_batch(
    first_pull_position: int,
    second_pull_position: int,
    batch_size: int,
) -> bool:
    return math.ceil(first_pull_position / batch_size) == math.ceil(
        second_pull_position / batch_size,
    )
