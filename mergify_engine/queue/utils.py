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
    "UNEXPECTED_QUEUE_CHANGE",
    "PR_FROZEN_NO_CASCADING",
    "TARGET_BRANCH_MISSING",
    "TARGET_BRANCH_CHANGED",
    "PR_UNEXPECTEDLY_FAILED_TO_MERGE",
    "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS",
    "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE",
]

UnqueueCodeT = typing.Literal["PR_MERGED"] | AbortCodeT


@dataclasses.dataclass
class BaseUnqueueReason:
    message: typing.ClassVar[str]
    unqueue_code: typing.ClassVar[UnqueueCodeT]

    def __str__(self) -> str:
        f = string.Formatter()
        fields = {
            field_name
            for _, field_name, _, _ in f.parse(self.message)
            if field_name is not None
        }
        values = {k: getattr(self, k) for k in fields}
        return self.message.format(**values)


@dataclasses.dataclass
class PrDequeued(BaseUnqueueReason):
    message = "Pull request #{pr_number} has been dequeued{details}"
    unqueue_code: typing.ClassVar[typing.Literal["PR_DEQUEUED"]] = "PR_DEQUEUED"
    pr_number: int
    details: str


@dataclasses.dataclass
class PrMerged(BaseUnqueueReason):
    message = "Pull request #{pr_number} has been merged automatically at *{sha}*"
    unqueue_code: typing.ClassVar[typing.Literal["PR_MERGED"]] = "PR_MERGED"
    pr_number: int
    sha: github_types.SHAType


@dataclasses.dataclass
class PrAheadDequeued(BaseUnqueueReason):
    message = "Pull request #{pr_number} which was ahead in the queue has been dequeued"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_DEQUEUED"]
    ] = "PR_AHEAD_DEQUEUED"
    pr_number: int


@dataclasses.dataclass
class PrAheadFailedToMerge(BaseUnqueueReason):
    message = "Pull requests combination ({_formated_pr_numbers}) which was ahead in the queue failed to get merged"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_FAILED_TO_MERGE"]
    ] = "PR_AHEAD_FAILED_TO_MERGE"
    pr_numbers: list[int]

    @property
    def _formated_pr_numbers(self) -> str:
        return ", ".join(f"#{pr_number}" for pr_number in self.pr_numbers)


@dataclasses.dataclass
class PrUnexpectedlyFailedToMerge(BaseUnqueueReason):
    message = "Pull request unexpectedly failed to get merged"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_UNEXPECTEDLY_FAILED_TO_MERGE"]
    ] = "PR_UNEXPECTEDLY_FAILED_TO_MERGE"


# FIXME(sileht): should be something like PRQueuePriorityChanged
@dataclasses.dataclass
class PrWithHigherPriorityQueued(BaseUnqueueReason):
    message = "Pull request #{pr_number} with higher priority has been queued"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_WITH_HIGHER_PRIORITY_QUEUED"]
    ] = "PR_WITH_HIGHER_PRIORITY_QUEUED"
    pr_number: int


@dataclasses.dataclass
class ChecksStoppedBecauseMergeQueuePause(BaseUnqueueReason):
    message = "The checks have been interrupted because the merge queue is paused on this repository"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE"]
    ] = "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE"


@dataclasses.dataclass
class PrFrozenNoCascading(BaseUnqueueReason):
    message = "The pull request was frozen by a freeze with cascading effect disabled"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_FROZEN_NO_CASCADING"]
    ] = "PR_FROZEN_NO_CASCADING"


@dataclasses.dataclass
class PrQueuedTwice(BaseUnqueueReason):
    message = "The pull request has been queued twice"
    unqueue_code: typing.ClassVar[typing.Literal["PR_QUEUED_TWICE"]] = "PR_QUEUED_TWICE"


@dataclasses.dataclass
class SpeculativeCheckNumberReduced(BaseUnqueueReason):
    message = "The number of speculative checks has been reduced"
    unqueue_code: typing.ClassVar[
        typing.Literal["SPECULATIVE_CHECK_NUMBER_REDUCED"]
    ] = "SPECULATIVE_CHECK_NUMBER_REDUCED"


@dataclasses.dataclass
class ChecksTimeout(BaseUnqueueReason):
    message = "The queue conditions cannot be satisfied due to checks timeout"
    unqueue_code: typing.ClassVar[typing.Literal["CHECKS_TIMEOUT"]] = "CHECKS_TIMEOUT"


@dataclasses.dataclass
class ChecksFailed(BaseUnqueueReason):
    message = "The queue conditions cannot be satisfied due to failing checks"
    unqueue_code: typing.ClassVar[typing.Literal["CHECKS_FAILED"]] = "CHECKS_FAILED"


@dataclasses.dataclass
class QueueRuleMissing(BaseUnqueueReason):
    message = "The associated queue rule does not exist anymore"
    unqueue_code: typing.ClassVar[
        typing.Literal["QUEUE_RULE_MISSING"]
    ] = "QUEUE_RULE_MISSING"


@dataclasses.dataclass
class TargetBranchMissing(BaseUnqueueReason):
    message = "The target branch does not exist anymore `{ref}`"
    unqueue_code: typing.ClassVar[
        typing.Literal["TARGET_BRANCH_MISSING"]
    ] = "TARGET_BRANCH_MISSING"
    ref: str


@dataclasses.dataclass
class TargetBranchChanged(BaseUnqueueReason):
    message = "The pull request target branch has changed"
    unqueue_code: typing.ClassVar[
        typing.Literal["TARGET_BRANCH_CHANGED"]
    ] = "TARGET_BRANCH_CHANGED"


@dataclasses.dataclass
class UnexpectedQueueChange(BaseUnqueueReason):
    message = "Unexpected queue change: {change}"
    unqueue_code: typing.ClassVar[
        typing.Literal["UNEXPECTED_QUEUE_CHANGE"]
    ] = "UNEXPECTED_QUEUE_CHANGE"
    change: str


@dataclasses.dataclass
class MaximumBatchFailureResolutionAttemptsReached(BaseUnqueueReason):
    message = "The maximum batch failure resolution attempts has been reached"
    unqueue_code: typing.ClassVar[
        typing.Literal["BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS"]
    ] = "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS"


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

    payload = utils.get_hidden_payload_from_comment_body(pull_request_body)
    if payload is None:
        return False

    return payload.get("merge-queue-pr", False)


def is_same_batch(
    first_pull_position: int, second_pull_position: int, batch_size: int
) -> bool:
    return math.ceil(first_pull_position / batch_size) == math.ceil(
        second_pull_position / batch_size
    )
