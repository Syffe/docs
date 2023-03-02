import dataclasses
import math
import string
import typing

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
]

UnqueueCodeT = typing.Union[typing.Literal["PR_MERGED"], AbortCodeT]  # noqa: NU003


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
    message = "Pull request #{pr_number} has been dequeued{details}"  # noqa: FS003
    unqueue_code: typing.ClassVar[typing.Literal["PR_DEQUEUED"]] = "PR_DEQUEUED"
    pr_number: int
    details: str


@dataclasses.dataclass
class PrMerged(BaseUnqueueReason):
    message = "Pull request #{pr_number} has been merged automatically at *{sha}*"  # noqa: FS003
    unqueue_code: typing.ClassVar[typing.Literal["PR_MERGED"]] = "PR_MERGED"
    pr_number: int
    sha: github_types.SHAType


@dataclasses.dataclass
class PrAheadDequeued(BaseUnqueueReason):
    message = "Pull request #{pr_number} which was ahead in the queue has been dequeued"  # noqa: FS003
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_DEQUEUED"]
    ] = "PR_AHEAD_DEQUEUED"
    pr_number: int


@dataclasses.dataclass
class PrAheadFailedToMerge(BaseUnqueueReason):
    message = "Pull request ahead in queue failed to get merged"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_FAILED_TO_MERGE"]
    ] = "PR_AHEAD_FAILED_TO_MERGE"


@dataclasses.dataclass
class PrUnexpectedlyFailedToMerge(BaseUnqueueReason):
    message = "Pull request unexpectedly failed to get merged"
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_UNEXPECTEDLY_FAILED_TO_MERGE"]
    ] = "PR_UNEXPECTEDLY_FAILED_TO_MERGE"


# FIXME(sileht): should be something like PRQueuePriorityChanged
@dataclasses.dataclass
class PrWithHigherPriorityQueued(BaseUnqueueReason):
    message = (
        "Pull request #{pr_number} with higher priority has been queued"  # noqa: FS003
    )
    unqueue_code: typing.ClassVar[
        typing.Literal["PR_WITH_HIGHER_PRIORITY_QUEUED"]
    ] = "PR_WITH_HIGHER_PRIORITY_QUEUED"
    pr_number: int


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
    message = "The target branch does not exist anymore `{ref}`"  # noqa: FS003
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
    message = "Unexpected queue change: {change}"  # noqa: FS003
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
