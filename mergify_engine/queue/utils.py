import dataclasses
import typing

from mergify_engine import utils


AbortCodeT = typing.Literal[
    "PR_AHEAD_DEQUEUED",
    "PR_AHEAD_FAILED_TO_MERGE",
    "PR_WITH_HIGHER_PRIORITY_QUEUED",
    "PR_QUEUED_TWICE",
    "SPECULATIVE_CHECK_NUMBER_REDUCED",
    "CHECKS_TIMEOUT",
    "CHECKS_FAILED",
    "QUEUE_RULE_MISSING",
    "UNEXPECTED_QUEUE_CHANGE",
]


@dataclasses.dataclass
class BaseAbortReason:
    message: typing.ClassVar[str]
    abort_code: typing.ClassVar[AbortCodeT]

    def __str__(self) -> str:
        format_vars = {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in ("code", "message", "abort_code")
        }
        return self.message.format(**format_vars)


@dataclasses.dataclass
class PrAheadDequeued(BaseAbortReason):
    message = "Pull request #{pr_number} which was ahead in the queue has been dequeued"  # noqa: FS003
    abort_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_DEQUEUED"]
    ] = "PR_AHEAD_DEQUEUED"
    pr_number: int


@dataclasses.dataclass
class PrAheadFailedToMerge(BaseAbortReason):
    message = "Pull request ahead in queue failed to get merged"
    abort_code: typing.ClassVar[
        typing.Literal["PR_AHEAD_FAILED_TO_MERGE"]
    ] = "PR_AHEAD_FAILED_TO_MERGE"


@dataclasses.dataclass
class PrWithHigherPriorityQueued(BaseAbortReason):
    message = (
        "Pull request #{pr_number} with higher priority has been queued"  # noqa: FS003
    )
    abort_code: typing.ClassVar[
        typing.Literal["PR_WITH_HIGHER_PRIORITY_QUEUED"]
    ] = "PR_WITH_HIGHER_PRIORITY_QUEUED"
    pr_number: int


@dataclasses.dataclass
class PrQueuedTwice(BaseAbortReason):
    message = "The pull request has been queued twice"
    abort_code: typing.ClassVar[typing.Literal["PR_QUEUED_TWICE"]] = "PR_QUEUED_TWICE"


@dataclasses.dataclass
class SpeculativeCheckNumberReduced(BaseAbortReason):
    message = "The number of speculative checks has been reduced"
    abort_code: typing.ClassVar[
        typing.Literal["SPECULATIVE_CHECK_NUMBER_REDUCED"]
    ] = "SPECULATIVE_CHECK_NUMBER_REDUCED"


@dataclasses.dataclass
class ChecksTimeout(BaseAbortReason):
    message = "Checks have timed out"
    abort_code: typing.ClassVar[typing.Literal["CHECKS_TIMEOUT"]] = "CHECKS_TIMEOUT"


@dataclasses.dataclass
class ChecksFailed(BaseAbortReason):
    message = "Checks did not succeed"
    abort_code: typing.ClassVar[typing.Literal["CHECKS_FAILED"]] = "CHECKS_FAILED"


@dataclasses.dataclass
class QueueRuleMissing(BaseAbortReason):
    message = "The associated queue rule does not exist anymore"
    abort_code: typing.ClassVar[
        typing.Literal["QUEUE_RULE_MISSING"]
    ] = "QUEUE_RULE_MISSING"


@dataclasses.dataclass
class UnexpectedQueueChange(BaseAbortReason):
    message = "Unexpected queue change: {change}"  # noqa: FS003
    abort_code: typing.ClassVar[
        typing.Literal["UNEXPECTED_QUEUE_CHANGE"]
    ] = "UNEXPECTED_QUEUE_CHANGE"
    change: str


def is_pr_body_a_merge_queue_pr(pull_request_body: typing.Optional[str]) -> bool:
    if pull_request_body is None:
        return False

    payload = utils.get_hidden_payload_from_comment_body(pull_request_body)
    if payload is None:
        return False

    return payload.get("merge-queue-pr", False)
