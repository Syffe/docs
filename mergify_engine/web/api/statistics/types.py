import dataclasses
import typing

import pydantic


class TimeToMergeResponse(typing.TypedDict):
    mean: float | None
    median: float | None


class ChecksDurationResponse(typing.TypedDict):
    mean: float | None
    median: float | None


@pydantic.dataclasses.dataclass
class QueueChecksOutcome:
    PR_AHEAD_DEQUEUED: int = dataclasses.field(
        metadata={"description": "A pull request ahead in the queue has been dequeued."}
    )

    PR_AHEAD_FAILED_TO_MERGE: int = dataclasses.field(
        metadata={
            "description": "A pull request ahead in the queue failed to get merged."
        }
    )

    PR_WITH_HIGHER_PRIORITY_QUEUED: int = dataclasses.field(
        metadata={
            "description": "A pull request with a higher priority has been queued."
        }
    )

    PR_QUEUED_TWICE: int = dataclasses.field(
        metadata={"description": "A pull request was queued twice."}
    )

    SPECULATIVE_CHECK_NUMBER_REDUCED: int = dataclasses.field(
        metadata={
            "description": "The number of speculative checks, in the queue rules, has been reduced."
        }
    )

    CHECKS_TIMEOUT: int = dataclasses.field(
        metadata={
            "description": "The checks for the queued pull request have timed out."
        }
    )

    CHECKS_FAILED: int = dataclasses.field(
        metadata={"description": "The checks for the queued pull request have failed."}
    )

    QUEUE_RULE_MISSING: int = dataclasses.field(
        metadata={
            "description": "The queue rules are missing because of a configuration change."
        }
    )

    UNEXPECTED_QUEUE_CHANGE: int = dataclasses.field(
        metadata={"description": "An unexpected change happened."}
    )

    PR_FROZEN_NO_CASCADING: int = dataclasses.field(
        metadata={
            "description": "A pull request has been freezed because of a queue freeze with cascading effect disabled."
        }
    )
    SUCCESS: int = dataclasses.field(
        metadata={"description": "Successfully merged the pull request."}
    )

    PR_DEQUEUED: int = dataclasses.field(
        metadata={"description": "Pull request has been dequeued"}
    )
    TARGET_BRANCH_CHANGED: int = dataclasses.field(
        metadata={"description": "The pull request target branch has changed"}
    )
    TARGET_BRANCH_MISSING: int = dataclasses.field(
        metadata={"description": "The target branch does not exist anymore"}
    )
    PR_UNEXPECTEDLY_FAILED_TO_MERGE: int = dataclasses.field(
        metadata={"description": "Pull request unexpectedly failed to get merged"}
    )
    BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS: int = dataclasses.field(
        metadata={
            "description": "The maximum batch failure resolution attempts has been reached"
        }
    )
