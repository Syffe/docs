import collections
import dataclasses
import statistics
import typing

import fastapi
import pydantic

from mergify_engine import context
from mergify_engine import date
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


def is_timestamp_in_future(timestamp: int) -> bool:
    return timestamp > int(date.utcnow().timestamp())


class TimestampNotInFuture(int):
    @classmethod
    def __get_validators__(cls) -> collections.abc.Generator[typing.Any, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: str) -> int:
        if is_timestamp_in_future(int(v)):
            raise ValueError("Timestamp cannot be in the future")

        return int(v)


class TimeToMergeResponse(typing.TypedDict):
    mean: float | None
    median: float | None


async def get_time_to_merge_stats_for_queue(
    repository_ctxt: context.Repository,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    at: int | None = None,
) -> TimeToMergeResponse:
    stats = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        queue_name=queue_name,
        branch_name=branch_name,
        at=at,
    )
    if qstats := stats.get(queue_name, []):
        return TimeToMergeResponse(
            mean=statistics.fmean(qstats), median=statistics.median(qstats)
        )

    return TimeToMergeResponse(mean=None, median=None)


async def get_time_to_merge_stats_for_all_queues(
    repository_ctxt: context.Repository,
    branch_name: str | None = None,
    at: int | None = None,
) -> dict[qr_config.QueueName, TimeToMergeResponse]:
    """
    Returns a dict containing a TimeToMergeResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        branch_name=branch_name,
        at=at,
    )
    stats_out: dict[qr_config.QueueName, TimeToMergeResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = TimeToMergeResponse(mean=None, median=None)
        else:
            stats_out[queue_name] = TimeToMergeResponse(
                mean=statistics.fmean(stats_list),
                median=statistics.median(stats_list),
            )

    return stats_out


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",  # noqa: FS003
    summary="Get the time to merge statistics for the specified queue name",
    description="Get the average time to merge statistics, in seconds, for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_endpoint(
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(
            description="Name of the queue",
        ),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    at: TimestampNotInFuture
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the average time to merge for the queue at this timestamp (in seconds)",
    ),
    branch: str
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="The name of the branch on which we want the statistics",
    ),
) -> TimeToMergeResponse:
    try:
        return await get_time_to_merge_stats_for_queue(
            repository_ctxt,
            queue_name=queue_name,
            branch_name=branch,
            at=at,
        )
    except queue_statistics.TimestampTooFar:
        raise fastapi.HTTPException(
            status_code=400,
            detail=(
                f"The provided 'at' timestamp ({at}) is too far in the past. "
                "You can only query a maximum of "
                f"{queue_statistics.QUERY_MERGE_QUEUE_STATS_RETENTION.days} days in the past."
            ),
        )


class ChecksDurationResponse(typing.TypedDict):
    mean: float | None
    median: float | None


async def get_checks_duration_stats_for_queue(
    repository_ctxt: context.Repository,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> ChecksDurationResponse:
    stats = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )

    if qstats := stats.get(queue_name, []):
        return ChecksDurationResponse(
            mean=statistics.fmean(qstats),
            median=statistics.median(qstats),
        )

    return ChecksDurationResponse(mean=None, median=None)


async def get_checks_duration_stats_for_all_queues(
    repository_ctxt: context.Repository,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> dict[qr_config.QueueName, ChecksDurationResponse]:
    """
    Returns a dict containing a ChecksDurationResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )
    stats_out: dict[qr_config.QueueName, ChecksDurationResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = ChecksDurationResponse(mean=None, median=None)
        else:
            stats_out[queue_name] = ChecksDurationResponse(
                mean=statistics.fmean(stats_list),
                median=statistics.median(stats_list),
            )

    return stats_out


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/checks_duration",  # noqa: FS003
    summary="Get the average checks duration statistics, in seconds, for the specified queue name",
    description="Get the average checks duration statistics, in seconds, for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=ChecksDurationResponse,
)
async def get_checks_duration_stats_endpoint(
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(description="Name of the queue"),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    start_at: TimestampNotInFuture
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp (in seconds)",
    ),
    end_at: TimestampNotInFuture
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp (in seconds)",
    ),
    branch: str
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="The name of the branch on which we want the statistics",
    ),
) -> ChecksDurationResponse:
    return await get_checks_duration_stats_for_queue(
        repository_ctxt,
        queue_name=queue_name,
        branch_name=branch,
        start_at=start_at,
        end_at=end_at,
    )


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


async def get_queue_checks_outcome_for_queue(
    repository_ctxt: context.Repository,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> queue_statistics.QueueChecksOutcomeT:
    stats = await queue_statistics.get_queue_checks_outcome_stats(
        repository_ctxt,
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )
    if queue_name not in stats:
        return queue_statistics.BASE_QUEUE_CHECKS_OUTCOME_T_DICT

    return stats[queue_name]


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/queue_checks_outcome",  # noqa: FS003
    summary="Get the queue checks outcome statistics for the specified queue name",
    description="Get the queue checks outcome statistics for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=QueueChecksOutcome,
)
async def get_queue_checks_outcome_stats_endpoint(
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(
            description="Name of the queue",
        ),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    start_at: TimestampNotInFuture
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp (in seconds)",
    ),
    end_at: TimestampNotInFuture
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp (in seconds)",
    ),
    branch: str
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="The name of the branch on which we want the statistics",
    ),
) -> QueueChecksOutcome:
    return QueueChecksOutcome(
        **(
            await get_queue_checks_outcome_for_queue(
                repository_ctxt,
                queue_name=queue_name,
                branch_name=branch,
                start_at=start_at,
                end_at=end_at,
            )
        )
    )
