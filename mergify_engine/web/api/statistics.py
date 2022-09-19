import statistics
import typing

import fastapi

from mergify_engine import context
from mergify_engine import rules
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Depends(security.require_authentication),
    ],
    include_in_schema=False,
)


class TimeToMergeResponse(typing.TypedDict):
    mean: float | None


async def get_time_to_merge_stats_for_queue(
    repository_ctxt: context.Repository,
    queue_name: rules.QueueName,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> TimeToMergeResponse:
    stats = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
    )
    if qstats := stats.get(queue_name, []):
        return TimeToMergeResponse(mean=statistics.fmean(qstats))

    return TimeToMergeResponse(mean=None)


async def get_time_to_merge_stats_for_all_queues(
    repository_ctxt: context.Repository,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> dict[str, TimeToMergeResponse]:
    """
    Returns a dict containing a TimeToMergeResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        start_at=start_at,
        end_at=end_at,
    )
    stats_out: dict[str, TimeToMergeResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = TimeToMergeResponse(mean=None)
        else:
            stats_out[queue_name] = TimeToMergeResponse(
                mean=statistics.fmean(stats_list)
            )

    return stats_out


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",  # noqa: FS003
    summary="Get the average time to merge statistics, in seconds, for the specified queue name",
    description="Get the average time to merge statistics, in seconds, for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_endpoint(
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the average time to merge after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the average time to merge before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> TimeToMergeResponse:
    return await get_time_to_merge_stats_for_queue(
        repository_ctxt, queue_name=queue_name, start_at=start_at, end_at=end_at
    )


class ChecksDurationResponse(typing.TypedDict):
    mean: float | None


async def get_checks_duration_stats_for_queue(
    repository_ctxt: context.Repository,
    queue_name: rules.QueueName,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> ChecksDurationResponse:
    stats = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
    )

    if qstats := stats.get(queue_name, []):
        return ChecksDurationResponse(mean=statistics.fmean(qstats))

    return ChecksDurationResponse(mean=None)


async def get_checks_duration_stats_for_all_queues(
    repository_ctxt: context.Repository,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> dict[str, ChecksDurationResponse]:
    """
    Returns a dict containing a ChecksDurationResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        start_at=start_at,
        end_at=end_at,
    )
    stats_out: dict[str, ChecksDurationResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = ChecksDurationResponse(mean=None)
        else:
            stats_out[queue_name] = ChecksDurationResponse(
                mean=statistics.fmean(stats_list)
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
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> ChecksDurationResponse:
    return await get_checks_duration_stats_for_queue(
        repository_ctxt,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
    )


async def get_failure_by_reason_stats_for_queue(
    repository_ctxt: context.Repository,
    queue_name: rules.QueueName,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> queue_statistics.FailureByReasonT:
    stats = await queue_statistics.get_failure_by_reason_stats(
        repository_ctxt,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
    )
    if queue_name not in stats:
        return queue_statistics.BASE_FAILURE_BY_REASON_T_DICT

    return stats[queue_name]


async def get_failure_by_reason_stats_for_all_queues(
    repository_ctxt: context.Repository,
    start_at: typing.Optional[int] = None,
    end_at: typing.Optional[int] = None,
) -> dict[str, queue_statistics.FailureByReasonT]:
    """
    Returns a dict containing a `queue_statistics.FailureByReasonT` for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    return await queue_statistics.get_failure_by_reason_stats(
        repository_ctxt,
        start_at=start_at,
        end_at=end_at,
    )


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/failure_by_reason",  # noqa: FS003
    summary="Get the failure by reason statistics for the specified queue name",
    description="Get the failure by reason statistics for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=queue_statistics.FailureByReasonT,
)
async def get_failure_by_reason_stats_endpoint(
    queue_name: rules.QueueName = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> queue_statistics.FailureByReasonT:
    return await get_failure_by_reason_stats_for_queue(
        repository_ctxt,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
    )
