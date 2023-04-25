import typing

import fastapi

from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",
    summary="Time to merge statistics for queue name",
    description="Get the average time to merge statistics, in seconds, for the specified queue name only if partitions are not used in this repository",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=web_stat_types.TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_endpoint(
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(
            description="Name of the queue",
        ),
    ],
    at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the average time to merge for the queue at this timestamp (in seconds)",
        ),
    ] = None,
    branch: typing.Annotated[
        str | None,
        fastapi.Query(
            description="The name of the branch on which we want the statistics",
        ),
    ] = None,
) -> web_stat_types.TimeToMergeResponse:
    try:
        return await web_stat_utils.get_time_to_merge_stats_for_queue(
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
