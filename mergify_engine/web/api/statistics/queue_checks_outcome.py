import typing

import fastapi

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
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for queue name",
    description="Get the queue checks outcome statistics for the specified queue name, only if partitions are not used in this repository",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=web_stat_types.QueueChecksOutcome,
)
async def get_queue_checks_outcome_stats_all_partitions_endpoint(
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(
            description="Name of the queue",
        ),
    ],
    start_at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the stats that happened after this timestamp (in seconds)",
        ),
    ] = None,
    end_at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the stats that happened before this timestamp (in seconds)",
        ),
    ] = None,
    branch: typing.Annotated[
        str | None,
        fastapi.Query(
            description="The name of the branch on which we want the statistics",
        ),
    ] = None,
) -> web_stat_types.QueueChecksOutcome:
    return web_stat_types.QueueChecksOutcome(
        **(
            await web_stat_utils.get_queue_checks_outcome_for_queue(
                repository_ctxt,
                queue_name=queue_name,
                branch_name=branch,
                start_at=start_at,
                end_at=end_at,
            )
        )
    )
