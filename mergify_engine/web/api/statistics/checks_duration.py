import typing

import fastapi

from mergify_engine import database
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


router = fastapi.APIRouter()


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/checks_duration",
    summary="Average checks duration statistics for queue name",
    description="Get the average checks duration statistics, in seconds, for the specified queue name only if partitions are not used in this repository",
    response_model=web_stat_types.ChecksDurationResponse,
)
async def get_checks_duration_stats_endpoint(
    session: database.Session,
    repository_ctxt: security.Repository,
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(description="Name of the queue"),
    ],
    partition_rules: security.PartitionRules,
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
) -> web_stat_types.ChecksDurationResponse:
    if len(partition_rules):
        raise fastapi.HTTPException(
            status_code=400,
            detail="This endpoint cannot be called for this repository because it uses partition rules.",
        )
    return await web_stat_utils.get_queue_checks_duration(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        queue_names=(queue_name,),
        partition_names=(partr_config.DEFAULT_PARTITION_NAME,),
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )


@router.get(
    "/repos/{owner}/{repository}/partitions/{partition_name}/queues/{queue_name}/stats/checks_duration",
    summary="Average checks duration statistics for queue name of partition",
    description="Get the average checks duration statistics, in seconds, for the specified queue name of the specified partition",
    response_model=web_stat_types.ChecksDurationResponse,
)
async def get_checks_duration_stats_partition_endpoint(
    session: database.Session,
    repository_ctxt: security.Repository,
    partition_name: typing.Annotated[
        partr_config.PartitionRuleName,
        fastapi.Path(description="The partition name"),
    ],
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(description="Name of the queue"),
    ],
    partition_rules: security.PartitionRules,
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
) -> web_stat_types.ChecksDurationResponse:
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )
    return await web_stat_utils.get_queue_checks_duration(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        queue_names=(queue_name,),
        partition_names=(partition_name,),
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )
