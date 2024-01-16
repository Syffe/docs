import fastapi

from mergify_engine import database
from mergify_engine.rules.config import partition_rules as partr_config
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
    repository_ctxt: security.RepositoryWithConfig,
    queue_name: security.QueueNameFromPath,
    timerange: security.TimeRange,
    branch: security.OptionalBranchFromQuery = None,
) -> web_stat_types.ChecksDurationResponse:
    if len(repository_ctxt.mergify_config["partition_rules"]):
        raise fastapi.HTTPException(
            status_code=400,
            detail="This endpoint cannot be called for this repository because it uses partition rules.",
        )
    return await web_stat_utils.get_queue_checks_duration(
        session=session,
        repository_id=repository_ctxt.repo["id"],
        queue_names=(queue_name,),
        partition_names=(partr_config.DEFAULT_PARTITION_NAME,),
        timerange=timerange,
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
    repository_ctxt: security.RepositoryWithConfig,
    partition_name: security.PartitionNameFromPath,
    queue_name: security.QueueNameFromPath,
    timerange: security.TimeRange,
    branch: security.OptionalBranchFromQuery = None,
) -> web_stat_types.ChecksDurationResponse:
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in repository_ctxt.mergify_config["partition_rules"]
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )
    return await web_stat_utils.get_queue_checks_duration(
        session=session,
        repository_id=repository_ctxt.repo["id"],
        queue_names=(queue_name,),
        partition_names=(partition_name,),
        timerange=timerange,
        branch=branch,
    )
