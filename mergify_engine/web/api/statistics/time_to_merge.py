import dataclasses
import typing

import fastapi
import pydantic

from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


router = fastapi.APIRouter()


@pydantic.dataclasses.dataclass
class TimeToMergePerQueue:
    queue_name: qr_config.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue"}
    )

    time_to_merge: web_stat_types.TimeToMergeResponse = dataclasses.field(
        metadata={"description": "The time to merge data for the partition's queue"}
    )


@pydantic.dataclasses.dataclass
class TimeToMergePerPartition:
    partition_name: partr_config.PartitionRuleName = dataclasses.field(
        metadata={
            "description": f"The name of the partition, if no partition are used the partition name will be `{partr_config.DEFAULT_PARTITION_NAME}`"
        }
    )

    queues: list[TimeToMergePerQueue] = dataclasses.field(
        metadata={
            "description": "The time to merge data for each queue in the current partition"
        }
    )


@router.get(
    "/repos/{owner}/{repository}/stats/time_to_merge",
    summary="Time to merge statistics for every queues and partitions",
    description="Get the average time to merge statistics, in seconds, for all the queues and partitions in the repository",
    response_model=list[TimeToMergePerPartition],
)
async def get_time_to_merge_stats_for_all_queues_and_partitions_endpoint(
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
    at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the time to merge at this timestamp (in seconds)",
        ),
    ] = None,
    branch: typing.Annotated[
        str | None,
        fastapi.Query(
            description="The name of the branch on which we want the statistics",
        ),
    ] = None,
) -> list[TimeToMergePerPartition]:
    if not len(partition_rules):
        partition_names = [partr_config.DEFAULT_PARTITION_NAME]
    else:
        partition_names = [rule.name for rule in partition_rules]

    queue_names = [rule.name for rule in queue_rules]

    data: list[TimeToMergePerPartition] = []
    for part_name in partition_names:
        ttm_queues = await web_stat_utils.get_time_to_merge_stats_for_all_queues(
            repository_ctxt,
            part_name,
            branch,
            at,
        )

        queues: list[TimeToMergePerQueue] = []
        for queue_name in queue_names:
            if queue_name in ttm_queues:
                ttm_data = ttm_queues[queue_name]
            else:
                ttm_data = web_stat_types.TimeToMergeResponse(
                    mean=None,
                    median=None,
                )

            queues.append(
                TimeToMergePerQueue(
                    queue_name=queue_name,
                    time_to_merge=ttm_data,
                )
            )

        data.append(
            TimeToMergePerPartition(
                partition_name=part_name,
                queues=queues,
            )
        )

    return data


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",
    summary="Time to merge statistics for queue name",
    description="Get the average time to merge statistics, in seconds, for the specified queue name only if partitions are not used in this repository",
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
    partition_rules: security.PartitionRules,
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
    if len(partition_rules):
        raise fastapi.HTTPException(
            status_code=400,
            detail="This endpoint cannot be called for this repository because it uses partition rules.",
        )

    try:
        return await web_stat_utils.get_time_to_merge_stats_for_queue(
            repository_ctxt,
            partr_config.DEFAULT_PARTITION_NAME,
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


@router.get(
    "/repos/{owner}/{repository}/partitions/{partition_name}/queues/{queue_name}/stats/time_to_merge",
    summary="Time to merge statistics for queue name of partition",
    description="Get the average time to merge statistics, in seconds, for the specified queue name of the specified partition",
    response_model=web_stat_types.TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_partition_endpoint(
    repository_ctxt: security.Repository,
    partition_name: typing.Annotated[
        partr_config.PartitionRuleName,
        fastapi.Path(description="The partition name"),
    ],
    queue_name: typing.Annotated[
        qr_config.QueueName,
        fastapi.Path(
            description="Name of the queue",
        ),
    ],
    partition_rules: security.PartitionRules,
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
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )

    try:
        return await web_stat_utils.get_time_to_merge_stats_for_queue(
            repository_ctxt,
            partition_name,
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
