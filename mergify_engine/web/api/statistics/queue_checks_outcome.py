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
class QueueChecksOutcomePerQueue:
    queue_name: qr_config.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue"}
    )

    queue_checks_outcome: web_stat_types.QueueChecksOutcome = dataclasses.field(
        metadata={"description": "The checks outcomes data for the partition's queue"}
    )


@pydantic.dataclasses.dataclass
class QueueChecksOutcomePerPartition:
    partition_name: partr_config.PartitionRuleName = dataclasses.field(
        metadata={
            "description": f"The name of the partition, if no partition are used the partition name will be `{partr_config.DEFAULT_PARTITION_NAME}`"
        }
    )

    queues: list[QueueChecksOutcomePerQueue] = dataclasses.field(
        metadata={
            "description": "The checks outcomes data for each queue in the current partition"
        }
    )


@router.get(
    "/repos/{owner}/{repository}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for every queues and partitions",
    description="Get the queue checks outcome statistics for all the queues and partitions in the repository",
    response_model=list[QueueChecksOutcomePerPartition],
)
async def get_queue_checks_outcome_stats_for_all_queues_and_partitions_endpoint(
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
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
) -> list[QueueChecksOutcomePerPartition]:
    if not partition_rules:
        partition_names = [partr_config.DEFAULT_PARTITION_NAME]
    else:
        partition_names = [rule.name for rule in partition_rules]

    queue_names = [rule.name for rule in queue_rules]

    data: list[QueueChecksOutcomePerPartition] = []
    for part_name in partition_names:
        ttm_queues = await web_stat_utils.get_queue_checks_outcome_for_all_queues(
            repository_ctxt,
            part_name,
            branch,
            start_at,
            end_at,
        )

        queues: list[QueueChecksOutcomePerQueue] = []
        for queue_name in queue_names:
            if queue_name in ttm_queues:
                qco_data = ttm_queues[queue_name]
            else:
                qco_data = queue_statistics.BASE_QUEUE_CHECKS_OUTCOME_T_DICT

            queues.append(
                QueueChecksOutcomePerQueue(
                    queue_name=queue_name,
                    queue_checks_outcome=web_stat_types.QueueChecksOutcome(**qco_data),
                )
            )

        data.append(
            QueueChecksOutcomePerPartition(
                partition_name=part_name,
                queues=queues,
            )
        )

    return data


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for queue name",
    description="Get the queue checks outcome statistics for the specified queue name, only if partitions are not used in this repository",
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
) -> web_stat_types.QueueChecksOutcome:
    if len(partition_rules):
        raise fastapi.HTTPException(
            status_code=400,
            detail="This endpoint cannot be called for this repository because it uses partition rules.",
        )

    return web_stat_types.QueueChecksOutcome(
        **(
            await web_stat_utils.get_queue_checks_outcome_for_queue(
                repository_ctxt,
                partr_config.DEFAULT_PARTITION_NAME,
                queue_name=queue_name,
                branch_name=branch,
                start_at=start_at,
                end_at=end_at,
            )
        )
    )


@router.get(
    "/repos/{owner}/{repository}/partitions/{partition_name}/queues/{queue_name}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for queue name of partition",
    description="Get the queue checks outcome statistics for the specified queue name of the specified partition",
    response_model=web_stat_types.QueueChecksOutcome,
)
async def get_queue_checks_outcome_stats_partition_endpoint(
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
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )

    return web_stat_types.QueueChecksOutcome(
        **(
            await web_stat_utils.get_queue_checks_outcome_for_queue(
                repository_ctxt,
                partition_name,
                queue_name=queue_name,
                branch_name=branch,
                start_at=start_at,
                end_at=end_at,
            )
        )
    )
