import dataclasses
import typing

import fastapi
import pydantic
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import date
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_models
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


router = fastapi.APIRouter()


async def get_queue_checks_end_events(
    session: database.Session,
    repository_ctxt: security.Repository,
    partition_rules: security.PartitionRules,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    start_at: web_stat_utils.TimestampNotInFuture | None = None,
    end_at: web_stat_utils.TimestampNotInFuture | None = None,
    branch: str | None = None,
) -> sqlalchemy.ScalarResult[evt_models.EventActionQueueChecksEnd]:
    model = evt_models.EventActionQueueChecksEnd

    query_filter = {
        model.repository_id == repository_ctxt.repo["id"],
        model.type == enumerations.EventType.ActionQueueChecksEnd,
        model.received_at >= web_stat_utils.get_oldest_datetime(),
    }
    if partition_names:
        query_filter.add(model.partition_name.in_(partition_names))
    if queue_names:
        query_filter.add(model.queue_name.in_(queue_names))
    if branch is not None:
        query_filter.add(model.branch == branch)
    if start_at is not None:
        query_filter.add(model.received_at >= date.fromtimestamp(start_at))
    if end_at is not None:
        query_filter.add(model.received_at <= date.fromtimestamp(end_at))

    stmt = sqlalchemy.select(model).where(*query_filter).order_by(model.id.desc())
    return await session.scalars(stmt)


async def get_queue_checks_outcome(
    session: database.Session,
    repository_ctxt: security.Repository,
    partition_rules: security.PartitionRules,
    partition_name: partr_config.PartitionRuleName,
    queue_name: qr_config.QueueName,
    start_at: web_stat_utils.TimestampNotInFuture | None = None,
    end_at: web_stat_utils.TimestampNotInFuture | None = None,
    branch: str | None = None,
) -> web_stat_types.QueueChecksOutcome:
    events = await get_queue_checks_end_events(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        partition_names=(partition_name,),
        queue_names=(queue_name,),
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )

    stats = queue_statistics.BASE_QUEUE_CHECKS_OUTCOME_T_DICT.copy()
    for event in events.all():
        if event.abort_code is None:
            stats["SUCCESS"] += 1
        else:
            stats[event.abort_code.value] += 1
    return web_stat_types.QueueChecksOutcome(**stats)


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
    session: database.Session,
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
    queue_names = tuple(rule.name for rule in queue_rules)
    if partition_rules:
        partition_names = tuple(rule.name for rule in partition_rules)
    else:
        partition_names = (partr_config.DEFAULT_PARTITION_NAME,)

    events = await get_queue_checks_end_events(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        partition_names=partition_names,
        queue_names=queue_names,
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )

    data: dict[
        partr_config.PartitionRuleName,
        dict[qr_config.QueueName, queue_statistics.QueueChecksOutcomeT],
    ] = {}

    for partition_name in partition_names:
        data[partition_name] = {}
        for queue in queue_names:
            data[partition_name][
                queue
            ] = queue_statistics.BASE_QUEUE_CHECKS_OUTCOME_T_DICT.copy()

    for event in events.all():
        partition_name = event.partition_name or partr_config.DEFAULT_PARTITION_NAME
        queue_name = qr_config.QueueName(event.queue_name)
        if event.abort_code is None:
            data[partition_name][queue_name]["SUCCESS"] += 1
        else:
            data[partition_name][queue_name][event.abort_code.value] += 1

    result: list[QueueChecksOutcomePerPartition] = []
    for partition_name, queues_data in data.items():
        result.append(
            QueueChecksOutcomePerPartition(
                partition_name=partition_name,
                queues=[
                    QueueChecksOutcomePerQueue(
                        queue_name=queue_name,
                        queue_checks_outcome=web_stat_types.QueueChecksOutcome(**stats),
                    )
                    for queue_name, stats in queues_data.items()
                ],
            )
        )

    return result


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for queue name",
    description="Get the queue checks outcome statistics for the specified queue name, only if partitions are not used in this repository",
    response_model=web_stat_types.QueueChecksOutcome,
)
async def get_queue_checks_outcome_stats_all_partitions_endpoint(
    session: database.Session,
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

    return await get_queue_checks_outcome(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        partition_name=partr_config.DEFAULT_PARTITION_NAME,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )


@router.get(
    "/repos/{owner}/{repository}/partitions/{partition_name}/queues/{queue_name}/stats/queue_checks_outcome",
    summary="Queue checks outcome statistics for queue name of partition",
    description="Get the queue checks outcome statistics for the specified queue name of the specified partition",
    response_model=web_stat_types.QueueChecksOutcome,
)
async def get_queue_checks_outcome_stats_partition_endpoint(
    session: database.Session,
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

    return await get_queue_checks_outcome(
        session=session,
        repository_ctxt=repository_ctxt,
        partition_rules=partition_rules,
        partition_name=partition_name,
        queue_name=queue_name,
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )
