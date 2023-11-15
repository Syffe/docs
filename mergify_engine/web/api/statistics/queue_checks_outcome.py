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
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


router = fastapi.APIRouter()


# Every key is an `abort_code` of a class that inherits from `BaseAbortReason`
# in mergify_engine/queue/utils.py
class QueueChecksOutcomeT(typing.TypedDict):
    PR_DEQUEUED: int
    PR_AHEAD_DEQUEUED: int
    PR_AHEAD_FAILED_TO_MERGE: int
    PR_WITH_HIGHER_PRIORITY_QUEUED: int
    PR_QUEUED_TWICE: int
    SPECULATIVE_CHECK_NUMBER_REDUCED: int
    CHECKS_TIMEOUT: int
    CHECKS_FAILED: int
    QUEUE_RULE_MISSING: int
    UNEXPECTED_QUEUE_CHANGE: int
    PR_FROZEN_NO_CASCADING: int
    TARGET_BRANCH_CHANGED: int
    TARGET_BRANCH_MISSING: int
    PR_UNEXPECTEDLY_FAILED_TO_MERGE: int
    BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS: int
    PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE: int
    CONFLICT_WITH_BASE_BRANCH: int
    CONFLICT_WITH_PULL_AHEAD: int
    BRANCH_UPDATE_FAILED: int
    SUCCESS: int


BASE_QUEUE_CHECKS_OUTCOME_T_DICT: QueueChecksOutcomeT = QueueChecksOutcomeT(
    {
        "PR_DEQUEUED": 0,
        "PR_AHEAD_DEQUEUED": 0,
        "PR_AHEAD_FAILED_TO_MERGE": 0,
        "PR_WITH_HIGHER_PRIORITY_QUEUED": 0,
        "PR_QUEUED_TWICE": 0,
        "SPECULATIVE_CHECK_NUMBER_REDUCED": 0,
        "CHECKS_TIMEOUT": 0,
        "CHECKS_FAILED": 0,
        "QUEUE_RULE_MISSING": 0,
        "UNEXPECTED_QUEUE_CHANGE": 0,
        "PR_FROZEN_NO_CASCADING": 0,
        "SUCCESS": 0,
        "TARGET_BRANCH_CHANGED": 0,
        "TARGET_BRANCH_MISSING": 0,
        "PR_UNEXPECTEDLY_FAILED_TO_MERGE": 0,
        "BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS": 0,
        "PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE": 0,
        "CONFLICT_WITH_BASE_BRANCH": 0,
        "CONFLICT_WITH_PULL_AHEAD": 0,
        "BRANCH_UPDATE_FAILED": 0,
    },
)


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

    stats = BASE_QUEUE_CHECKS_OUTCOME_T_DICT.copy()
    for event in events.all():
        if event.abort_code is None:
            stats["SUCCESS"] += 1
        else:
            stats[event.abort_code.value] += 1
    return web_stat_types.QueueChecksOutcome(**stats)


@pydantic.dataclasses.dataclass
class QueueChecksOutcomePerQueue:
    queue_name: qr_config.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue"},
    )

    queue_checks_outcome: web_stat_types.QueueChecksOutcome = dataclasses.field(
        metadata={"description": "The checks outcomes data for the partition's queue"},
    )


@pydantic.dataclasses.dataclass
class QueueChecksOutcomePerPartition:
    partition_name: partr_config.PartitionRuleName = dataclasses.field(
        metadata={
            "description": f"The name of the partition, if no partition are used the partition name will be `{partr_config.DEFAULT_PARTITION_NAME}`",
        },
    )

    queues: list[QueueChecksOutcomePerQueue] = dataclasses.field(
        metadata={
            "description": "The checks outcomes data for each queue in the current partition",
        },
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
    branch: security.OptionalBranchFromQuery = None,
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
        dict[qr_config.QueueName, QueueChecksOutcomeT],
    ] = {}

    for partition_name in partition_names:
        data[partition_name] = {}
        for queue in queue_names:
            data[partition_name][queue] = BASE_QUEUE_CHECKS_OUTCOME_T_DICT.copy()

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
            ),
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
    queue_name: security.QueueNameFromPath,
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
    branch: security.OptionalBranchFromQuery = None,
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
    partition_name: security.PartitionNameFromPath,
    queue_name: security.QueueNameFromPath,
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
    branch: security.OptionalBranchFromQuery = None,
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
