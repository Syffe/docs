import dataclasses
import datetime
import statistics
import typing

import fastapi
import pydantic
import sqlalchemy

from mergify_engine import context
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


async def get_queue_leave_events(
    session: database.Session,
    repository_ctxt: context.Repository,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    at: web_stat_utils.TimestampNotInFuture | None = None,
    branch: str | None = None,
) -> sqlalchemy.ScalarResult[evt_models.EventActionQueueLeave]:
    model = evt_models.EventActionQueueLeave

    query_filter = {
        model.repository_id == repository_ctxt.repo["id"],
        model.type == enumerations.EventType.ActionQueueLeave,
        model.merged.is_(True),
    }
    if partition_names:
        query_filter.add(model.partition_name.in_(partition_names))
    if queue_names:
        query_filter.add(model.queue_name.in_(queue_names))
    if branch is not None:
        query_filter.add(model.branch == branch)

    oldest_dt = web_stat_utils.get_oldest_datetime()
    if at is None:
        query_filter.add(model.received_at >= oldest_dt)
    else:
        at_dt = date.fromtimestamp(at) if at is not None else None
        if at_dt is not None and at_dt < oldest_dt:
            raise fastapi.HTTPException(
                status_code=400,
                detail=(
                    f"The provided 'at' timestamp ({at}) is too far in the past. "
                    "You can only query a maximum of "
                    f"{web_stat_utils.QUERY_MERGE_QUEUE_STATS_RETENTION.days} days in the past."
                ),
            )

        query_filter.add(
            model.received_at
            >= at_dt - web_stat_utils.QUERY_MERGE_QUEUE_STATS_RETENTION,
        )
        query_filter.add(model.received_at <= at_dt)

    stmt = sqlalchemy.select(model).where(*query_filter).order_by(model.id.asc())
    return await session.scalars(stmt)


def get_time_to_merge_from_event(event: evt_models.EventActionQueueLeave) -> float:
    return (
        (event.received_at - event.queued_at)
        - datetime.timedelta(seconds=event.seconds_waiting_for_freeze)
        - datetime.timedelta(seconds=event.seconds_waiting_for_schedule)
    ).total_seconds()


async def get_time_to_merge_from_partitions_and_queues(
    session: database.Session,
    repository_ctxt: context.Repository,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    at: web_stat_utils.TimestampNotInFuture | None = None,
    branch: str | None = None,
) -> web_stat_types.TimeToMergeResponse:
    events = await get_queue_leave_events(
        session=session,
        repository_ctxt=repository_ctxt,
        queue_names=queue_names,
        partition_names=partition_names,
        at=at,
        branch=branch,
    )

    qstats: list[float] = []
    for event in events.all():
        qstats.append(get_time_to_merge_from_event(event))

    if qstats:
        return web_stat_types.TimeToMergeResponse(
            mean=statistics.fmean(qstats),
            median=statistics.median(qstats),
        )

    return web_stat_types.TimeToMergeResponse(mean=None, median=None)


@pydantic.dataclasses.dataclass
class TimeToMergePerQueue:
    queue_name: qr_config.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue"},
    )

    time_to_merge: web_stat_types.TimeToMergeResponse = dataclasses.field(
        metadata={"description": "The time to merge data for the partition's queue"},
    )


@pydantic.dataclasses.dataclass
class TimeToMergePerPartition:
    partition_name: partr_config.PartitionRuleName = dataclasses.field(
        metadata={
            "description": f"The name of the partition, if no partition are used the partition name will be `{partr_config.DEFAULT_PARTITION_NAME}`",
        },
    )

    queues: list[TimeToMergePerQueue] = dataclasses.field(
        metadata={
            "description": "The time to merge data for each queue in the current partition",
        },
    )


@router.get(
    "/repos/{owner}/{repository}/stats/time_to_merge",
    summary="Time to merge statistics for every queues and partitions",
    description="Get the average time to merge statistics, in seconds, for all the queues and partitions in the repository",
    response_model=list[TimeToMergePerPartition],
)
async def get_time_to_merge_stats_for_all_queues_and_partitions_endpoint(
    session: database.Session,
    repository_ctxt: security.RepositoryWithConfig,
    at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the time to merge at this timestamp (in seconds)",
        ),
    ] = None,
    branch: security.OptionalBranchFromQuery = None,
) -> list[TimeToMergePerPartition]:
    queue_rules = repository_ctxt.mergify_config["queue_rules"]
    partition_rules = repository_ctxt.mergify_config["partition_rules"]

    queue_names = tuple(rule.name for rule in queue_rules)
    if partition_rules:
        partition_names = tuple(rule.name for rule in partition_rules)
    else:
        partition_names = (partr_config.DEFAULT_PARTITION_NAME,)

    events = await get_queue_leave_events(
        session=session,
        repository_ctxt=repository_ctxt,
        queue_names=queue_names,
        partition_names=partition_names,
        at=at,
        branch=branch,
    )

    data: dict[str, dict[str, list[float]]] = {}
    for event in events.all():
        if event.partition_name is None:
            raise RuntimeError(
                "It's not possible anymore, but please mypy until we drop eventlogs in Redis code",
            )

        if event.partition_name not in data:
            data[event.partition_name] = {}
        if event.queue_name not in data[event.partition_name]:
            data[event.partition_name][event.queue_name] = []
        data[event.partition_name][event.queue_name].append(
            get_time_to_merge_from_event(event),
        )

    result: list[TimeToMergePerPartition] = []
    for part_name in partition_names:
        queues: list[TimeToMergePerQueue] = []
        ttm_queues = data.get(part_name, {})
        for queue_name in queue_names:
            if queue_name in ttm_queues:
                ttm_data = web_stat_types.TimeToMergeResponse(
                    mean=statistics.fmean(ttm_queues[queue_name]),
                    median=statistics.median(ttm_queues[queue_name]),
                )
            else:
                ttm_data = web_stat_types.TimeToMergeResponse(
                    mean=None,
                    median=None,
                )

            queues.append(
                TimeToMergePerQueue(
                    queue_name=queue_name,
                    time_to_merge=ttm_data,
                ),
            )

        result.append(
            TimeToMergePerPartition(
                partition_name=part_name,
                queues=queues,
            ),
        )

    return result


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",
    summary="Time to merge statistics for queue name",
    description="Get the average time to merge statistics, in seconds, for the specified queue name only if partitions are not used in this repository",
    response_model=web_stat_types.TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_endpoint(
    session: database.Session,
    repository_ctxt: security.RepositoryWithConfig,
    queue_name: security.QueueNameFromPath,
    at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the average time to merge for the queue at this timestamp (in seconds)",
        ),
    ] = None,
    branch: security.OptionalBranchFromQuery = None,
) -> web_stat_types.TimeToMergeResponse:
    partition_rules = repository_ctxt.mergify_config["partition_rules"]
    if len(partition_rules):
        raise fastapi.HTTPException(
            status_code=400,
            detail="This endpoint cannot be called for this repository because it uses partition rules.",
        )

    return await get_time_to_merge_from_partitions_and_queues(
        session=session,
        repository_ctxt=repository_ctxt,
        queue_names=(queue_name,),
        partition_names=(partr_config.DEFAULT_PARTITION_NAME,),
        at=at,
        branch=branch,
    )


@router.get(
    "/repos/{owner}/{repository}/partitions/{partition_name}/queues/{queue_name}/stats/time_to_merge",
    summary="Time to merge statistics for queue name of partition",
    description="Get the average time to merge statistics, in seconds, for the specified queue name of the specified partition",
    response_model=web_stat_types.TimeToMergeResponse,
)
async def get_average_time_to_merge_stats_partition_endpoint(
    session: database.Session,
    repository_ctxt: security.RepositoryWithConfig,
    partition_name: security.PartitionNameFromPath,
    queue_name: security.QueueNameFromPath,
    at: typing.Annotated[
        web_stat_utils.TimestampNotInFuture | None,
        fastapi.Query(
            description="Retrieve the average time to merge for the queue at this timestamp (in seconds)",
        ),
    ] = None,
    branch: security.OptionalBranchFromQuery = None,
) -> web_stat_types.TimeToMergeResponse:
    partition_rules = repository_ctxt.mergify_config["partition_rules"]
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )

    return await get_time_to_merge_from_partitions_and_queues(
        session=session,
        repository_ctxt=repository_ctxt,
        queue_names=(queue_name,),
        partition_names=(partition_name,),
        at=at,
        branch=branch,
    )
