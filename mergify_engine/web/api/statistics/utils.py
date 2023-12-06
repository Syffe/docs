import datetime
import math
import statistics
import typing

import pydantic
import pydantic_core
import sqlalchemy

from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import types as web_stat_types


# The maximum time in the past we allow users to query
QUERY_MERGE_QUEUE_STATS_RETENTION: datetime.timedelta = datetime.timedelta(days=30)


def get_oldest_datetime() -> datetime.datetime:
    return date.utcnow() - QUERY_MERGE_QUEUE_STATS_RETENTION


def is_timestamp_in_future(timestamp: int) -> bool:
    return timestamp > int(date.utcnow().timestamp())


class TimestampNotInFuture(int):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        from_int_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.int_schema(),
                pydantic_core.core_schema.no_info_plain_validator_function(
                    cls.validate,
                ),
            ],
        )

        return pydantic_core.core_schema.json_or_python_schema(
            json_schema=from_int_schema,
            python_schema=from_int_schema,
        )

    @classmethod
    def validate(cls, v: str) -> int:
        if is_timestamp_in_future(int(v)):
            raise ValueError("Timestamp cannot be in the future")

        return int(v)


async def get_queue_checks_end_events(
    session: database.Session,
    repository_ctxt: context.Repository,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    start_at: TimestampNotInFuture,
    end_at: TimestampNotInFuture,
    branch: str | None = None,
) -> sqlalchemy.ScalarResult[evt_models.EventActionQueueChecksEnd]:
    model = evt_models.EventActionQueueChecksEnd

    query_filter = {
        model.repository_id == repository_ctxt.repo["id"],
        model.type == enumerations.EventType.ActionQueueChecksEnd,
        model.aborted.is_(False),
        model.received_at >= get_oldest_datetime(),
        model.received_at >= date.fromtimestamp(start_at),
        model.received_at <= date.fromtimestamp(end_at),
    }
    if partition_names:
        query_filter.add(model.partition_name.in_(partition_names))
    if queue_names:
        query_filter.add(model.queue_name.in_(queue_names))
    if branch is not None:
        query_filter.add(model.branch == branch)

    stmt = sqlalchemy.select(model).where(*query_filter).order_by(model.id.asc())
    return await session.scalars(stmt)


async def get_queue_checks_duration(
    session: database.Session,
    repository_ctxt: context.Repository,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    start_at: TimestampNotInFuture | None = None,
    end_at: TimestampNotInFuture | None = None,
    branch: str | None = None,
) -> web_stat_types.ChecksDurationResponse:
    if end_at is None:
        end_at = TimestampNotInFuture(
            math.ceil(date.utcnow().timestamp()),
        )

    if start_at is None:
        start_at = TimestampNotInFuture(
            end_at - datetime.timedelta(days=1).total_seconds(),
        )

    events = await get_queue_checks_end_events(
        session=session,
        repository_ctxt=repository_ctxt,
        queue_names=queue_names,
        partition_names=partition_names,
        start_at=start_at,
        end_at=end_at,
        branch=branch,
    )

    qstats = []
    for event in events.all():
        if not (
            event.speculative_check_pull_request.checks_ended_at
            and event.speculative_check_pull_request.checks_started_at
        ):
            continue
        qstats.append(
            (
                event.speculative_check_pull_request.checks_ended_at
                - event.speculative_check_pull_request.checks_started_at
            ).total_seconds(),
        )

    if qstats:
        return web_stat_types.ChecksDurationResponse(
            mean=statistics.fmean(qstats),
            median=statistics.median(qstats),
        )
    return web_stat_types.ChecksDurationResponse(mean=None, median=None)


QueueChecksDurationsPerPartitionQueueBranchT = dict[
    str,
    dict[str, dict[str, list[float]]],
]


async def get_queue_check_durations_per_partition_queue_branch(
    session: database.Session,
    repository_ctxt: context.Repository,
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    queue_names: tuple[qr_config.QueueName, ...],
) -> QueueChecksDurationsPerPartitionQueueBranchT:
    # Only compute it on the last 7 days
    end_at = TimestampNotInFuture(
        math.ceil(date.utcnow().timestamp()),
    )
    start_at = TimestampNotInFuture(
        end_at - datetime.timedelta(days=7).total_seconds(),
    )

    events = await get_queue_checks_end_events(
        session,
        repository_ctxt,
        queue_names,
        partition_names,
        start_at,
        end_at,
    )

    stats: QueueChecksDurationsPerPartitionQueueBranchT = {}
    for event in events.all():
        if not (
            event.speculative_check_pull_request.checks_ended_at
            and event.speculative_check_pull_request.checks_started_at
        ):
            continue

        partition_name = event.partition_name or partr_config.DEFAULT_PARTITION_NAME
        if partition_name not in stats:
            stats[partition_name] = {}

        if event.queue_name not in stats[partition_name]:
            stats[partition_name][event.queue_name] = {}

        if event.branch not in stats[partition_name][event.queue_name]:
            stats[partition_name][event.queue_name][event.branch] = []

        stats[partition_name][event.queue_name][event.branch].append(
            (
                event.speculative_check_pull_request.checks_ended_at
                - event.speculative_check_pull_request.checks_started_at
            ).total_seconds(),
        )
    return stats
