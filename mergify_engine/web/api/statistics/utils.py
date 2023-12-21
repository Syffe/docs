import datetime
import math
import statistics
import typing

import fastapi
import pydantic
import pydantic_core
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.dialects import postgresql

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
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


class DatetimeNotInFuture(datetime.datetime):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        from_dt_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.datetime_schema(),
                pydantic_core.core_schema.no_info_plain_validator_function(
                    cls.validate,
                ),
            ],
        )

        return pydantic_core.core_schema.json_or_python_schema(
            json_schema=from_dt_schema,
            python_schema=from_dt_schema,
        )

    @classmethod
    def validate(cls, v: datetime.datetime) -> datetime.datetime:
        if is_timestamp_in_future(int(v.timestamp())):
            raise ValueError("Datetime cannot be in the future")

        return v


async def get_queue_checks_end_events(
    session: database.Session,
    repository_id: github_types.GitHubRepositoryIdType,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    start_at: TimestampNotInFuture,
    end_at: TimestampNotInFuture,
    branch: str | None = None,
) -> sqlalchemy.ScalarResult[evt_models.EventActionQueueChecksEnd]:
    model = evt_models.EventActionQueueChecksEnd

    query_filter = {
        model.repository_id == repository_id,
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
    repository_id: github_types.GitHubRepositoryIdType,
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
        repository_id=repository_id,
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
    repository_id: github_types.GitHubRepositoryIdType,
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
        repository_id,
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


def get_interval(
    duration: datetime.timedelta,
) -> tuple[sqlalchemy.sql.functions.Function[postgresql.INTERVAL], ...]:
    # aggregation interval depending on the wanted duration = end_at - start_at
    if duration <= datetime.timedelta(days=2):
        # 1 value per hour (max 48 points)
        return func.make_interval(0, 0, 0, 0, 1), func.make_interval(
            0,
            0,
            0,
            0,
            0,
            59,
            59,
        )
    if duration <= datetime.timedelta(days=7):
        # 1 value per 4 hours (max 42 points)
        return func.make_interval(0, 0, 0, 0, 4), func.make_interval(
            0,
            0,
            0,
            0,
            3,
            59,
            59,
        )
    if duration <= datetime.timedelta(days=60):
        # 1 value per day (max 60 points)
        return func.make_interval(0, 0, 0, 1), func.make_interval(
            0,
            0,
            0,
            0,
            23,
            59,
            59,
        )
    # 1 value per 2 days (max 45 points)
    return func.make_interval(0, 0, 0, 2), func.make_interval(
        0,
        0,
        0,
        1,
        23,
        59,
        59,
    )


StartAt = typing.Annotated[
    DatetimeNotInFuture | None,
    fastapi.Query(description="Get the stats from this date"),
]

EndAt = typing.Annotated[
    DatetimeNotInFuture | None,
    fastapi.Query(description="Get the stats until this date"),
]


def verify_start_end(
    start_at: StartAt = None,
    end_at: EndAt = None,
) -> tuple[datetime.datetime, datetime.datetime]:
    end_at_ = date.utcnow() if end_at is None else end_at
    start_at_ = end_at_ - datetime.timedelta(days=1) if start_at is None else start_at

    if end_at_ < start_at_:
        raise fastapi.HTTPException(
            status_code=422,
            detail="provided end_at should be after start_at",
        )

    return start_at_, end_at_


StatsStartEnd = typing.Annotated[
    tuple[StartAt, EndAt],
    fastapi.Depends(verify_start_end),
]
