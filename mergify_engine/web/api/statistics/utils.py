import datetime
import statistics

import sqlalchemy

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import utils
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import types as web_stat_types


# The maximum time in the past we allow users to query
QUERY_MERGE_QUEUE_STATS_RETENTION: datetime.timedelta = datetime.timedelta(days=30)


def get_oldest_datetime() -> datetime.datetime:
    return date.utcnow() - QUERY_MERGE_QUEUE_STATS_RETENTION


async def get_queue_checks_end_events(
    session: database.Session,
    repository_id: github_types.GitHubRepositoryIdType,
    queue_names: tuple[qr_config.QueueName, ...],
    partition_names: tuple[partr_config.PartitionRuleName, ...],
    timerange: utils.TimeRange,
    branch: str | None = None,
) -> sqlalchemy.ScalarResult[evt_models.EventActionQueueChecksEnd]:
    model = evt_models.EventActionQueueChecksEnd

    query_filter = {
        model.repository_id == repository_id,
        model.type == enumerations.EventType.ActionQueueChecksEnd,
        model.aborted.is_(False),
        model.received_at >= get_oldest_datetime(),
        model.received_at >= timerange.start_at,
        model.received_at <= timerange.end_at,
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
    timerange: utils.TimeRange | None = None,
    branch: str | None = None,
) -> web_stat_types.ChecksDurationResponse:
    if timerange is None:
        end_at = date.utcnow()
        timerange = utils.TimeRange(
            end_at - QUERY_MERGE_QUEUE_STATS_RETENTION,
            end_at,
        )

    events = await get_queue_checks_end_events(
        session=session,
        repository_id=repository_id,
        queue_names=queue_names,
        partition_names=partition_names,
        timerange=timerange,
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
    end_at = date.utcnow()
    timerange = utils.TimeRange(end_at - datetime.timedelta(days=7), end_at)

    events = await get_queue_checks_end_events(
        session,
        repository_id,
        queue_names,
        partition_names,
        timerange,
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
