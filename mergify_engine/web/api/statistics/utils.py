import collections
import statistics
import typing

from mergify_engine import context
from mergify_engine import date
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import types as web_stat_types


async def get_queue_checks_outcome_for_queue(
    repository_ctxt: context.Repository,
    partition_name: partr_config.PartitionRuleName | None,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> queue_statistics.QueueChecksOutcomeT:
    stats = await queue_statistics.get_queue_checks_outcome_stats(
        repository_ctxt,
        partition_name,
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )
    if queue_name not in stats:
        return queue_statistics.BASE_QUEUE_CHECKS_OUTCOME_T_DICT

    return stats[queue_name]


async def get_checks_duration_stats_for_all_queues(
    repository_ctxt: context.Repository,
    partition_name: partr_config.PartitionRuleName | None,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> dict[qr_config.QueueName, web_stat_types.ChecksDurationResponse]:
    """
    Returns a dict containing a web_stat_types.ChecksDurationResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        partition_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )
    stats_out: dict[qr_config.QueueName, web_stat_types.ChecksDurationResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = web_stat_types.ChecksDurationResponse(
                mean=None, median=None
            )
        else:
            stats_out[queue_name] = web_stat_types.ChecksDurationResponse(
                mean=statistics.fmean(stats_list),
                median=statistics.median(stats_list),
            )

    return stats_out


async def get_checks_duration_stats_for_queue(
    repository_ctxt: context.Repository,
    partition_name: partr_config.PartitionRuleName | None,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> web_stat_types.ChecksDurationResponse:
    stats = await queue_statistics.get_checks_duration_stats(
        repository_ctxt,
        partition_name,
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    )

    if qstats := stats.get(queue_name, []):
        return web_stat_types.ChecksDurationResponse(
            mean=statistics.fmean(qstats),
            median=statistics.median(qstats),
        )

    return web_stat_types.ChecksDurationResponse(mean=None, median=None)


async def get_time_to_merge_stats_for_queue(
    repository_ctxt: context.Repository,
    partition_name: partr_config.PartitionRuleName | None,
    queue_name: qr_config.QueueName,
    branch_name: str | None = None,
    at: int | None = None,
) -> web_stat_types.TimeToMergeResponse:
    stats = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        partition_name,
        queue_name=queue_name,
        branch_name=branch_name,
        at=at,
    )
    if qstats := stats.get(queue_name, []):
        return web_stat_types.TimeToMergeResponse(
            mean=statistics.fmean(qstats), median=statistics.median(qstats)
        )

    return web_stat_types.TimeToMergeResponse(mean=None, median=None)


async def get_time_to_merge_stats_for_all_queues(
    repository_ctxt: context.Repository,
    partition_name: partr_config.PartitionRuleName | None,
    branch_name: str | None = None,
    at: int | None = None,
) -> dict[qr_config.QueueName, web_stat_types.TimeToMergeResponse]:
    """
    Returns a dict containing a web_stat_types.TimeToMergeResponse for each queue.
    If a queue is not in the returned dict, that means there are no available data
    for this queue.
    """
    stats_dict = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        partition_name,
        branch_name=branch_name,
        at=at,
    )
    stats_out: dict[qr_config.QueueName, web_stat_types.TimeToMergeResponse] = {}
    for queue_name, stats_list in stats_dict.items():
        if len(stats_list) == 0:
            stats_out[queue_name] = web_stat_types.TimeToMergeResponse(
                mean=None, median=None
            )
        else:
            stats_out[queue_name] = web_stat_types.TimeToMergeResponse(
                mean=statistics.fmean(stats_list),
                median=statistics.median(stats_list),
            )

    return stats_out


def is_timestamp_in_future(timestamp: int) -> bool:
    return timestamp > int(date.utcnow().timestamp())


class TimestampNotInFuture(int):
    @classmethod
    def __get_validators__(cls) -> collections.abc.Generator[typing.Any, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: str) -> int:
        if is_timestamp_in_future(int(v)):
            raise ValueError("Timestamp cannot be in the future")

        return int(v)
