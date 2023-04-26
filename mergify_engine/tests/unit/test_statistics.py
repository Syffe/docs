import datetime

from freezegun import freeze_time
import msgpack

from mergify_engine import context
from mergify_engine import date
from mergify_engine import redis_utils
from mergify_engine.queue import statistics as queue_stats
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import utils as web_stat_utils


def test_is_timestamp_in_future() -> None:
    with freeze_time("2022-10-14T10:00:00"):
        now_ts = int(date.utcnow().timestamp())
        assert not web_stat_utils.is_timestamp_in_future(now_ts)
        assert web_stat_utils.is_timestamp_in_future(now_ts + 1)
        assert not web_stat_utils.is_timestamp_in_future(now_ts - 1)


async def test_statistics_start_at_boundary(
    fake_repository: context.Repository,
    redis_stats: redis_utils.RedisStats,
) -> None:
    queue_name = qr_config.QueueName("default")
    branch_name = "main"
    partition_name = partr_config.PartitionRuleName("default")

    failure_by_reason_key = queue_stats.get_statistic_redis_key(
        fake_repository.installation.owner_id,
        fake_repository.repo["id"],
        "failure_by_reason",
    )
    pr_ahead_dequeued_stat = queue_stats.FailureByReason.from_reason_code_str(
        queue_name=queue_name,
        branch_name=branch_name,
        reason_code_str=queue_utils.PrAheadDequeued.unqueue_code,
        partition_name=partition_name,
    )
    pr_ahead_dequeued_fields = {
        b"version": queue_stats.VERSION,
        b"data": msgpack.packb(pr_ahead_dequeued_stat.to_dict(), datetime=True),
    }
    pipe = await redis_stats.pipeline()
    date_2days_back = date.utcnow() - datetime.timedelta(days=2)
    # Insert 5 PrAheadDequeued 2 days in the past
    for i in range(5):
        await pipe.xadd(
            failure_by_reason_key,
            id=int(
                (date_2days_back + datetime.timedelta(minutes=i)).timestamp() * 1000
            ),
            fields=pr_ahead_dequeued_fields,
        )

    checks_duration_key = queue_stats.get_statistic_redis_key(
        fake_repository.installation.owner_id,
        fake_repository.repo["id"],
        "checks_duration",
    )
    checks_duration_stat = queue_stats.ChecksDuration(
        queue_name=queue_name,
        branch_name=branch_name,
        duration_seconds=10,
        partition_name=partition_name,
    )
    checks_duration_fields = {
        b"version": queue_stats.VERSION,
        b"data": msgpack.packb(checks_duration_stat.to_dict(), datetime=True),
    }
    # Insert 3 ChecksDuration now
    for _ in range(3):
        await pipe.xadd(
            checks_duration_key,
            fields=checks_duration_fields,
        )

    await pipe.execute()

    queue_checks_outcome_2days = await queue_stats.get_queue_checks_outcome_stats(
        fake_repository,
        partition_name,
        queue_name,
        branch_name,
        start_at=int(date_2days_back.timestamp()),
    )
    assert queue_checks_outcome_2days[queue_name]["PR_AHEAD_DEQUEUED"] == 5
    assert queue_checks_outcome_2days[queue_name]["SUCCESS"] == 3

    date_1day_back = date.utcnow() - datetime.timedelta(days=1)
    queue_checks_outcome_1day = await queue_stats.get_queue_checks_outcome_stats(
        fake_repository,
        partition_name,
        queue_name,
        branch_name,
        start_at=int(date_1day_back.timestamp()),
    )

    assert queue_checks_outcome_1day[queue_name]["PR_AHEAD_DEQUEUED"] == 0
    assert queue_checks_outcome_1day[queue_name]["SUCCESS"] == 3
