import datetime
from unittest import mock

from freezegun import freeze_time
import msgpack

from mergify_engine import yaml
from mergify_engine.queue import statistics
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import base


class TestStatisticsRedis(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_statistics_format_in_redis_without_partitions(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")

        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.wait_for_pull_request("closed", draft_pr["number"])

        prs_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p1["number"], p2["number"]]) == sorted(
            [p["number"] for p in prs_closed]
        )

        time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

        bdata = await self.redis_links.stats.xrange(time_to_merge_key, min="-", max="+")
        for _, raw in bdata:
            unpacked = msgpack.unpackb(raw[b"data"], timestamp=3)
            assert "time_seconds" in unpacked
            assert unpacked["branch_name"] == self.main_branch_name
            assert unpacked["queue_name"] == "default"
            assert unpacked["partition_name"] is None

            statistics.TimeToMerge(**unpacked)

        # Add a statistic with the old format (without partition name)
        # just to make sure that we are still able to use them.
        await self.redis_links.stats.xadd(
            time_to_merge_key,
            fields={
                b"version": statistics.VERSION,
                b"data": msgpack.packb(
                    {
                        "queue_name": "default",
                        "branch_name": self.main_branch_name,
                        "time_seconds": 1,
                    },
                    datetime=True,
                ),
            },
        )

        assert await self.redis_links.stats.xlen(time_to_merge_key) == 3
        # In this function, we instantiate `TimeToMerge` class,
        # so this call also make sure that both new and old statistics can
        # be used properly.
        stats = await statistics.get_time_to_merge_stats(self.repository_ctxt)
        assert len(stats) == 1
        assert "default" in stats
        assert len(stats[qr_config.QueueName("default")]) == 3

    async def test_statistics_format_in_redis_with_partitions(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projB",
                    "conditions": [
                        "files~=^projB/",
                    ],
                },
            ],
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"projA/test.txt": "test"})
        p2 = await self.create_pr(files={"projB/test.txt": "test"})

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("opened")
        await self.wait_for_pull_request("opened")

        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")

        time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

        bdata = await self.redis_links.stats.xrange(time_to_merge_key, min="-", max="+")
        for _, raw in bdata:
            unpacked = msgpack.unpackb(raw[b"data"], timestamp=3)
            assert "time_seconds" in unpacked
            assert unpacked["branch_name"] == self.main_branch_name
            assert unpacked["queue_name"] == "default"
            assert unpacked["partition_name"] in ("projA", "projB")

            statistics.TimeToMerge(**unpacked)

    async def test_accuracy_measure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        start_date = datetime.datetime(2022, 8, 18, 10, tzinfo=datetime.UTC)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr1 = await self.wait_for_pull_request("opened")

        with freeze_time(start_date + datetime.timedelta(hours=1), tick=True):
            # Merge first PR 1 hour after
            await self.create_status(tmp_mq_pr1["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr1["number"])
            await self.wait_for_pull_request("closed", p1["number"])

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr2 = await self.wait_for_pull_request("opened")

        with freeze_time(start_date + datetime.timedelta(hours=2), tick=True):
            # Merge 2nd PR 1 hour after
            await self.create_status(tmp_mq_pr2["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr2["number"])
            await self.wait_for_pull_request("closed", p2["number"])

            checks_duration_key = self.get_statistic_redis_key("checks_duration")
            assert await self.redis_links.stats.xlen(checks_duration_key) == 2

        with freeze_time(
            start_date + datetime.timedelta(hours=2, minutes=5),
            tick=True,
        ):
            await self.add_label(p3["number"], "queue")
            await self.add_label(p4["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3_4 = await self.wait_for_pull_request("opened")

        with freeze_time(
            start_date + datetime.timedelta(hours=3), tick=True
        ), mock.patch("mergify_engine.queue.statistics_accuracy.statsd") as statsd:
            statsd.gauge = mock.Mock()
            await self.create_status(tmp_mq_pr_3_4["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr_3_4["number"])
            await self.wait_for_pull_request("closed")
            await self.wait_for_pull_request("closed")

            assert await self.redis_links.stats.xlen(checks_duration_key) == 4

            # Called twice per PR for the last 2 PRs
            assert statsd.gauge.call_count == 4

            # Since the time is freezed but still ticking, we cannot know the real value.
            # The best we can do is to make sure that the eta diff is around
            # 5 minutes.
            # 5 minutes difference because the first 2 PRs were merged with 1h checks duration
            # and the last 2 PRs were queued at `start_date + datetime.timedelta(hours=2, minutes=5)` and merged at `start_date + datetime.timedelta(hours=3)`
            assert -5 < statsd.gauge.call_args_list[1].args[1] < -4.0
