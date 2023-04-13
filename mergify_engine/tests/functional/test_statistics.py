import datetime
from unittest import mock

from freezegun import freeze_time
import msgpack

from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.queue import statistics
from mergify_engine.tests.functional import base


class TestStatisticsRedis(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_statistics_format_in_redis(self) -> None:
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

        await self.wait_for("pull_request", {"action": "opened"})

        await self.run_engine()

        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        redis_repo_key = statistics._get_repository_key(
            self.subscription.owner_id, self.RECORD_CONFIG["repository_id"]
        )
        time_to_merge_key = f"{redis_repo_key}/time_to_merge"
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

        bdata = await self.redis_links.stats.xrange(time_to_merge_key, min="-", max="+")
        for _, raw in bdata:
            statistics.TimeToMerge(**msgpack.unpackb(raw[b"data"], timestamp=3))

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

            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr1 = await self.wait_for_pull_request("opened")

        with freeze_time(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr1["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
            assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch={self.main_branch_name}",
            )

            estimated_value = r.json()["median"]
            assert estimated_value is not None
            assert r.status_code == 200

        with freeze_time(
            start_date + datetime.timedelta(hours=2, minutes=2), tick=True
        ):
            await self.add_label(p3["number"], "queue")
            await self.add_label(p4["number"], "queue")

            await self.run_engine()

            tmp_mq_pr2 = await self.wait_for_pull_request("opened")

        with freeze_time(
            start_date + datetime.timedelta(hours=3), tick=True
        ), mock.patch("mergify_engine.queue.statistics_accuracy.statsd") as statsd:
            statsd.gauge = mock.Mock()
            await self.create_status(tmp_mq_pr2["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            assert await self.redis_links.stats.xlen(time_to_merge_key) == 4

            # Called once per PR that get out of the queue, so
            # 6 times since we have 2 PR in merge queue.
            assert statsd.gauge.call_count == 6
            assert statsd.gauge.call_args_list[0].args == (
                "statistics.time_to_merge.accuracy.median_value",
                estimated_value,
            )

            assert (
                statsd.gauge.call_args_list[1].args[0]
                == "statistics.time_to_merge.accuracy.real_value"
            )
            assert statsd.gauge.call_args_list[2].args == (
                "statistics.time_to_merge.accuracy.seconds_waiting_for_queue_freeze",
                0,
            )

            assert (
                statsd.gauge.call_args_list[1].args[0]
                == "statistics.time_to_merge.accuracy.real_value"
            )

            # Since the time is freezed but still ticking, we cannot know the real value.
            # The best we can do is make sure its around the 58 minutes mark.
            assert (
                datetime.timedelta(minutes=56).total_seconds()
                < statsd.gauge.call_args_list[1].args[1]
                < datetime.timedelta(hours=1).total_seconds()
            )
