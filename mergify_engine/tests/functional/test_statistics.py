import datetime
from unittest import mock

from mergify_engine import yaml
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel


class TestStatisticsRedis(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_accuracy_measure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
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

        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            # Merge first PR 1 hour after
            await self.create_status(tmp_mq_pr1["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr1["number"])
            await self.wait_for_pull_request("closed", p1["number"])

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            # Merge 2nd PR 1 hour after
            await self.create_status(tmp_mq_pr2["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr2["number"])
            await self.wait_for_pull_request("closed", p2["number"])

        with time_travel(
            start_date + datetime.timedelta(hours=2, minutes=5),
            tick=True,
        ):
            await self.add_label(p3["number"], "queue")
            await self.add_label(p4["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3_4 = await self.wait_for_pull_request("opened")

        with time_travel(
            start_date + datetime.timedelta(hours=3), tick=True
        ), mock.patch("mergify_engine.queue.statistics_accuracy.statsd") as statsd:
            statsd.gauge = mock.Mock()
            await self.create_status(tmp_mq_pr_3_4["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr_3_4["number"])
            await self.wait_for_pull_request("closed")
            await self.wait_for_pull_request("closed")

            # Called twice per PR for the last 2 PRs
            assert statsd.gauge.call_count == 4

            # Since the time is freezed but still ticking, we cannot know the real value.
            # The best we can do is to make sure that the eta diff is around
            # 5 minutes.
            # 5 minutes difference because the first 2 PRs were merged with 1h checks duration
            # and the last 2 PRs were queued at `start_date + datetime.timedelta(hours=2, minutes=5)` and merged at `start_date + datetime.timedelta(hours=3)`
            assert -5.2 < statsd.gauge.call_args_list[1].args[1] < -4.0
