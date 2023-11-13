import datetime

from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.queue import utils as queue_utils
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel
from mergify_engine.web.api.statistics import utils as web_stat_utils


class TestStatisticsWithPartitionsEndpoints(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_time_to_merge_with_partitions_endpoint_normal(self) -> None:
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
                    "merge_conditions": [
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

        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/time_to_merge",
            )
            assert r.status_code == 200
            assert r.json()["median"] is None

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/time_to_merge",
            )
            assert r.status_code == 200
            assert r.json()["median"] is None

            p1 = await self.create_pr(files={"projA/test1.txt": "test"})
            p2 = await self.create_pr(files={"projA/test2.txt": "test"})
            p3 = await self.create_pr(files={"projB/test1.txt": "test"})
            p4 = await self.create_pr(files={"projB/test2.txt": "test"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_projA = await self.wait_for_pull_request("opened")

            await self.add_label(p3["number"], "queue")
            await self.add_label(p4["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_projB = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr_projA["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr_projA["number"])
            await self.wait_for_pull_request("closed")
            await self.wait_for_pull_request("closed")

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/time_to_merge",
            )
            assert r.status_code == 200

            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                r.json()["median"]
                > datetime.timedelta(hours=1, minutes=58).total_seconds()
            )
            previous_result_projA = r.json()["median"]

        with time_travel(start_date + datetime.timedelta(hours=4), tick=True):
            await self.create_status(tmp_mq_pr_projB["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr_projB["number"])
            await self.wait_for_pull_request("closed")
            await self.wait_for_pull_request("closed")

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/time_to_merge",
            )
            assert r.status_code == 200

            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 4 hours).
            assert (
                r.json()["median"]
                > datetime.timedelta(hours=3, minutes=58).total_seconds()
            )
            previous_result_projB = r.json()["median"]

        with time_travel(
            start_date + (web_stat_utils.QUERY_MERGE_QUEUE_STATS_RETENTION / 2),
            tick=True,
        ):
            at_timestamp = int((start_date + datetime.timedelta(hours=5)).timestamp())
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/time_to_merge?at={at_timestamp}",
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result_projA

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/time_to_merge?at={at_timestamp}",
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result_projB

    async def test_time_to_merge_with_partitions_endpoint_with_schedule(self) -> None:
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
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                        "schedule=Mon-Fri 08:00-17:00[UTC]",
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

        # Friday
        start_date = datetime.datetime(2022, 10, 14, 10, tzinfo=datetime.UTC)

        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr(files={"projA/test1.txt": "test"})
            p2 = await self.create_pr(files={"projA/test2.txt": "test"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        # Friday at 18:00, outside schedule
        with time_travel(start_date + datetime.timedelta(hours=8), tick=True):
            # Create status for the schedule to be the only condition not valid
            await self.create_status(tmp_mq_pr["pull_request"])
            # Run the engine for it to update train state
            await self.run_full_engine()

        with time_travel(start_date + datetime.timedelta(days=3), tick=True):
            # We are in schedule again, PR should be updated itself because
            # of delayed refresh
            await self.run_full_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr["number"])
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Result should be only around ~8hours
            assert (
                datetime.timedelta(hours=8, minutes=2).total_seconds()
                > r.json()["mean"]
                > datetime.timedelta(hours=7, seconds=58).total_seconds()
            )

    async def test_time_to_merge_with_partitions_endpoint_with_freeze(self) -> None:
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
                    "merge_conditions": [
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

        start_date = datetime.datetime(2022, 10, 14, 10, tzinfo=datetime.UTC)

        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr(files={"projA/test1.txt": "test"})
            p2 = await self.create_pr(files={"projA/test2.txt": "test"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

            r = await self.admin_app.put(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
                json={"reason": "test"},
            )
            assert r.status_code == 200
            await self.run_engine()

        with time_travel(start_date + datetime.timedelta(days=1), tick=True):
            await self.create_status(tmp_mq_pr["pull_request"])
            # Run the engine for it to update train state
            await self.run_engine()

        with time_travel(start_date + datetime.timedelta(days=1, hours=8), tick=True):
            r = await self.admin_app.delete(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
            )
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr["number"])
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Result should be only around ~1day
            assert (
                datetime.timedelta(days=1, minutes=2).total_seconds()
                > r.json()["mean"]
                > datetime.timedelta(hours=23, minutes=58).total_seconds()
            )

    async def test_checks_duration_with_partitions_endpoint(self) -> None:
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
                    "speculative_checks": 1,
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 8, 18, 10)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/checks_duration",
            )
            assert r.status_code == 200
            assert r.json()["median"] is None

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/checks_duration",
            )
            assert r.status_code == 200
            assert r.json()["median"] is None

            p1 = await self.create_pr(files={"projA/test.txt": "test"})

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_mq_pr["number"])
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/checks_duration",
            )
            assert r.status_code == 200
            assert r.json()["mean"] is not None
            assert r.json()["median"] is not None

            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                datetime.timedelta(hours=1, minutes=55).total_seconds()
                < r.json()["median"]
                < datetime.timedelta(hours=2, minutes=10).total_seconds()
            )

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/checks_duration",
            )
            assert r.status_code == 200
            assert r.json()["median"] is None
            assert r.json()["mean"] is None

    async def test_queue_checks_outcome_with_partitions_endpoint(self) -> None:
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
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "label=queue",
                    ],
                    "allow_inplace_checks": False,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                    "speculative_checks": 1,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 8, 18, 10)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            # #####
            # Create FailureByReason
            p1 = await self.create_pr(files={"projA/test1.txt": "test"})
            p2 = await self.create_pr(
                two_commits=True, files={"projA/test2.txt": "test"}
            )
            p3 = await self.create_pr(files={"projA/test3.txt": "test"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            draft_pr = await self.wait_for_pull_request("opened")

            await self.remove_label(p1["number"], "queue")
            await self.run_engine()
            await self.wait_for_pull_request("closed", draft_pr["number"])
            draft_pr = await self.wait_for_pull_request("opened")

            await self.close_pull(p1["number"])
            await self.wait_for_pull_request("closed", p1["number"])
            await self.close_pull(p2["number"])
            await self.wait_for_pull_request("closed", p2["number"])
            await self.close_pull(p3["number"])
            await self.wait_for_pull_request("closed", p3["number"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", draft_pr["number"])

            # #####
            # Create ChecksDuration for SUCCESS
            p4 = await self.create_pr(files={"projA/test4.txt": "test"})
            await self.add_label(p4["number"], "queue")
            await self.run_full_engine()

            draft_pr = await self.wait_for_pull_request("opened")
            await self.create_status(draft_pr["pull_request"])

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.run_full_engine()

            await self.wait_for_pull_request("closed", draft_pr["number"])
            await self.wait_for_pull_request("closed", p4["number"])

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projA/queues/default/stats/queue_checks_outcome",
            )

            assert r.status_code == 200
            assert r.json()[queue_utils.PrDequeued.dequeue_code] == 2
            assert r.json()[queue_utils.PrAheadDequeued.dequeue_code] == 3
            assert r.json()["SUCCESS"] == 1

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/projB/queues/default/stats/queue_checks_outcome",
            )
            assert r.status_code == 200
            assert len(r.json().values()) > 0
            for value in r.json().values():
                assert value == 0
