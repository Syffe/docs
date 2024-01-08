import datetime

from mergify_engine import date
from mergify_engine import settings
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel
from mergify_engine.web.api.statistics import queue_checks_outcome
from mergify_engine.web.api.statistics import utils as web_stat_utils
from mergify_engine.yaml import yaml


class TestStatisticsEndpoints(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_time_to_merge_endpoint_normal(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                },
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
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert len(r.json()) == 1
            assert r.json()[0]["partition_name"] == partr_config.DEFAULT_PARTITION_NAME
            assert len(r.json()[0]["queues"]) == 1
            assert r.json()[0]["queues"][0]["queue_name"] == "default"
            assert r.json()[0]["queues"][0]["time_to_merge"]["median"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                r.json()["median"]
                > datetime.timedelta(hours=1, minutes=58).total_seconds()
            )
            previous_result = r.json()["median"]

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert len(r.json()) == 1
            assert len(r.json()[0]["queues"]) == 1
            assert (
                r.json()[0]["queues"][0]["time_to_merge"]["median"] == previous_result
            )

        with time_travel(
            start_date + (web_stat_utils.QUERY_MERGE_QUEUE_STATS_RETENTION / 2),
            tick=True,
        ):
            at_timestamp = int((start_date + datetime.timedelta(hours=3)).timestamp())
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={at_timestamp}",
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/stats/time_to_merge?at={at_timestamp}",
            )

            assert r.status_code == 200
            assert len(r.json()) == 1
            assert len(r.json()[0]["queues"]) == 1
            assert (
                r.json()[0]["queues"][0]["time_to_merge"]["median"] == previous_result
            )

    async def test_time_to_merge_endpoint_with_schedule(self) -> None:
        rules = {
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
                },
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

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        # Friday at 18:00, outside schedule
        with time_travel(start_date + datetime.timedelta(hours=8), tick=True):
            # Create status for the schedule to be the only condition not valid
            await self.create_status(tmp_mq_pr["pull_request"])
            # Run the engine for it to update train state
            await self.run_engine({"delayed-refresh"})

        with time_travel(start_date + datetime.timedelta(days=3), tick=True):
            # We are in schedule again, PR should be updated itself because
            # of delayed refresh
            await self.run_engine({"delayed-refresh"})

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Result should be only around ~8hours
            assert (
                datetime.timedelta(hours=8, minutes=2).total_seconds()
                > r.json()["mean"]
                > datetime.timedelta(hours=7, seconds=58).total_seconds()
            )

    async def test_time_to_merge_endpoint_with_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                },
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

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

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

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Result should be only around ~1day
            assert (
                datetime.timedelta(days=1, minutes=2).total_seconds()
                > r.json()["mean"]
                > datetime.timedelta(hours=23, minutes=58).total_seconds()
            )

    async def test_checks_duration_endpoint(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "speculative_checks": 1,
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
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
        with time_travel("2022-08-18T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration",
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        with time_travel("2022-08-18T12:00:00", tick=True):
            await self.create_status(tmp_mq_pr["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration",
            )

            assert r.status_code == 200
            assert r.json().get("mean") is not None
            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                datetime.timedelta(hours=1, minutes=55).total_seconds()
                < r.json()["median"]
                < datetime.timedelta(hours=2, minutes=10).total_seconds()
            )

    async def test_start_at_end_at_query_params(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                },
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
        await self.setup_repo(yaml.dump(rules))

        timestamp = int(date.utcnow().timestamp())

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "closed"})

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?start_at={timestamp}",
        )

        assert r.status_code == 200
        assert r.json()[queue_utils.PrDequeued.dequeue_code] == 1
        assert r.json()[queue_utils.PrAheadDequeued.dequeue_code] == 2

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?end_at={timestamp}",
        )

        assert r.status_code == 200
        for count in r.json().values():
            assert count == 0

    async def test_time_to_merge_endpoint_at_timestamp_too_far(self) -> None:
        at_timestamp = int(
            (
                date.utcnow() - (web_stat_utils.QUERY_MERGE_QUEUE_STATS_RETENTION * 2)
            ).timestamp(),
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={at_timestamp}",
        )

        assert r.status_code == 400
        assert (
            f"The provided 'at' timestamp ({at_timestamp}) is too far in the past"
            in r.json()["detail"]
        )

    async def test_queue_statistics_branch_name_filter(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                },
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
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                r.json()["median"]
                > datetime.timedelta(hours=1, minutes=58).total_seconds()
            )
            previous_result = r.json()["median"]

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch=abc123",
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

    async def test_queue_checks_outcome_endpoint(self) -> None:
        rules = {
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
                },
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
        with time_travel("2022-08-18T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/stats/queue_checks_outcome",
            )

            assert r.status_code == 200
            assert len(r.json()) == 1
            assert r.json()[0]["partition_name"] == partr_config.DEFAULT_PARTITION_NAME
            assert len(r.json()[0]["queues"]) == 1
            assert r.json()[0]["queues"][0]["queue_name"] == "default"

            # NOTE(Kontrolix): TARGET_BRANCH_CHANGED and TARGET_BRANCH_MISSING are
            # manually added for retrocompatibility. They are copy of
            # BASE_BRANCH_CHANGED and BASE_BRANCH_MISSING
            assert r.json()[0]["queues"][0][
                "queue_checks_outcome"
            ] == queue_checks_outcome.BASE_QUEUE_CHECKS_OUTCOME_T_DICT | {
                "TARGET_BRANCH_CHANGED": 0,
                "TARGET_BRANCH_MISSING": 0,
            }
            # #####
            # Create FailureByReason
            p1 = await self.create_pr()
            p2 = await self.create_pr(two_commits=True)
            p3 = await self.create_pr()

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
            # Create ChecksDuration
            p4 = await self.create_pr()

            await self.add_label(p4["number"], "queue")
            await self.run_engine({"delayed-refresh"})

            draft_pr = await self.wait_for_pull_request("opened")
            await self.create_status(draft_pr["pull_request"])

        with time_travel("2022-08-18T12:00:00", tick=True):
            await self.run_engine({"delayed-refresh"})

            await self.wait_for_pull_request("closed", draft_pr["number"])
            await self.wait_for_pull_request("closed", p4["number"])

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome",
            )

            assert r.status_code == 200
            assert r.json()[queue_utils.PrDequeued.dequeue_code] == 2
            assert r.json()[queue_utils.PrAheadDequeued.dequeue_code] == 3
            assert r.json()["SUCCESS"] == 1

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/stats/queue_checks_outcome",
            )

            assert r.status_code == 200
            assert len(r.json()) == 1
            assert r.json()[0]["partition_name"] == partr_config.DEFAULT_PARTITION_NAME
            assert len(r.json()[0]["queues"]) == 1
            assert r.json()[0]["queues"][0]["queue_name"] == "default"
            assert (
                r.json()[0]["queues"][0]["queue_checks_outcome"][
                    queue_utils.PrDequeued.dequeue_code
                ]
                == 2
            )
            assert (
                r.json()[0]["queues"][0]["queue_checks_outcome"][
                    queue_utils.PrAheadDequeued.dequeue_code
                ]
                == 3
            )
            assert r.json()[0]["queues"][0]["queue_checks_outcome"]["SUCCESS"] == 1

    async def test_stats_endpoint_timestamp_in_future(self) -> None:
        with time_travel("2022-08-18T10:00:00"):
            future_timestamp = int(date.utcnow().timestamp()) + 1000
            fail_urls = [
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={future_timestamp}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?start_at={future_timestamp}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?end_at={future_timestamp}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?start_at={future_timestamp}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?end_at={future_timestamp}",
            ]
            now_ts = int(date.utcnow().timestamp())
            valid_urls = [
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={now_ts}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?start_at={now_ts}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?end_at={now_ts}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?start_at={now_ts}",
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?end_at={now_ts}",
            ]

            for url in fail_urls:
                r = await self.admin_app.get(
                    url,
                )
                assert r.status_code == 422

            for url in valid_urls:
                r = await self.admin_app.get(
                    url,
                )
                assert r.status_code == 200
