import datetime

from freezegun import freeze_time

from mergify_engine import config
from mergify_engine import date
from mergify_engine import yaml
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.queue import utils as queue_utils
from mergify_engine.tests.functional import base


class TestStatisticsEndpoints(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_time_to_merge_endpoint_normal(self) -> None:
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

        start_date = datetime.datetime(2022, 8, 18, 10, tzinfo=datetime.timezone.utc)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

        with freeze_time(start_date + datetime.timedelta(hours=2), tick=True):
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
            assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
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

        with freeze_time(
            start_date + (queue_statistics.QUERY_MERGE_QUEUE_STATS_RETENTION / 2),
            tick=True,
        ):
            at_timestamp = int((start_date + datetime.timedelta(hours=3)).timestamp())
            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={at_timestamp}",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result

    async def test_time_to_merge_endpoint_with_schedule(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
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
        start_date = datetime.datetime(2022, 10, 14, 10, tzinfo=datetime.timezone.utc)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
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
        with freeze_time(start_date + datetime.timedelta(hours=8), tick=True):
            # Create status for the schedule to be the only condition not valid
            await self.create_status(tmp_mq_pr["pull_request"])
            # Run the engine for it to update train state
            await self.run_full_engine()

        with freeze_time(start_date + datetime.timedelta(days=3), tick=True):
            # We are in schedule again, PR should be updated itself because
            # of delayed refresh
            await self.run_full_engine()

            await self.wait_for(
                "pull_request", {"action": "closed"}, timeout=30 if base.RECORD else 1
            )
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
            assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
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

        start_date = datetime.datetime(2022, 10, 14, 10, tzinfo=datetime.timezone.utc)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")

            r = await self.app.put(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
                json={"reason": "test"},
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )
            assert r.status_code == 200
            await self.run_full_engine()

        with freeze_time(start_date + datetime.timedelta(days=1), tick=True):

            await self.create_status(tmp_mq_pr["pull_request"])
            # Run the engine for it to update train state
            await self.run_full_engine()

        with freeze_time(start_date + datetime.timedelta(days=1, hours=8), tick=True):
            r = await self.app.delete(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )
            await self.run_full_engine()

            await self.wait_for(
                "pull_request", {"action": "closed"}, timeout=30 if base.RECORD else 1
            )
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
            assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            # Result should be only around ~1day
            assert (
                datetime.timedelta(days=1, minutes=2).total_seconds()
                > r.json()["mean"]
                > datetime.timedelta(hours=23, minutes=58).total_seconds()
            )

    async def test_failure_by_reason_endpoint(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "closed"})

        failure_by_reason_key = self.get_statistic_redis_key("failure_by_reason")
        assert await self.redis_links.stats.xlen(failure_by_reason_key) == 3

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 200
        assert r.json()[queue_utils.PrAheadDequeued.abort_code] == 3

    async def test_checks_duration_endpoint(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "speculative_checks": 1,
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
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
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        with freeze_time("2022-08-18T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

        with freeze_time("2022-08-18T12:00:00", tick=True):
            await self.run_full_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            checks_duration_key = self.get_statistic_redis_key("checks_duration")
            assert await self.redis_links.stats.xlen(checks_duration_key) == 1

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            # Because of execution time we can't really make sure that it will
            # always be the expected number. The best we can do is make sure
            # it is at least close to what we expect (around 2 hours).
            assert (
                datetime.timedelta(hours=1, minutes=58).total_seconds()
                < r.json()["mean"]
                < datetime.timedelta(hours=2, minutes=10).total_seconds()
            )

    async def test_start_at_end_at_query_params(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
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
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "closed"})

        failure_by_reason_key = self.get_statistic_redis_key("failure_by_reason")
        assert await self.redis_links.stats.xlen(failure_by_reason_key) == 3

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?start_at={timestamp}",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 200
        assert r.json()[queue_utils.PrAheadDequeued.abort_code] == 3

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?end_at={timestamp}",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 200
        for count in r.json().values():
            assert count == 0

    async def test_time_to_merge_endpoint_at_timestamp_too_far(self) -> None:
        at_timestamp = int(
            (
                date.utcnow() - (queue_statistics.QUERY_MERGE_QUEUE_STATS_RETENTION * 2)
            ).timestamp()
        )

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={at_timestamp}",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
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

        start_date = datetime.datetime(2022, 8, 18, 10, tzinfo=datetime.timezone.utc)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")

            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

        with freeze_time(start_date + datetime.timedelta(hours=2), tick=True):
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
            assert await self.redis_links.stats.xlen(time_to_merge_key) == 2

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
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

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch={self.main_branch_name}",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["median"] == previous_result

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch=abc123",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["median"] is None

    async def test_queue_checks_outcome_endpoint(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
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
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        with freeze_time("2022-08-18T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))

            # #####
            # Create FailureByReason
            p1 = await self.create_pr()
            p2 = await self.create_pr(two_commits=True)
            p3 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

            await self.remove_label(p1["number"], "queue")
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})

            failure_by_reason_key = self.get_statistic_redis_key("failure_by_reason")
            assert await self.redis_links.stats.xlen(failure_by_reason_key) == 3

            await self.close_pull(p1["number"])
            await self.close_pull(p2["number"])
            await self.close_pull(p3["number"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})

            # #####
            # Create ChecksDuration
            p4 = await self.create_pr()

            # To force others to be rebased
            p5 = await self.create_pr()
            await self.merge_pull(p5["number"])
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p4["number"], "queue")
            await self.run_full_engine()

            tmp_mq_pr = await self.wait_for_pull_request("opened")
            await self.create_status(tmp_mq_pr["pull_request"])

        with freeze_time("2022-08-18T12:00:00", tick=True):
            await self.run_full_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()[queue_utils.PrAheadDequeued.abort_code] == 3
            assert r.json()["SUCCESS"] == 1

    async def test_stats_endpoint_timestamp_in_future(self) -> None:
        with freeze_time("2022-08-18T10:00:00"):
            future_timestamp = int(date.utcnow().timestamp()) + 1000
            fail_urls = [
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?start_at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?end_at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?start_at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?end_at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?start_at={future_timestamp}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?end_at={future_timestamp}",
            ]
            now_ts = int(date.utcnow().timestamp())
            valid_urls = [
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?start_at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/checks_duration?end_at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?start_at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/failure_by_reason?end_at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?start_at={now_ts}",
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/queue_checks_outcome?end_at={now_ts}",
            ]

            for url in fail_urls:
                r = await self.app.get(
                    url,
                    headers={
                        "Authorization": f"bearer {self.api_key_admin}",
                        "Content-type": "application/json",
                    },
                )
                assert r.status_code == 422

            for url in valid_urls:
                r = await self.app.get(
                    url,
                    headers={
                        "Authorization": f"bearer {self.api_key_admin}",
                        "Content-type": "application/json",
                    },
                )
                assert r.status_code == 200
