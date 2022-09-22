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
            assert r.json()["mean"] is None

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
                r.json()["mean"]
                > datetime.timedelta(hours=1, minutes=58).total_seconds()
            )
            previous_result = r.json()["mean"]

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
            assert r.json()["mean"] == previous_result

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
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        await self.wait_for("pull_request", {"action": "opened"})
        await self.run_full_engine()

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
            await self.run_engine()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

        with freeze_time("2022-08-18T12:00:00", tick=True):
            await self.run_full_engine()

            await self.wait_for(
                "check_suite", {"check_suite": {"conclusion": "success"}}
            )
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
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        await self.wait_for("pull_request", {"action": "opened"})
        await self.run_full_engine()

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
            assert r.json()["mean"] is None

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
                r.json()["mean"]
                > datetime.timedelta(hours=1, minutes=58).total_seconds()
            )
            previous_result = r.json()["mean"]

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch={self.main_branch_name}",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["mean"] == previous_result

            r = await self.app.get(
                f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/default/stats/time_to_merge?branch=abc123",
                headers={
                    "Authorization": f"bearer {self.api_key_admin}",
                    "Content-type": "application/json",
                },
            )

            assert r.status_code == 200
            assert r.json()["mean"] is None
