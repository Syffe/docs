import datetime
import os
from unittest import mock

import anys
import pytest

from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import settings
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel
from mergify_engine.web.api.statistics import utils as web_stats_utils
from mergify_engine.yaml import yaml


class TestQueueApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_invalid_rules_in_config(self) -> None:
        invalid_rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "batch_max_wait_time": "15 s",
                    "speculative_checks": 1,
                    "false_condition": 3,
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "false_condition": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "checks_timeout": "10 m",
                    "false_condition": "mergify-test4",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"false_action": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"false_action": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"false_action": {"name": "low-priority"}},
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(invalid_rules),
            forward_to_engine=True,
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
        )

        assert r.status_code == 422
        assert r.json() == {"detail": "The configuration file is invalid."}

    async def test_get_queues_config_list(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "batch_max_wait_time": "15 s",
                    "speculative_checks": 1,
                    "batch_size": 3,
                    "queue_branch_prefix": "urgent-",
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "speculative_checks": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "checks_timeout": "10 m",
                    "draft_bot_account": "mergify-test4",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"queue": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"queue": {"name": "low-priority"}},
                },
            ],
        }

        # Mock get_mergify_config_file to force the case where we do not
        # have a config file. Default behavior will look into the repository for
        # a config file but, since we are in test mode and everyone might
        # have a config file in its own test repo, it could fail.
        mock_get_mergify_config_file = mock.patch.object(
            context.Repository,
            "get_mergify_config_file",
            return_value=None,
        )
        mock_get_mergify_config_file.start()
        # Add a cleanup in case we do not reach the `.stop()`
        self.addCleanup(mock_get_mergify_config_file.stop)

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
        )

        mock_get_mergify_config_file.stop()

        assert r.status_code == 200
        assert r.json() == {
            "configuration": [
                {
                    "name": "default",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": True,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 1,
                        "batch_max_wait_time": 30.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "speculative_checks": 1,
                        "priority": 0,
                        "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                        "queue_branch_merge_method": None,
                        "batch_max_failure_resolution_attempts": None,
                        "commit_message_template": None,
                        "merge_method": None,
                        "merge_bot_account": None,
                        "update_method": None,
                        "update_bot_account": None,
                        "autosquash": True,
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            forward_to_engine=True,
            preload_configuration=True,
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
        )

        assert r.status_code == 200
        assert r.json() == {
            "configuration": [
                {
                    "name": "urgent",
                    "config": {
                        "allow_inplace_checks": True,
                        "allow_queue_branch_edit": False,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 3,
                        "batch_max_wait_time": 15.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "queue_branch_prefix": "urgent-",
                        "queue_branch_merge_method": None,
                        "priority": 2,
                        "speculative_checks": 1,
                        "batch_max_failure_resolution_attempts": None,
                        "commit_message_template": None,
                        "merge_method": None,
                        "merge_bot_account": None,
                        "update_method": None,
                        "update_bot_account": None,
                        "autosquash": True,
                    },
                },
                {
                    "name": "default",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": True,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 2,
                        "batch_max_wait_time": 0.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                        "queue_branch_merge_method": None,
                        "priority": 1,
                        "speculative_checks": 2,
                        "batch_max_failure_resolution_attempts": None,
                        "commit_message_template": None,
                        "merge_method": None,
                        "merge_bot_account": None,
                        "update_method": None,
                        "update_bot_account": None,
                        "autosquash": True,
                    },
                },
                {
                    "name": "low-priority",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": False,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 1,
                        "batch_max_wait_time": 30.0,
                        "checks_timeout": 600.0,
                        "draft_bot_account": "mergify-test4",
                        "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                        "queue_branch_merge_method": None,
                        "priority": 0,
                        "speculative_checks": 1,
                        "batch_max_failure_resolution_attempts": None,
                        "commit_message_template": None,
                        "merge_method": None,
                        "merge_bot_account": None,
                        "update_method": None,
                        "update_bot_account": None,
                        "autosquash": True,
                    },
                },
            ],
        }

    async def test_get_queues(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        p4 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        tmp_mq_pr_1 = await self.wait_for_pull_request("opened")
        await self.create_status(tmp_mq_pr_1["pull_request"])
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_mq_pr_2 = await self.wait_for_pull_request("opened")
        await self.create_status(tmp_mq_pr_2["pull_request"])
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_mq_pr_3 = await self.wait_for_pull_request("opened")
        await self.create_status(tmp_mq_pr_3["pull_request"])
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p4["number"], "queue")
        await self.run_engine()

        # GET /queues
        repository_name = self.RECORD_CONFIG["repository_name"]
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queues",
        )

        assert len(r.json()["queues"]) == 1
        assert len(r.json()["queues"][0]["pull_requests"]) == 1
        queue_pr_data = r.json()["queues"][0]["pull_requests"][0]

        assert queue_pr_data == {
            "number": p4["number"],
            "position": anys.ANY_INT,
            "priority": anys.ANY_INT,
            "effective_priority": anys.ANY_INT,
            "queue_rule": {
                "name": "foo",
                "config": anys.ANY_MAPPING,
            },
            "speculative_check_pull_request": {
                "in_place": False,
                "number": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "ended_at": None,
                "checks": anys.ANY_LIST,
                "evaluated_conditions": anys.ANY_STR,
                "state": "pending",
            },
            "mergeability_check": {
                "check_type": "draft_pr",
                "pull_request_number": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "ended_at": None,
                "state": "pending",
            },
            "queued_at": anys.ANY_DATETIME_STR,
            "estimated_time_of_merge": anys.ANY_DATETIME_STR,
            "partition_name": partr_config.DEFAULT_PARTITION_NAME,
        }

    async def test_get_queue_pull(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                        "schedule!=MON-FRI 12:00-15:00",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        # Tuesday
        with time_travel("2023-01-10T14:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p = await self.create_pr()
            await self.add_label(p["number"], "queue")
            await self.run_engine()

            # Create a pending check on the queue PR to see it in condition's
            # related checks
            tmp_mq_pr = await self.wait_for_pull_request("opened")
            await self.create_status(tmp_mq_pr["pull_request"], state="pending")
            await self.run_engine()
            ctxt = context.Context(self.repository_ctxt, p, [])
            queue_ctxt = context.Context(
                self.repository_ctxt,
                tmp_mq_pr["pull_request"],
                [],
            )
            pull = condition_value_querier.QueuePullRequest(ctxt, queue_ctxt)
            assert "continuous-integration/fake-ci" in (  # type:ignore [operator]
                await pull.get_attribute_value("check")
            )

        repository_name = self.RECORD_CONFIG["repository_name"]
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/{p['number']}",
        )

        q = await self.get_train()
        base_sha = await q.get_base_sha()

        assert r.status_code == 200
        assert r.json() == {
            "number": p["number"],
            "position": anys.ANY_INT,
            "priority": anys.ANY_INT,
            "effective_priority": anys.ANY_INT,
            "queue_rule": {
                "name": "foo",
                "config": anys.ANY_MAPPING,
            },
            "mergeability_check": {
                "check_type": "draft_pr",
                "pull_request_number": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "ended_at": None,
                "continuous_integrations_ended_at": None,
                "continuous_integrations_state": "pending",
                "checks": anys.ANY_LIST,
                "evaluated_conditions": anys.ANY_STR,
                "conditions_evaluation": {
                    "match": False,
                    "label": "all of",
                    "description": None,
                    "schedule": None,
                    "subconditions": [
                        {
                            "match": False,
                            "label": "check-success=continuous-integration/fake-ci",
                            "description": None,
                            "schedule": None,
                            "subconditions": [],
                            "evaluations": [
                                {
                                    "pull_request": p["number"],
                                    "match": False,
                                    "evaluation_error": None,
                                    "related_checks": [
                                        "continuous-integration/fake-ci",
                                    ],
                                    "next_evaluation_at": None,
                                },
                            ],
                        },
                        {
                            "match": False,
                            "label": "schedule!=MON-FRI 12:00-15:00",
                            "description": None,
                            "schedule": anys.ANY_MAPPING,
                            "subconditions": [],
                            "evaluations": [
                                {
                                    "pull_request": p["number"],
                                    "match": False,
                                    "evaluation_error": None,
                                    "related_checks": [],
                                    "next_evaluation_at": anys.AnyContains(
                                        "2023-01-10T15:00:01",
                                    ),
                                },
                            ],
                        },
                        {
                            "match": True,
                            "label": f"base={self.main_branch_name}",
                            "description": None,
                            "schedule": None,
                            "subconditions": [],
                            "evaluations": [
                                {
                                    "pull_request": p["number"],
                                    "match": True,
                                    "evaluation_error": None,
                                    "related_checks": [],
                                    "next_evaluation_at": None,
                                },
                            ],
                        },
                    ],
                    "evaluations": [],
                },
                "state": "pending",
            },
            "queued_at": anys.ANY_DATETIME_STR,
            "estimated_time_of_merge": None,
            "summary": {
                "title": f"The pull request is embarked with {self.main_branch_name} ({base_sha[:7]}) for merge and is waiting for checks to finish",
                "unexpected_changes": None,
                "freeze": None,
                "checks_timeout": None,
                "batch_failure": None,
            },
        }

        conditions_evaluation = r.json()["mergeability_check"]["conditions_evaluation"]
        schedule = conditions_evaluation["subconditions"][1]["schedule"]
        expected_day = {
            "times": [
                {
                    "start_at": {"hour": 0, "minute": 0},
                    "end_at": {"hour": 12, "minute": 0},
                },
                {
                    "start_at": {"hour": 15, "minute": 0},
                    "end_at": {"hour": 23, "minute": 59},
                },
            ],
        }
        assert schedule == {
            "timezone": "UTC",
            "days": {
                "monday": expected_day,
                "tuesday": expected_day,
                "wednesday": expected_day,
                "thursday": expected_day,
                "friday": expected_day,
                "saturday": date.FULL_DAY,
                "sunday": date.FULL_DAY,
            },
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/unknown_queue/pull/{p['number']}",
        )
        assert r.status_code == 404
        assert r.json()["detail"] == "The queue `unknown_queue` does not exist."

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/0",
        )
        assert r.status_code == 404
        assert r.json()["detail"] == "Pull request not found."

    async def _get_median_check_durations(self) -> float:
        async with database.create_session() as session:
            stat = await web_stats_utils.get_queue_checks_duration(
                session,
                self.repository_ctxt.repo["id"],
                queue_names=(),
                partition_names=(),
            )
            assert stat["median"] is not None
            assert stat["median"] > 0
            return stat["median"]

    async def test_estimated_time_of_merge_normal(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 1, 5, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()
            await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            await self.create_status(tmp_mq_pr_1["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr_2["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            await self.create_status(tmp_mq_pr_3["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            median_checks_duration = await self._get_median_check_durations()

            await self.add_label(p4["number"], "queue")
            await self.run_engine()

            await self.wait_for_pull_request("opened")

            # GET /queues
            repository_name = self.RECORD_CONFIG["repository_name"]
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 1
            queue_pr_data = r.json()["queues"][0]["pull_requests"][0]
            checks_start = datetime.datetime.fromisoformat(
                queue_pr_data["mergeability_check"]["started_at"],
            )
            expected_time_of_merge = checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )
            assert (
                queue_pr_data["estimated_time_of_merge"]
                # The replace is here because pydantic use the iso `Z` instead
                # of `+00:00` (both are valid iso formats)
                == expected_time_of_merge.isoformat().replace("+00:00", "Z")
            )

    async def test_estimated_time_of_merge_multiple_pr_waiting(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 1, 5, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()
            p5 = await self.create_pr()
            p6 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            await self.create_status(tmp_mq_pr_1["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr_2["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            await self.create_status(tmp_mq_pr_3["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            median_checks_duration = await self._get_median_check_durations()

            await self.add_label(p4["number"], "queue")
            await self.run_engine()
            await self.wait_for_pull_request("opened")

            # No wait for PR opened for those 2 since they are waiting at
            # the end of the queue and that speculative_checks = 1 in conf.
            await self.add_label(p5["number"], "queue")
            await self.run_engine()

            await self.add_label(p6["number"], "queue")
            await self.run_engine()

            # GET /queues
            repository_name = self.RECORD_CONFIG["repository_name"]
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 3
            first_queued_pr_data = r.json()["queues"][0]["pull_requests"][0]
            first_queued_pr_checks_start = datetime.datetime.fromisoformat(
                first_queued_pr_data["mergeability_check"]["started_at"],
            )
            first_queued_pr_started_at = datetime.datetime.fromisoformat(
                first_queued_pr_data["mergeability_check"]["started_at"],
            )

            assert datetime.datetime.fromisoformat(
                first_queued_pr_data["estimated_time_of_merge"],
            ) == first_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )

            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][1]["estimated_time_of_merge"],
            ) == first_queued_pr_started_at + datetime.timedelta(
                seconds=2 * median_checks_duration,
            )
            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][2]["estimated_time_of_merge"],
            ) == first_queued_pr_started_at + datetime.timedelta(
                seconds=3 * median_checks_duration,
            )

    async def test_estimated_time_of_merge_multiple_pr_waiting_batch(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 1, 5, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()
            p5 = await self.create_pr()
            p6 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            await self.create_status(tmp_mq_pr_1["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr_2["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            await self.create_status(tmp_mq_pr_3["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            median_checks_duration = await self._get_median_check_durations()

            await self.add_label(p4["number"], "queue")
            await self.add_label(p5["number"], "queue")
            await self.run_engine()
            await self.wait_for_pull_request("opened")

            # No PR opened for this one since batch_size=2 and speculative_checks=1
            await self.add_label(p6["number"], "queue")
            await self.run_engine()

            # GET /queues
            repository_name = self.RECORD_CONFIG["repository_name"]
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queues",
            )

            assert r.status_code == 200, r.text
            assert len(r.json()["queues"]) == 1
            pull_requests_data = r.json()["queues"][0]["pull_requests"]
            assert len(pull_requests_data) == 3

            first_batch_prs_expected_eta = datetime.datetime.fromisoformat(
                pull_requests_data[0]["mergeability_check"]["started_at"],
            ) + datetime.timedelta(seconds=median_checks_duration)

            # Both PR are in the same batch so they should have the same ETA
            assert (
                datetime.datetime.fromisoformat(
                    pull_requests_data[0]["estimated_time_of_merge"],
                )
                == first_batch_prs_expected_eta
            )

            assert (
                datetime.datetime.fromisoformat(
                    pull_requests_data[1]["estimated_time_of_merge"],
                )
                == first_batch_prs_expected_eta
            )

            # GET /queue/{queue_name}/pull/{pr_number}
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/{p6['number']}",
            )
            # The PR alone cannot have an ETA because it doesn't have a car,
            # with evaluated rules (last_merge_conditions_evaluation), and it doesn't
            # have previous car as a reference either.
            assert r.json()["estimated_time_of_merge"] is None

            # GET /queue/{queue_name}/pull/{pr_number}
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/{p4['number']}",
            )
            assert (
                datetime.datetime.fromisoformat(r.json()["estimated_time_of_merge"])
                == first_batch_prs_expected_eta
            )

    @pytest.mark.timeout(
        os.environ["PYTEST_TIMEOUT"] if settings.TESTING_RECORD else 60,
    )
    async def test_estimated_time_of_merge_multiple_pr_waiting_multiple_batch(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                    "speculative_checks": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 1, 5, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()
            p4 = await self.create_pr()
            p5 = await self.create_pr()
            p6 = await self.create_pr()
            p7 = await self.create_pr()
            p8 = await self.create_pr()
            await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            await self.create_status(tmp_mq_pr_1["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            await self.create_status(tmp_mq_pr_2["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_3 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            await self.create_status(tmp_mq_pr_3["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            median_checks_duration = await self._get_median_check_durations()

            await self.add_label(p4["number"], "queue")
            await self.add_label(p5["number"], "queue")
            await self.run_engine()
            await self.wait_for_pull_request("opened")

            await self.add_label(p6["number"], "queue")
            await self.add_label(p7["number"], "queue")
            await self.run_engine()
            await self.wait_for_pull_request("opened")

            # No PR opened for this one since batch_size=2 and speculative_checks=2
            await self.add_label(p8["number"], "queue")
            await self.run_engine()

            # GET /queues
            repository_name = self.RECORD_CONFIG["repository_name"]
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queues",
            )

            assert len(r.json()["queues"]) == 1

            pull_requests_data = r.json()["queues"][0]["pull_requests"]
            assert len(pull_requests_data) == 5

            first_queued_pr_checks_start = datetime.datetime.fromisoformat(
                pull_requests_data[0]["mergeability_check"]["started_at"],
            )

            # Spec check 1, pr #1 in batch
            assert datetime.datetime.fromisoformat(
                pull_requests_data[0]["estimated_time_of_merge"],
            ) == first_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )
            # Spec check 1, pr #2 in batch
            assert datetime.datetime.fromisoformat(
                pull_requests_data[1]["estimated_time_of_merge"],
            ) == first_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )

            second_queued_pr_checks_start = datetime.datetime.fromisoformat(
                pull_requests_data[2]["mergeability_check"]["started_at"],
            )
            # Spec check 2, pr #1 in batch
            assert datetime.datetime.fromisoformat(
                pull_requests_data[2]["estimated_time_of_merge"],
            ) == second_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )
            # Spec check 2, pr #2 in batch
            assert datetime.datetime.fromisoformat(
                pull_requests_data[3]["estimated_time_of_merge"],
            ) == second_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )
            assert datetime.datetime.fromisoformat(
                pull_requests_data[4]["estimated_time_of_merge"],
            ) == second_queued_pr_checks_start + datetime.timedelta(
                seconds=2 * median_checks_duration,
            )

            # GET /queue/{queue_name}/pull/{pr_number}
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/{p8['number']}",
            )
            # The PR alone cannot have an ETA because it doesn't have a car,
            # with evaluated rules (last_merge_conditions_evaluation), and it doesn't
            # have previous car as a reference either.
            assert r.json()["estimated_time_of_merge"] is None

            # GET /queue/{queue_name}/pull/{pr_number}
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{repository_name}/queue/foo/pull/{p4['number']}",
            )
            assert datetime.datetime.fromisoformat(
                r.json()["estimated_time_of_merge"],
            ) == first_queued_pr_checks_start + datetime.timedelta(
                seconds=median_checks_duration,
            )

    async def test_estimated_time_of_merge_when_queue_is_frozen(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        tmp_mq_pr_1 = await self.wait_for_pull_request("opened")
        await self.create_status(tmp_mq_pr_1["pull_request"])
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_mq_pr_2 = await self.wait_for_pull_request("opened")
        await self.create_status(tmp_mq_pr_2["pull_request"])
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("opened")

        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/foo/freeze",
            json={"reason": "test freeze"},
        )
        assert r.status_code == 200

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
        )

        assert len(r.json()["queues"]) == 1
        assert len(r.json()["queues"][0]["pull_requests"]) == 1
        assert "estimated_time_of_merge" in r.json()["queues"][0]["pull_requests"][0]
        assert (
            r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"] is None
        )

    async def test_estimated_time_of_merge_schedule_condition_non_match(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                        "schedule=MON-FRI 08:00-17:00[UTC]",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        # Friday, 15:00 UTC
        start_date = datetime.datetime(2022, 10, 14, 15, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            # Merge p1
            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")
            await self.create_status(tmp_mq_pr_1["pull_request"])

            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2["number"], "queue")
            await self.run_engine({"delayed-refresh"})

            await self.wait_for_pull_request("opened")

        # Friday, 18:00 UTC
        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            await self.run_engine({"delayed-refresh"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 1
            assert (
                r.json()["queues"][0]["pull_requests"][0].get("estimated_time_of_merge")
                is not None
            )

            # Make sure the eta is after the schedule start
            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"],
            ) == datetime.datetime(2022, 10, 17, 8, tzinfo=datetime.UTC)

        # Monday, 08:00 UTC, at the very start of the schedule
        with time_travel(
            datetime.datetime(2022, 10, 17, 8, tzinfo=datetime.UTC),
            tick=True,
        ):
            await self.run_engine({"delayed-refresh"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 1
            assert (
                r.json()["queues"][0]["pull_requests"][0].get("estimated_time_of_merge")
                is not None
            )

            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"],
            ) == datetime.datetime(2022, 10, 17, 8, tzinfo=datetime.UTC)

    async def test_estimated_time_of_merge_schedule_condition_match(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                        "schedule=MON-FRI 08:00-17:00[UTC]",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        # Friday, 15:00 UTC
        start_date = datetime.datetime(2022, 10, 14, 15, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            # Merge p1
            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")
            await self.create_status(tmp_mq_pr_1["pull_request"])

            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            # Create draft pr for p2 but dont merge it
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 1
            assert (
                r.json()["queues"][0]["pull_requests"][0].get("estimated_time_of_merge")
                is not None
            )

            # ETA should be close to `start_date` since p1 was merged really fast.
            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"],
            ) < (start_date + datetime.timedelta(minutes=5))

    async def test_estimated_time_of_merge_schedule_condition_match_but_eta_dont(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                        "schedule=MON-FRI 08:00-17:00[UTC]",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        # Friday, 15:00 UTC
        start_date = datetime.datetime(2022, 10, 14, 15, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            # Merge p1
            await self.add_label(p1["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(minutes=10), tick=True):
            # This will make the time_to_merge >10 minutes
            await self.create_status(tmp_mq_pr_1["pull_request"])

            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

        # This should make the ETA out of schedule
        # (16:52 + more than 10 minutes > 17:00 on the schedule)
        date_close_to_end_schedule = datetime.datetime(
            2022,
            10,
            14,
            16,
            52,
            tzinfo=datetime.UTC,
        )
        with time_travel(date_close_to_end_schedule, tick=True):
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "opened"})

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
            )

            assert len(r.json()["queues"]) == 1
            assert len(r.json()["queues"][0]["pull_requests"]) == 1
            assert (
                r.json()["queues"][0]["pull_requests"][0].get("estimated_time_of_merge")
                is not None
            )

            # ETA should be close to `start_date` since p1 was merged really fast.
            assert datetime.datetime.fromisoformat(
                r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"],
            ) == datetime.datetime(2022, 10, 17, 8, tzinfo=datetime.UTC)


class TestNewQueueApiEndpoint(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_pr_in_queue_without_partitions(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/new/queue/foo/pull/{p1['number']}",
        )

        train = await self.get_train()
        base_sha = await train.get_base_sha()

        assert r.status_code == 200
        assert r.json() == {
            "number": p1["number"],
            "priority": 2000,
            "effective_priority": 2000,
            "queue_rule": {
                "name": "foo",
                "config": anys.ANY_MAPPING,
            },
            "queued_at": anys.ANY_AWARE_DATETIME_STR,
            "estimated_time_of_merge": None,
            "positions": {partr_config.DEFAULT_PARTITION_NAME: 0},
            "mergeability_checks": {
                partr_config.DEFAULT_PARTITION_NAME: {
                    "check_type": "draft_pr",
                    "checks": [],
                    "conditions_evaluation": anys.ANY_MAPPING,
                    "continuous_integrations_ended_at": None,
                    "continuous_integrations_state": "pending",
                    "ended_at": None,
                    "evaluated_conditions": anys.ANY_STR,
                    "pull_request_number": draft_pr_p1["number"],
                    "started_at": anys.ANY_AWARE_DATETIME_STR,
                    "state": "pending",
                },
            },
            "partition_names": [partr_config.DEFAULT_PARTITION_NAME],
            "summary": {
                "__default__": {
                    "batch_failure": None,
                    "checks_timeout": None,
                    "freeze": None,
                    "pause": None,
                    "title": "The pull request is embarked with "
                    f"{self.main_branch_name} "
                    f"({base_sha[:7]}) for merge and is waiting for checks to finish",
                    "unexpected_changes": None,
                },
            },
        }

    async def test_pr_in_multiple_partitions(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projectA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projectB",
                    "conditions": [
                        "files~=^projB/",
                    ],
                },
            ],
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test.txt": "testB",
            },
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/new/queue/foo/pull/{p1['number']}",
        )

        assert r.status_code == 404
        assert r.json()["detail"] == f"Pull request `{p1['number']}` not found"

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/new/queue/foo/pull/{p1['number']}",
        )

        train_projA = await self.get_train(partr_config.PartitionRuleName("projectA"))
        base_sha_projA = await train_projA.get_base_sha()

        train_projB = await self.get_train(partr_config.PartitionRuleName("projectB"))
        base_sha_projB = await train_projB.get_base_sha()

        assert r.status_code == 200
        assert r.json() == {
            "number": p1["number"],
            "priority": 2000,
            "effective_priority": 2000,
            "queue_rule": {
                "name": "foo",
                "config": anys.ANY_MAPPING,
            },
            "queued_at": anys.ANY_AWARE_DATETIME_STR,
            "estimated_time_of_merge": None,
            "positions": {"projectA": 0, "projectB": 0},
            "mergeability_checks": {
                "projectA": {
                    "check_type": "draft_pr",
                    "checks": [],
                    "conditions_evaluation": anys.ANY_MAPPING,
                    "continuous_integrations_ended_at": None,
                    "continuous_integrations_state": "pending",
                    "ended_at": None,
                    "evaluated_conditions": anys.ANY_STR,
                    "pull_request_number": draft_pr_p1["number"],
                    "started_at": anys.ANY_AWARE_DATETIME_STR,
                    "state": "pending",
                },
                "projectB": {
                    "check_type": "draft_pr",
                    "checks": [],
                    "conditions_evaluation": anys.ANY_MAPPING,
                    "continuous_integrations_ended_at": None,
                    "continuous_integrations_state": "pending",
                    "ended_at": None,
                    "evaluated_conditions": anys.ANY_STR,
                    "pull_request_number": draft_pr_p1["number"],
                    "started_at": anys.ANY_AWARE_DATETIME_STR,
                    "state": "pending",
                },
            },
            "partition_names": ["projectA", "projectB"],
            "summary": {
                "projectA": {
                    "batch_failure": None,
                    "checks_timeout": None,
                    "freeze": None,
                    "pause": None,
                    "title": "The pull request is embarked with "
                    f"{self.main_branch_name} "
                    f"({base_sha_projA[:7]}) for merge and is waiting for checks to finish",
                    "unexpected_changes": None,
                },
                "projectB": {
                    "batch_failure": None,
                    "checks_timeout": None,
                    "freeze": None,
                    "pause": None,
                    "title": "The pull request is embarked with "
                    f"{self.main_branch_name} "
                    f"({base_sha_projB[:7]}) for merge and is waiting for checks to finish",
                    "unexpected_changes": None,
                },
            },
        }
