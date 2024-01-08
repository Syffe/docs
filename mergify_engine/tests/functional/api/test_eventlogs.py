import datetime
import typing
from unittest import mock

import anys
import pytest
import sqlalchemy
from sqlalchemy import func

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests import utils as test_utils
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel
from mergify_engine.yaml import yaml


@pytest.mark.subscription(
    subscription.Features.QUEUE_ACTION,
    subscription.Features.QUEUE_FREEZE,
    subscription.Features.MERGE_QUEUE,
    subscription.Features.WORKFLOW_AUTOMATION,
)
class TestEventLogsAction(base.FunctionalTestBase):
    async def test_eventlogs(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "hello",
                    "conditions": [f"base={self.main_branch_name}", "label=auto-merge"],
                    "actions": {
                        "comment": {
                            "message": "Hello!",
                        },
                    },
                },
                {
                    "name": "mergeit",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "assign": {"users": ["mergify-test1"]},
                        "label": {
                            "add": ["need-review"],
                            "remove": ["auto-merge"],
                        },
                        "merge": {},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.add_label(p1["number"], "auto-merge")
        await self.run_engine()

        p1_expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.label",
                "metadata": {
                    "added": ["need-review"],
                    "removed": ["auto-merge"],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.assign",
                "metadata": {
                    "added": ["mergify-test1"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.comment",
                "metadata": {"message": "Hello!"},
                "trigger": "Rule: hello",
            },
        ]
        p2_expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.label",
                "metadata": {
                    "added": ["need-review"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.assign",
                "metadata": {
                    "added": ["mergify-test1"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 10,
            "size": 4,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p2['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p2_expected_events,
            "per_page": 10,
            "size": 3,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p2_expected_events + p1_expected_events,
            "per_page": 10,
            "size": 7,
        }

        # pagination
        r_pagination = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?per_page=2",
        )
        assert r_pagination.status_code == 200
        assert r_pagination.json() == {
            "events": p2_expected_events[:2],
            "per_page": 2,
            "size": 2,
        }

        # first page
        r_first = await self.admin_app.get(
            r_pagination.links["first"]["url"],
        )
        assert r_first.status_code == 200
        assert r_first.json() == {
            "events": p2_expected_events[:2],
            "per_page": 2,
            "size": 2,
        }
        # next page
        r_next = await self.admin_app.get(
            r_pagination.links["next"]["url"],
        )
        assert r_next.status_code == 200
        assert r_next.json() == {
            "events": [p2_expected_events[2], p1_expected_events[0]],
            "per_page": 2,
            "size": 2,
        }
        # next next
        r_next_next = await self.admin_app.get(
            r_next.links["next"]["url"],
        )
        assert r_next_next.status_code == 200
        assert r_next_next.json() == {
            "events": p1_expected_events[1:3],
            "per_page": 2,
            "size": 2,
        }
        # prev
        r_prev = await self.admin_app.get(
            r_next.links["prev"]["url"],
        )
        assert r_prev.status_code == 200
        assert r_prev.json() == {
            "events": p2_expected_events[:2],
            "per_page": 2,
            "size": 2,
        }

        # last
        r_last = await self.admin_app.get(
            r_pagination.links["last"]["url"],
        )
        assert r_last.status_code == 200
        assert r_last.json() == {
            "events": p1_expected_events[-2:],
            "per_page": 2,
            "size": 2,
        }

    async def test_freeze_eventlogs(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queueit",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
            json={"reason": "test freeze reason"},
        )
        assert r.status_code == 200
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "cascading": True,
                },
            ],
        }

        await self.create_status(p1, context="continuous-integration/fake-ci")
        await self.run_engine()

        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
            json={"reason": "test updated freeze reason", "cascading": False},
        )
        assert r.status_code == 200
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test updated freeze reason",
                    "cascading": False,
                },
            ],
        }

        r = await self.admin_app.delete(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
        )
        assert r.status_code == 204

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        p1 = await self.get_pull(p1["number"])

        p1_expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.merged",
                "metadata": {
                    "branch": self.main_branch_name,
                    "partition_names": [partr_config.DEFAULT_PARTITION_NAME],
                    "queue_name": "default",
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.leave",
                "metadata": {
                    "reason": f"Pull request #{p1['number']} has been merged automatically at *{p1['merge_commit_sha']}*",
                    "merged": True,
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "seconds_waiting_for_schedule": 0,
                    "seconds_waiting_for_freeze": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "base_ref": self.main_branch_name,
                "id": anys.ANY_INT,
                "metadata": {
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "queue_name": "default",
                    "running_checks": 0,
                    "size": 0,
                },
                "pull_request": p1["number"],
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "repository": p1["base"]["repo"]["full_name"],
                "trigger": "Rule: queueit",
                "type": "action.queue.change",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "success",
                        "checks_started_at": mock.ANY,
                        "checks_ended_at": mock.ANY,
                        "unsuccessful_checks": [],
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.checks_start",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "start_reason": f"First time checking pull request #{p1['number']}",
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "pending",
                        "checks_started_at": None,
                        "checks_ended_at": None,
                        "unsuccessful_checks": [],
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.enter",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "base_ref": self.main_branch_name,
                "id": anys.ANY_INT,
                "metadata": {
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "queue_name": "default",
                    "running_checks": 0,
                    "size": 1,
                },
                "pull_request": p1["number"],
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "repository": p1["base"]["repo"]["full_name"],
                "trigger": "Rule: queueit",
                "type": "action.queue.change",
            },
        ]

        repo_expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.merged",
                "metadata": {
                    "branch": self.main_branch_name,
                    "partition_names": [partr_config.DEFAULT_PARTITION_NAME],
                    "queue_name": "default",
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.leave",
                "metadata": {
                    "reason": f"Pull request #{p1['number']} has been merged automatically at *{p1['merge_commit_sha']}*",
                    "merged": True,
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "seconds_waiting_for_schedule": 0,
                    "seconds_waiting_for_freeze": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "base_ref": self.main_branch_name,
                "id": anys.ANY_INT,
                "metadata": {
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "queue_name": "default",
                    "running_checks": 0,
                    "size": 0,
                },
                "pull_request": p1["number"],
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "repository": p1["base"]["repo"]["full_name"],
                "trigger": "Rule: queueit",
                "type": "action.queue.change",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "success",
                        "checks_started_at": mock.ANY,
                        "checks_ended_at": mock.ANY,
                        "unsuccessful_checks": [],
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "base_ref": None,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "queue.freeze.delete",
                "metadata": {
                    "queue_name": "default",
                    "deleted_by": {
                        "id": test_utils.ANY_UUID4,
                        "name": "on-premise-app-from-env",
                        "type": "application",
                    },
                },
                "trigger": "Delete queue freeze",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "base_ref": None,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "queue.freeze.update",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test updated freeze reason",
                    "cascading": False,
                    "updated_by": {
                        "id": test_utils.ANY_UUID4,
                        "name": "on-premise-app-from-env",
                        "type": "application",
                    },
                },
                "trigger": "Update queue freeze",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "base_ref": None,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "queue.freeze.create",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test freeze reason",
                    "cascading": True,
                    "created_by": {
                        "id": test_utils.ANY_UUID4,
                        "name": "on-premise-app-from-env",
                        "type": "application",
                    },
                },
                "trigger": "Create queue freeze",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.change",
                "metadata": {
                    "partition_name": "__default__",
                    "running_checks": 1,
                    "queue_name": "default",
                    "size": 1,
                },
                "trigger": "merge queue internal",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.checks_start",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "start_reason": f"First time checking pull request #{p1['number']}",
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "pending",
                        "checks_ended_at": None,
                        "checks_started_at": None,
                        "unsuccessful_checks": [],
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "base_ref": self.main_branch_name,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "type": "action.queue.enter",
                "metadata": {
                    "branch": self.main_branch_name,
                    "position": 0,
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "queue_name": "default",
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "base_ref": self.main_branch_name,
                "id": anys.ANY_INT,
                "metadata": {
                    "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                    "queue_name": "default",
                    "running_checks": 0,
                    "size": 1,
                },
                "pull_request": p1["number"],
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "repository": p1["base"]["repo"]["full_name"],
                "trigger": "Rule: queueit",
                "type": "action.queue.change",
            },
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}&per_page=20",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 20,
            "size": 7,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?per_page=20",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": repo_expected_events,
            "per_page": 20,
            "size": 11,
        }

    async def test_incomplete_eventlogs_metadata(self) -> None:
        await self.setup_repo()
        # Insert will simply fail with an error message
        # NOTE(lecrepont01): We could probably use caplog in functional to test this
        await signals.send(
            self.repository_ctxt,
            github_types.GitHubPullRequestNumber(123),
            github_types.GitHubRefType("some_base_branch_name"),
            "action.queue.merged",
            signals.EventQueueMergedMetadata({}),
            "gogogo",
        )
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request=123",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
        }

    @staticmethod
    def assert_unsuccessful_checks(
        events: typing.Any,
        expected_unsuccessful_checks: list[typing.Any],
    ) -> None:
        for event in events:
            if event["type"] == "action.queue.checks_end":
                assert (
                    event["metadata"]["speculative_check_pull_request"][
                        "unsuccessful_checks"
                    ]
                    == expected_unsuccessful_checks
                )
                break
        else:
            raise KeyError("Event `action.queue.checks_end` not found")

    async def test_unsuccessful_checks_failed_with_batch(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        {
                            "or": [
                                {
                                    "and": [
                                        "status-success=continuous-integration/toto",
                                        "status-success=continuous-integration/tutu",
                                    ],
                                },
                                "status-success=continuous-integration/tata",
                            ],
                        },
                        "status-success=continuous-integration/fake-ci",
                        "status-success=continuous-integration/fake-ci_2",
                        "status-success=continuous-integration/fake-ci_3",
                    ],
                    "speculative_checks": 1,
                    "batch_size": 2,
                    "allow_inplace_checks": True,
                    "merge_method": "squash",
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

        pr_1 = await self.create_pr()
        pr_2 = await self.create_pr()

        await self.add_label(pr_1["number"], "queue")
        await self.add_label(pr_2["number"], "queue")

        await self.run_engine()
        tmp_pull = await self.wait_for_pull_request("opened")
        assert tmp_pull["pull_request"]["number"] not in [
            pr_1["number"],
            pr_2["number"],
        ]

        # make the first PR pass the CIs
        await self.create_status(pr_1, "continuous-integration/fake-ci")
        await self.create_status(pr_1, "continuous-integration/fake-ci_2")
        await self.create_status(pr_1, "continuous-integration/fake-ci_3")
        await self.create_status(pr_1, "continuous-integration/toto")
        await self.create_status(pr_1, "continuous-integration/tata")

        # make the draft PR fails the CIs, so the 2nd PR is notified as failing
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/fake-ci",
            state="failure",
        )
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/toto",
            state="failure",
        )
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/tata",
            state="failure",
        )
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/fake-ci_2",
            state="pending",
        )
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/fake-ci_3",
        )

        # Create another one to check that it's not present in the eventlog even if it
        # not success
        await self.create_status(
            tmp_pull["pull_request"],
            "continuous-integration/fake-ci_4",
            state="failure",
        )

        await self.run_engine()
        await self.wait_for_pull_request("closed")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_1['number']}",
        )

        # assert first PR has no unsuccessful checks
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[],
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_2['number']}",
        )

        # assert second PR has unsuccessful checks reported from the draft PR failing CIs
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/fake-ci",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/toto",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/tata",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is pending",
                    "name": "continuous-integration/fake-ci_2",
                    "state": "pending",
                    "url": anys.ANY_STR,
                },
            ],
        )

    async def test_unsuccessful_checks_failed_with_spec_checks(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        {
                            "or": [
                                {
                                    "and": [
                                        "status-success=continuous-integration/toto",
                                        "status-success=continuous-integration/tutu",
                                    ],
                                },
                                "status-success=continuous-integration/tata",
                            ],
                        },
                        "status-success=continuous-integration/fake-ci",
                        "status-success=continuous-integration/fake-ci_2",
                        "status-success=continuous-integration/fake-ci_3",
                    ],
                    "speculative_checks": 3,
                    "allow_inplace_checks": False,
                    "merge_method": "squash",
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

        pr_1 = await self.create_pr()

        await self.add_label(pr_1["number"], "queue")

        await self.run_engine()
        tmp_pull_1 = await self.wait_for_pull_request("opened")
        assert tmp_pull_1["pull_request"]["number"] != pr_1["number"]

        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/toto",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/tata",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_2",
            state="pending",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_3",
        )
        # Create another one to check that it's not present in the eventlog even if it
        # not success
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_4",
            state="failure",
        )

        await self.run_engine()
        await self.wait_for_pull_request("closed")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_1['number']}",
        )
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/fake-ci",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/toto",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/tata",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is pending",
                    "name": "continuous-integration/fake-ci_2",
                    "state": "pending",
                    "url": anys.ANY_STR,
                },
            ],
        )

    async def test_unsuccessful_checks_failed_with_spec_checks_and_second_pr_failing(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        {
                            "or": [
                                {
                                    "and": [
                                        "status-success=continuous-integration/toto",
                                        "status-success=continuous-integration/tutu",
                                    ],
                                },
                                "status-success=continuous-integration/tata",
                            ],
                        },
                        "status-success=continuous-integration/fake-ci",
                        "status-success=continuous-integration/fake-ci_2",
                        "status-success=continuous-integration/fake-ci_3",
                    ],
                    "speculative_checks": 3,
                    "allow_inplace_checks": True,
                    "merge_method": "squash",
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

        pr_1 = await self.create_pr()
        pr_2 = await self.create_pr()

        await self.add_label(pr_1["number"], "queue")
        await self.add_label(pr_2["number"], "queue")

        await self.run_engine()
        tmp_pull_1 = await self.wait_for_pull_request("opened")
        assert tmp_pull_1["pull_request"]["number"] not in [
            pr_1["number"],
            pr_2["number"],
        ]

        # make the first PR pass the CIs
        await self.create_status(pr_1, "continuous-integration/fake-ci")
        await self.create_status(pr_1, "continuous-integration/fake-ci_2")
        await self.create_status(pr_1, "continuous-integration/fake-ci_3")
        await self.create_status(pr_1, "continuous-integration/toto")
        await self.create_status(pr_1, "continuous-integration/tata")

        # make the draft PR (PR1+PR2) fails the CIs, so the 2nd PR is notified as failing
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/toto",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/tata",
            state="failure",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_2",
            state="pending",
        )
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_3",
        )

        # Create another one to check that it's not present in the eventlog even if it
        # not success
        await self.create_status(
            tmp_pull_1["pull_request"],
            "continuous-integration/fake-ci_4",
            state="failure",
        )

        await self.run_engine()
        await self.wait_for_pull_request("closed")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_1['number']}",
        )

        # assert first PR has no unsuccessful checks
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[],
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_2['number']}",
        )

        # assert second PR has unsuccessful checks reported from the draft PR failing CIs
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/fake-ci",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/toto",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/tata",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is pending",
                    "name": "continuous-integration/fake-ci_2",
                    "state": "pending",
                    "url": anys.ANY_STR,
                },
            ],
        )

    async def test_unsuccessful_checks_failed(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                        "status-success=continuous-integration/fake-ci_2",
                        "status-success=continuous-integration/fake-ci_3",
                    ],
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

        pr = await self.create_pr()

        await self.add_label(pr["number"], "queue")

        await self.run_engine()

        await self.create_status(pr, "continuous-integration/fake-ci", state="failure")
        await self.create_status(
            pr,
            "continuous-integration/fake-ci_2",
            state="pending",
        )
        await self.create_status(pr, "continuous-integration/fake-ci_3")
        # Create another one to check that it's not present in the eventlog even if it
        # not success
        await self.create_status(
            pr,
            "continuous-integration/fake-ci_4",
            state="failure",
        )

        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr['number']}",
        )
        self.assert_unsuccessful_checks(
            events=r.json()["events"],
            expected_unsuccessful_checks=[
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is failure",
                    "name": "continuous-integration/fake-ci",
                    "state": "failure",
                    "url": anys.ANY_STR,
                },
                {
                    "avatar_url": anys.ANY_STR,
                    "description": "The CI is pending",
                    "name": "continuous-integration/fake-ci_2",
                    "state": "pending",
                    "url": anys.ANY_STR,
                },
            ],
        )

    async def test_unsuccessful_checks_timeout(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                        "status-success=continuous-integration/fake-ci_2",
                    ],
                    "checks_timeout": "10 m",
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

        with time_travel(date.utcnow(), tick=True):
            pr = await self.create_pr()

            await self.add_label(pr["number"], "queue")

            await self.run_engine({"delayed-refresh"})

            await self.create_status(pr, "continuous-integration/fake-ci")
            await self.create_status(
                pr,
                "continuous-integration/fake-ci_2",
                state="pending",
            )
            # Create another one to check that it's not present in the eventlog even
            # if it not success
            await self.create_status(
                pr,
                "continuous-integration/fake-ci_3",
                state="pending",
            )

            with time_travel(date.utcnow() + datetime.timedelta(minutes=15), tick=True):
                await self.run_engine({"delayed-refresh"})

                r = await self.admin_app.get(
                    f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr['number']}",
                )
                self.assert_unsuccessful_checks(
                    events=r.json()["events"],
                    expected_unsuccessful_checks=[
                        {
                            "avatar_url": anys.ANY_STR,
                            "description": "The CI is pending",
                            "name": "continuous-integration/fake-ci_2",
                            "state": "pending",
                            "url": anys.ANY_STR,
                        },
                    ],
                )

    async def test_eventlogs_db(self) -> None:
        # NOTE(lecrepont01): this tests the integration of feature events in DB but should evolve in an API test
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "assign": {"users": ["mergify-test1"]},
                        "label": {"add": ["toto"]},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        await self.create_pr()
        await self.run_engine()
        async with database.create_session() as db:
            result = await db.execute(
                sqlalchemy.select(func.count()).select_from(evt_models.Event),
            )
        assert result.scalar() == 2
