from datetime import datetime
from datetime import timedelta
import typing
from unittest import mock

import anys
from freezegun import freeze_time
import pytest
import sqlalchemy
from sqlalchemy import func

from mergify_engine import database
from mergify_engine import events as evt_utils
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.models import events as evt_models
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base


@pytest.mark.usefixtures("enable_events_db_ingestion")
@pytest.mark.subscription(
    subscription.Features.QUEUE_ACTION,
    subscription.Features.EVENTLOGS_SHORT,
    subscription.Features.EVENTLOGS_LONG,
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
            ]
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.label",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.assign",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.merge",
                "type": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.comment",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.label",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.assign",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.merge",
                "type": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 10,
            "size": 4,
            "total": 4,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p2['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p2_expected_events,
            "per_page": 10,
            "size": 3,
            "total": 3,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p2_expected_events + p1_expected_events,
            "per_page": 10,
            "size": 7,
            "total": 7,
        }

        # pagination
        r_pagination = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events?per_page=2",
        )
        assert r_pagination.status_code == 200
        assert r_pagination.json() == {
            "events": p2_expected_events[:2],
            "per_page": 2,
            "size": 2,
            "total": 7,
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
            "total": 7,
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
            "total": 7,
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
            "total": 7,
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
            "total": 7,
        }

        # last
        r_last = await self.admin_app.get(
            r_pagination.links["last"]["url"],
        )
        assert r_last.status_code == 200
        assert r_last.json() == {
            "events": [p1_expected_events[3]],
            "per_page": 2,
            "size": 1,
            "total": 7,
        }

    async def test_freeze_eventlogs(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
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
                }
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
                }
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.merged",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.leave",
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
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.checks_end",
                "type": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "unqueue_code": None,
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.checks_start",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.enter",
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
        ]

        repo_expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.merged",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.leave",
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
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "queue.freeze.delete",
                "type": "queue.freeze.delete",
                "metadata": {
                    "queue_name": "default",
                    "deleted_by": {
                        "id": 0,
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "queue.freeze.update",
                "type": "queue.freeze.update",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test updated freeze reason",
                    "cascading": False,
                    "updated_by": {
                        "id": 0,
                        "name": "on-premise-app-from-env",
                        "type": "application",
                    },
                },
                "trigger": "Update queue freeze",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.checks_end",
                "type": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "unqueue_code": None,
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "queue.freeze.create",
                "type": "queue.freeze.create",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test freeze reason",
                    "cascading": True,
                    "created_by": {
                        "id": 0,
                        "name": "on-premise-app-from-env",
                        "type": "application",
                    },
                },
                "trigger": "Create queue freeze",
            },
            {
                "id": anys.ANY_INT,
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.checks_start",
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
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.enter",
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
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 10,
            "size": 5,
            "total": 5,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": repo_expected_events,
            "per_page": 10,
            "size": 8,
            "total": 8,
        }

    async def test_incomplete_eventlogs_metadata(self) -> None:
        await self.setup_repo()

        expected_events = [
            {
                "id": anys.ANY_INT,
                "repository": self.repository_ctxt.repo["full_name"],
                "pull_request": 123,
                "timestamp": anys.ANY_AWARE_DATETIME_STR,
                "received_at": anys.ANY_AWARE_DATETIME_STR,
                "event": "action.queue.merged",
                "type": "action.queue.merged",
                "metadata": {},
                "trigger": "gogogo",
            },
        ]

        # We don't send any metadata on purpose
        with mock.patch.object(evt_utils, "insert", return_value=None):
            # NOTE(lecrepont01): An incomplete event should not be inserted in database, insert errors should be handled
            # in the future by watching the error logs. Mock the insert to avoid teardown failure via logger_checker.
            await signals.send(
                self.repository_ctxt,
                github_types.GitHubPullRequestNumber(123),
                "action.queue.merged",
                signals.EventQueueMergedMetadata({}),
                "gogogo",
            )
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/123/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": expected_events,
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    @staticmethod
    def assert_unsuccessful_checks(
        events: typing.Any,
        expected_unsuccessful_checks: list[typing.Any],
    ) -> None:
        for event in events:
            if event["event"] == "action.queue.checks_end":
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
                                    ]
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
            tmp_pull["pull_request"], "continuous-integration/toto", state="failure"
        )
        await self.create_status(
            tmp_pull["pull_request"], "continuous-integration/tata", state="failure"
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
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_1['number']}/events",
        )

        # assert first PR has no unsuccessful checks
        self.assert_unsuccessful_checks(
            events=r.json()["events"], expected_unsuccessful_checks=[]
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_2['number']}/events",
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
                                    ]
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
            tmp_pull_1["pull_request"], "continuous-integration/toto", state="failure"
        )
        await self.create_status(
            tmp_pull_1["pull_request"], "continuous-integration/tata", state="failure"
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
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_1['number']}/events",
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
                                    ]
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
            tmp_pull_1["pull_request"], "continuous-integration/toto", state="failure"
        )
        await self.create_status(
            tmp_pull_1["pull_request"], "continuous-integration/tata", state="failure"
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
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_1['number']}/events",
        )

        # assert first PR has no unsuccessful checks
        self.assert_unsuccessful_checks(
            events=r.json()["events"], expected_unsuccessful_checks=[]
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_2['number']}/events",
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
        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()

        await self.add_label(pr["number"], "queue")

        await self.run_engine()

        await self.create_status(pr, "continuous-integration/fake-ci", state="failure")
        await self.create_status(
            pr, "continuous-integration/fake-ci_2", state="pending"
        )
        await self.create_status(pr, "continuous-integration/fake-ci_3")
        # Create another one to check that it's not present in the eventlog even if it
        # not success
        await self.create_status(
            pr, "continuous-integration/fake-ci_4", state="failure"
        )

        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr['number']}/events",
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
        await self.setup_repo(yaml.dump(rules))

        with freeze_time(datetime.now(), tick=True):
            pr = await self.create_pr()

            await self.add_label(pr["number"], "queue")

            await self.run_full_engine()

            await self.create_status(pr, "continuous-integration/fake-ci")
            await self.create_status(
                pr, "continuous-integration/fake-ci_2", state="pending"
            )
            # Create another one to check that it's not present in the eventlog even
            # if it not success
            await self.create_status(
                pr, "continuous-integration/fake-ci_3", state="pending"
            )

            with freeze_time(datetime.now() + timedelta(minutes=15), tick=True):
                await self.run_full_engine()

                r = await self.admin_app.get(
                    f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr['number']}/events",
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
                        }
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
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        await self.create_pr()
        await self.run_engine()
        async with database.create_session() as db:
            result = await db.execute(
                sqlalchemy.select(func.count()).select_from(evt_models.Event)
            )
        assert result.scalar() == 2
