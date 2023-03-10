from unittest import mock

import pytest

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


@pytest.mark.subscription(
    subscription.Features.EVENTLOGS_SHORT,
    subscription.Features.EVENTLOGS_LONG,
    subscription.Features.QUEUE_FREEZE,
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
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.label",
                "metadata": {
                    "added": ["need-review"],
                    "removed": ["auto-merge"],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.assign",
                "metadata": {
                    "added": ["mergify-test1"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.comment",
                "metadata": {"message": "Hello!"},
                "trigger": "Rule: hello",
            },
        ]
        p2_expected_events = [
            {
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "timestamp": mock.ANY,
                "event": "action.label",
                "metadata": {
                    "added": ["need-review"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "timestamp": mock.ANY,
                "event": "action.assign",
                "metadata": {
                    "added": ["mergify-test1"],
                    "removed": [],
                },
                "trigger": "Rule: mergeit",
            },
            {
                "repository": p2["base"]["repo"]["full_name"],
                "pull_request": p2["number"],
                "timestamp": mock.ANY,
                "event": "action.merge",
                "metadata": {"branch": self.main_branch_name},
                "trigger": "Rule: mergeit",
            },
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 10,
            "size": 4,
            "total": 4,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p2['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p2_expected_events,
            "per_page": 10,
            "size": 3,
            "total": 3,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events",
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
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events?per_page=2",
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
                    "conditions": [
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
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
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
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
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
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
        )
        assert r.status_code == 204

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        p1 = await self.get_pull(p1["number"])

        p1_expected_events = [
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.merged",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.leave",
                "metadata": {
                    "reason": f"Pull request #{p1['number']} has been merged automatically at *{p1['merge_commit_sha']}*",
                    "merged": True,
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "seconds_waiting_for_schedule": 0,
                    "seconds_waiting_for_freeze": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "success",
                        "checks_started_at": mock.ANY,
                        "checks_ended_at": mock.ANY,
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.checks_start",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "pending",
                        "checks_ended_at": None,
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.enter",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
        ]

        repo_expected_events = [
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.merged",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.leave",
                "metadata": {
                    "reason": f"Pull request #{p1['number']} has been merged automatically at *{p1['merge_commit_sha']}*",
                    "merged": True,
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "seconds_waiting_for_schedule": 0,
                    "seconds_waiting_for_freeze": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.checks_end",
                "metadata": {
                    "aborted": False,
                    "abort_reason": "",
                    "abort_code": None,
                    "abort_status": "DEFINITIVE",
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "success",
                        "checks_started_at": mock.ANY,
                        "checks_ended_at": mock.ANY,
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "timestamp": mock.ANY,
                "event": "queue.freeze.delete",
                "metadata": {
                    "queue_name": "default",
                    "deleted_by": {
                        "id": 123,
                        "name": "testing application",
                        "type": "application",
                    },
                },
                "trigger": "Delete queue freeze",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "timestamp": mock.ANY,
                "event": "queue.freeze.update",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test updated freeze reason",
                    "cascading": False,
                    "updated_by": {
                        "id": 123,
                        "name": "testing application",
                        "type": "application",
                    },
                },
                "trigger": "Update queue freeze",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": None,
                "timestamp": mock.ANY,
                "event": "queue.freeze.create",
                "metadata": {
                    "queue_name": "default",
                    "reason": "test freeze reason",
                    "cascading": True,
                    "created_by": {
                        "id": 123,
                        "name": "testing application",
                        "type": "application",
                    },
                },
                "trigger": "Create queue freeze",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.checks_start",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                    "speculative_check_pull_request": {
                        "number": p1["number"],
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "pending",
                        "checks_ended_at": None,
                    },
                },
                "trigger": "merge queue internal",
            },
            {
                "repository": p1["base"]["repo"]["full_name"],
                "pull_request": p1["number"],
                "timestamp": mock.ANY,
                "event": "action.queue.enter",
                "metadata": {
                    "queue_name": "default",
                    "branch": self.main_branch_name,
                    "position": 0,
                    "queued_at": mock.ANY,
                },
                "trigger": "Rule: queueit",
            },
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": p1_expected_events,
            "per_page": 10,
            "size": 5,
            "total": 5,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/events",
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
                "repository": self.repository_ctxt.repo["full_name"],
                "pull_request": 123,
                "timestamp": mock.ANY,
                "event": "action.queue.merged",
                "metadata": {},
                "trigger": "gogogo",
            },
        ]

        # We don't send any metadata on purpose
        await signals.send(
            self.repository_ctxt,
            github_types.GitHubPullRequestNumber(123),
            "action.queue.merged",
            signals.EventQueueMergedMetadata({}),
            "gogogo",
        )
        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/123/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": expected_events,
            "per_page": 10,
            "size": 1,
            "total": 1,
        }
