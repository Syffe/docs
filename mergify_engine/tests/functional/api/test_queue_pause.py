import typing
from unittest import mock

from first import first
import httpx

from mergify_engine import context
from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.queue import merge_train
from mergify_engine.queue import pause
from mergify_engine.tests.functional import base


class TestQueuePause(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def _create_queue_pause(
        self,
        pause_payload: dict[str, typing.Any] | None,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/pause",
            json=pause_payload,
        )
        assert r.status_code == expected_status_code
        return r

    async def _delete_queue_pause(
        self,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.delete(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/pause",
        )
        assert r.status_code == expected_status_code
        return r

    async def _get_queue_pause(
        self,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/pause",
        )
        assert r.status_code == expected_status_code
        return r

    async def test_request_error_create_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))

        r = await self._create_queue_pause(pause_payload=None, expected_status_code=422)
        assert r.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["body"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        }

        r = await self._create_queue_pause(
            pause_payload={"false_key": "test pause reason"},
            expected_status_code=422,
        )
        assert r.json() == {
            "detail": [
                {
                    "input": {"false_key": "test pause reason"},
                    "loc": ["body", "reason"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        }

        r = await self._create_queue_pause(
            pause_payload={"reason": "too long" * 100},
            expected_status_code=422,
        )
        assert r.json() == {
            "detail": [
                {
                    "input": "too long" * 100,
                    "loc": ["body", "reason"],
                    "msg": "String should have at most 255 characters",
                    "type": "string_too_long",
                    "ctx": {"max_length": 255},
                },
            ],
        }

    async def test_queue_pause_create_and_delete_resume_merge(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_closed = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue-urgent")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_closed["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
            [p2["number"]],
        )
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }
        await self.run_engine()
        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"]],
        )

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
            in check["output"]["summary"]
        )

        p2 = await self.get_pull(p2["number"])
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        p3 = await self.create_pr()
        await self.add_label(p3["number"], "queue-urgent")
        await self.run_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"], p3["number"]],
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )
        assert "is queued for merge" in check["output"]["summary"]

        r = await self._delete_queue_pause(expected_status_code=204)

        queue_pause_data_default = await pause.QueuePause.get(
            self.repository_ctxt,
        )
        assert queue_pause_data_default is None

        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 3rd in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

    async def test_create_queue_pause_no_inplace(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "allow_inplace_checks": False,
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
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

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_closed = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue-urgent")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue-low")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_closed["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
            [p2["number"], p3["number"]],
        )
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 3rd in the queue to be merged"
        )

        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }
        await self.run_engine()
        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"], p3["number"]],
        )

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
            in check["output"]["summary"]
        )

        p2 = await self.get_pull(p2["number"])
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        p3 = await self.get_pull(p3["number"])
        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        p4 = await self.create_pr()
        await self.add_label(p4["number"], "queue-urgent")
        await self.run_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"], p3["number"], p4["number"]],
        )

        check = first(
            await context.Context(self.repository_ctxt, p4).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )
        assert "is queued for merge" in check["output"]["summary"]

    async def test_create_queue_pause_inplace(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_closed = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue-urgent")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue-low")
        await self.run_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_closed["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
            [p2["number"], p3["number"]],
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 3rd in the queue to be merged"
        )

        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }
        await self.run_engine()
        q = await self.get_train()

        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"], p3["number"]],
        )

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
            in check["output"]["summary"]
        )

        p2 = await self.get_pull(p2["number"])
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        p3 = await self.get_pull(p3["number"])
        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        p4 = await self.create_pr()
        await self.add_label(p4["number"], "queue-urgent")
        await self.run_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [],
            [p1["number"], p2["number"], p3["number"], p4["number"]],
        )

        check = first(
            await context.Context(self.repository_ctxt, p4).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )
        assert "is queued for merge" in check["output"]["summary"]

    async def test_update_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))

        pause_payload = {"reason": "test pause reason"}
        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

        queue_pause_data_default = await pause.QueuePause.get(self.repository_ctxt)

        assert queue_pause_data_default is not None
        assert queue_pause_data_default.reason == "test pause reason"

        r = await self._create_queue_pause(
            pause_payload=pause_payload,
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

        pause_payload = {"reason": "new test pause reason"}
        r = await self._create_queue_pause(
            pause_payload=pause_payload,
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "new test pause reason",
                },
            ],
        }

        pause_payload = {"reason": ""}
        r = await self._create_queue_pause(pause_payload=pause_payload)
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "No pause reason was specified.",
                },
            ],
        }

    async def test_request_error_delete_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))

        r = await self._delete_queue_pause(expected_status_code=404)
        assert r.json() == {
            "detail": "The merge queue is not currently paused on this repository",
        }

    async def test_delete_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))

        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

        queue_pause_data_default = await pause.QueuePause.get(self.repository_ctxt)
        assert queue_pause_data_default
        assert queue_pause_data_default.reason == "test pause reason"

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [], [p1["number"]])

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )

        r = await self._delete_queue_pause(expected_status_code=204)

        queue_pause_data_default = await pause.QueuePause.get(
            self.repository_ctxt,
        )
        assert queue_pause_data_default is None

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check
        assert (
            "The pull request is the 1st in the queue to be merged"
            in check["output"]["title"]
        )

        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check
        assert (
            "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
            not in check["output"]["summary"]
        )

    async def test_request_error_get_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))

        r = await self._get_queue_pause(expected_status_code=404)
        assert r.json() == {
            "detail": "The merge queue is not currently paused on this repository",
        }

    async def test_get_queue_pause(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "merge_conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
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

        await self.setup_repo(yaml.dump(rules))
        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

        queue_pause_data_default = await pause.QueuePause.get(self.repository_ctxt)
        assert queue_pause_data_default
        assert queue_pause_data_default.reason == "test pause reason"

        r = await self._get_queue_pause()
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

    async def test_queue_pause_with_pr_just_about_to_merge(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        r = await self._create_queue_pause(
            pause_payload={"reason": "test pause reason"},
        )
        assert r.json() == {
            "queue_pause": [
                {
                    "pause_date": mock.ANY,
                    "reason": "test pause reason",
                },
            ],
        }

        p1 = await self.create_pr()
        # Everything is ready, but queue is frozen
        await self.create_status(p1, context="continuous-integration/slow-ci")
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [], [p1["number"]])

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "⏸️ Checks are suspended, the merge queue is currently paused on this repository, for the following reason: `test pause reason` ⏸️"
        )
