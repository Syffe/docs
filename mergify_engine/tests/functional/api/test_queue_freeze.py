import typing
from unittest import mock

from first import first
import httpx

from mergify_engine import config
from mergify_engine import context
from mergify_engine import yaml
from mergify_engine.queue import freeze
from mergify_engine.tests.functional import base


class TestQueueFreeze(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def _create_queue_freeze(
        self,
        queue_name: str,
        freeze_payload: dict[str, typing.Any] | None,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.app.put(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
            json=freeze_payload,
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == expected_status_code
        return r

    async def _delete_queue_freeze(
        self,
        queue_name: str,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.app.delete(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == expected_status_code
        return r

    async def _get_queue_freeze(
        self,
        queue_name: str,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/{queue_name}/freeze",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == expected_status_code
        return r

    async def _get_all_queue_freeze(
        self,
        expected_status_code: int = 200,
    ) -> httpx.Response:
        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/freezes",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == expected_status_code
        return r

    async def test_request_error_create_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        r = await self._create_queue_freeze(
            queue_name="false_queue_name", freeze_payload=None, expected_status_code=422
        )
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ]
        }

        r = await self._create_queue_freeze(
            queue_name="false_queue_name",
            freeze_payload={"false_key": "test freeze reason"},
            expected_status_code=422,
        )
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body", "reason"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ]
        }

        r = await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={"reason": "too long" * 100},
            expected_status_code=422,
        )
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body", "reason"],
                    "msg": "ensure this value has at most 255 characters",
                    "type": "value_error.any_str.max_length",
                    "ctx": {"limit_value": 255},
                }
            ]
        }

        r = await self._create_queue_freeze(
            queue_name="false_queue_name",
            freeze_payload={"reason": "test freeze reason"},
            expected_status_code=404,
        )
        assert r.json() == {"detail": 'The queue "false_queue_name" does not exist.'}

    async def test_create_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        r = await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={"reason": "test freeze reason"},
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        p2 = await self.create_pr()
        p3 = await self.create_pr()

        await self.add_label(p2["number"], "queue-urgent")
        await self.add_label(p3["number"], "queue-low")
        await self.run_engine()

        check_run_p2 = await self.wait_for_check_run(
            name="Rule: Merge priority high (queue)",
            status="in_progress",
        )
        assert check_run_p2["check_run"]["pull_requests"][0]["number"] == p2["number"]
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check_run_p3 = await self.wait_for_check_run(
            name="Rule: Merge low (queue)",
            status="in_progress",
        )
        assert check_run_p3["check_run"]["pull_requests"][0]["number"] == p3["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p3["check_run"]["output"]["title"]
        )

        # merge p2
        await self.create_status(p2, context="continuous-integration/fast-ci")
        await self.run_engine()

        p2_closed = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_closed["pull_request"]["merged"]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        check_run_p2 = await self.wait_for_check_run(
            name="Rule: Merge priority high (queue)",
            status="completed",
            conclusion="success",
        )
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request has been merged automatically"
        )

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check["output"]["title"]
        )

    async def test_update_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        queue_name = "default"
        freeze_payload = {"reason": "test freeze reason"}
        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload=freeze_payload,
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )

        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"

        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload=freeze_payload,
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        freeze_payload = {"reason": "new test freeze reason"}
        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload=freeze_payload,
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": queue_name,
                    "reason": "new test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        freeze_payload = {"reason": ""}
        r = await self._create_queue_freeze(
            queue_name=queue_name, freeze_payload=freeze_payload
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": queue_name,
                    "reason": "No freeze reason was specified.",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

    async def test_request_error_delete_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        queue_name = "false_queue_name"
        r = await self._delete_queue_freeze(
            queue_name=queue_name, expected_status_code=404
        )
        assert r.json() == {
            "detail": f'The queue "{queue_name}" does not exist or is not currently frozen.'
        }

        queue_name = "default"
        r = await self._delete_queue_freeze(
            queue_name=queue_name, expected_status_code=404
        )
        assert r.json() == {
            "detail": f'The queue "{queue_name}" does not exist or is not currently frozen.'
        }

    async def test_delete_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        queue_name = "default"
        r = await self._create_queue_freeze(
            queue_name=queue_name, freeze_payload={"reason": "test freeze reason"}
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, queue_name
        )
        assert queue_freeze_data_default
        assert queue_freeze_data_default.reason == "test freeze reason"

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["title"]
        )

        check_run_p1 = await self.wait_for_check_run(
            name="Queue: Embarked in merge train",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["summary"]
        )

        r = await self._delete_queue_freeze(
            queue_name=queue_name, expected_status_code=204
        )

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, queue_name
        )
        assert queue_freeze_data_default is None

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
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            not in check["output"]["summary"]
        )

    async def test_request_error_get_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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
        r = await self._get_queue_freeze(
            queue_name="false_queue_name", expected_status_code=404
        )
        assert r.json() == {
            "detail": 'The queue "false_queue_name" does not exist or is not currently frozen.'
        }

        r = await self._get_queue_freeze(queue_name="urgent", expected_status_code=404)
        assert r.json() == {
            "detail": 'The queue "urgent" does not exist or is not currently frozen.'
        }

    async def test_get_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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
        queue_name = "default"
        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload={"reason": "test freeze reason"},
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_default
        assert queue_freeze_data_default.reason == "test freeze reason"

        r = await self._get_queue_freeze(queue_name=queue_name)
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

    async def test_get_list_queue_freeze(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        freeze_payload = {"reason": "test freeze reason"}
        r = await self._create_queue_freeze(
            queue_name="default", freeze_payload=freeze_payload
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        r = await self._create_queue_freeze(
            queue_name="low-priority", freeze_payload=freeze_payload
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "low-priority",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        r = await self._get_all_queue_freeze()
        json_data = r.json()
        list_queue_name = [qf["name"] for qf in json_data["queue_freezes"]]
        assert "default" in list_queue_name
        assert "low-priority" in list_queue_name

        await self._delete_queue_freeze(queue_name="default", expected_status_code=204)

        r = await self._get_all_queue_freeze()
        assert r.json() == {
            "queue_freezes": [
                {
                    "name": "low-priority",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "freeze_date": mock.ANY,
                    "cascading": True,
                },
            ],
        }

        await self._delete_queue_freeze(
            queue_name="low-priority", expected_status_code=204
        )
        r = await self._get_all_queue_freeze()
        assert r.json() == {
            "queue_freezes": [],
        }

    async def test_queue_freeze_with_pr_just_about_to_merge(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [],
                },
                {
                    "name": "default",
                    "conditions": [
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
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={"reason": "test freeze reason"},
        )

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        # Everything is ready, but queue is frozen
        await self.create_status(p1, context="continuous-integration/slow-ci")
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["title"]
        )

        check_run_p1 = await self.wait_for_check_run(
            name="Queue: Embarked in merge train",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["summary"]
        )

        # ensure p2 got queued before p1 and merged
        await self.add_label(p2["number"], "queue-urgent")
        await self.run_engine()

        p2_closed = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_closed["pull_request"]["merged"]

    async def test_queue_freeze_priority_with_empty_frozen_default_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/default-ci",
                    ],
                },
                {
                    "name": "lowprio",
                    "conditions": [
                        "status-success=continuous-integration/low-ci",
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
                    "name": "Merge lowprio",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=low-queue",
                    ],
                    "actions": {"queue": {"name": "lowprio"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={"reason": "test freeze reason"},
        )

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # Everything is ready, but queue default is frozen
        await self.create_status(p1, context="continuous-integration/low-ci")
        await self.add_label(p1["number"], "low-queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge lowprio (queue)",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["title"]
        )

        check_run_p1 = await self.wait_for_check_run(
            name="Queue: Embarked in merge train",
            status="in_progress",
        )
        assert check_run_p1["check_run"]["pull_requests"][0]["number"] == p1["number"]
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["check_run"]["output"]["summary"]
        )

        # ensure p2 got queued before p1 and merged
        await self.add_label(p2["number"], "queue-urgent")
        await self.run_engine()

        p2_closed = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_closed["pull_request"]["merged"]

    async def test_create_queue_freeze_without_cascading_effect(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        r = await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={
                "reason": "test freeze reason",
                "cascading": False,
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": False,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"
        assert queue_freeze_data_default.cascading is False

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        p2 = await self.create_pr()
        p3 = await self.create_pr()

        await self.add_label(p2["number"], "queue-urgent")
        await self.add_label(p3["number"], "queue-low")
        await self.run_engine()

        check_run_p2 = await self.wait_for_check_run(
            name="Rule: Merge priority high (queue)",
            status="in_progress",
        )
        assert check_run_p2["check_run"]["pull_requests"][0]["number"] == p2["number"]
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check_run_p3 = await self.wait_for_check_run(
            name="Rule: Merge low (queue)",
            status="in_progress",
        )
        assert check_run_p3["check_run"]["pull_requests"][0]["number"] == p3["number"]
        assert (
            check_run_p3["check_run"]["output"]["title"]
            == "The pull request is the 3rd in the queue to be merged"
        )

        # merge p2
        await self.create_status(p2, context="continuous-integration/fast-ci")
        await self.run_engine()

        p2_closed = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_closed["pull_request"]["merged"]

        check_run_p2 = await self.wait_for_check_run(
            name="Rule: Merge priority high (queue)",
            status="completed",
            conclusion="success",
        )
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request has been merged automatically"
        )

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        check_p3 = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check_p3
        assert (
            check_p3["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        # wait for p3 to be rebased
        p3_waiting = await self.wait_for_pull_request(
            "synchronize", pr_number=p3["number"]
        )
        await self.run_engine()

        # merge p3
        await self.create_status(
            p3_waiting["pull_request"], context="continuous-integration/slow-ci"
        )
        await self.run_engine()

        p3_closed = await self.wait_for_pull_request(
            "closed", pr_number=p3_waiting["pull_request"]["number"]
        )
        assert p3_closed["pull_request"]["merged"]

        check_run_p3 = await self.wait_for_check_run(
            name="Rule: Merge low (queue)",
            status="completed",
            conclusion="success",
        )
        assert (
            check_run_p3["check_run"]["output"]["title"]
            == "The pull request has been merged automatically"
        )

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

    async def test_update_freeze_cascading_effect(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        queue_name = "default"
        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload={
                "reason": "test freeze reason",
                "cascading": False,
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": False,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )

        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"
        assert queue_freeze_data_default.cascading is False

        r = await self._create_queue_freeze(
            queue_name=queue_name,
            freeze_payload={
                "reason": "new test freeze reason",
                "cascading": True,
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": queue_name,
                    "reason": "new test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

    async def test_create_queue_freeze_all_freeze_types_together(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue-low")
        await self.run_engine()

        r = await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={
                "reason": "test freeze reason",
                "cascading": False,
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": False,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"
        assert queue_freeze_data_default.cascading is False

        r = await self._create_queue_freeze(
            queue_name="urgent",
            freeze_payload={
                "reason": "urgent test freeze reason",
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "urgent",
                    "reason": "urgent test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_urgent = await freeze.QueueFreeze.get(
            self.repository_ctxt, "urgent"
        )
        assert queue_freeze_data_urgent is not None
        assert queue_freeze_data_urgent.reason == "urgent test freeze reason"
        assert queue_freeze_data_urgent.cascading

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        ctxt = context.Context(self.repository_ctxt, p2)
        check_run_p2 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check_run_p2
        assert (
            "The merge is currently blocked by the freeze of the queue `urgent`, for the following reason: `urgent test freeze reason`"
            in check_run_p2["output"]["title"]
        )

        check_run_p2 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p2 is None

        await self._delete_queue_freeze(queue_name="urgent", expected_status_code=204)

        queue_freeze_data_urgent = await freeze.QueueFreeze.get(
            self.repository_ctxt, "urgent"
        )
        assert queue_freeze_data_urgent is None

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        ctxt = context.Context(self.repository_ctxt, p2)
        check_run_p2 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check_run_p2
        assert (
            "The pull request is the 1st in the queue to be merged"
            in check_run_p2["output"]["title"]
        )

        check_run_p2 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p2
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            not in check_run_p2["output"]["summary"]
        )

        # merge p2
        await self.create_status(p2, context="continuous-integration/slow-ci")
        await self.run_engine()

        p2_closed = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_closed["pull_request"]["merged"]

        check = await self.wait_for_check_run(
            name="Rule: Merge low (queue)",
            status="completed",
            conclusion="success",
        )
        assert (
            check["check_run"]["output"]["title"]
            == "The pull request has been merged automatically"
        )

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

    async def test_cascading_freeze_queue_summary_update(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
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

        await self.add_label(p1["number"], "queue-low")
        await self.run_engine()

        r = await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={
                "reason": "test freeze reason",
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "default",
                    "reason": "test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_default = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_default is not None
        assert queue_freeze_data_default.reason == "test freeze reason"
        assert queue_freeze_data_default.cascading

        r = await self._create_queue_freeze(
            queue_name="urgent",
            freeze_payload={
                "reason": "urgent test freeze reason",
            },
        )
        assert r.json() == {
            "queue_freezes": [
                {
                    "freeze_date": mock.ANY,
                    "name": "urgent",
                    "reason": "urgent test freeze reason",
                    "application_name": "testing application",
                    "application_id": 123,
                    "cascading": True,
                }
            ],
        }

        queue_freeze_data_urgent = await freeze.QueueFreeze.get(
            self.repository_ctxt, "urgent"
        )
        assert queue_freeze_data_urgent is not None
        assert queue_freeze_data_urgent.reason == "urgent test freeze reason"
        assert queue_freeze_data_urgent.cascading

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `default`, for the following reason: `test freeze reason`"
            in check_run_p1["output"]["summary"]
        )

        await self._delete_queue_freeze(queue_name="default", expected_status_code=204)

        queue_freeze_data_urgent = await freeze.QueueFreeze.get(
            self.repository_ctxt, "default"
        )
        assert queue_freeze_data_urgent is None

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1)
        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge low (queue)",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `urgent`, for the following reason: `urgent test freeze reason`"
            in check_run_p1["output"]["title"]
        )

        check_run_p1 = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check_run_p1
        assert (
            "The merge is currently blocked by the freeze of the queue `urgent`, for the following reason: `urgent test freeze reason`"
            in check_run_p1["output"]["summary"]
        )
