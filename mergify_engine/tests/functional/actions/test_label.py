import anys
import pytest

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(
    subscription.Features.WORKFLOW_AUTOMATION,
)
class TestLabelAction(base.FunctionalTestBase):
    async def test_label_basic(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rename label",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "label": {
                            "add": ["unstable", "foobar", "vector"],
                            "remove": ["stable", "what", "remove-me"],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()

        # NOTE(sileht): We create first a label with a wrong case, GitHub
        # label... you can't have a label twice with different case.
        await self.add_label(p["number"], "vEcToR")
        await self.add_label(p["number"], "ReMoVe-Me")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "labeled"})
        p_updated = await self.wait_for_pull_request("unlabeled")
        self.assertEqual(
            sorted(["unstable", "foobar", "vEcToR"]),
            sorted(label["name"] for label in p_updated["pull_request"]["labels"]),
        )

        # Ensure it's idempotent
        await self.remove_label(p["number"], "unstable")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("labeled")
        self.assertEqual(
            sorted(["unstable", "foobar", "vEcToR"]),
            sorted(label["name"] for label in p_updated["pull_request"]["labels"]),
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.label",
                    "metadata": {
                        "added": ["unstable"],
                        "removed": [],
                    },
                    "trigger": "Rule: rename label",
                },
                {
                    "id": anys.ANY_INT,
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.label",
                    "metadata": {
                        "added": ["foobar", "unstable"],
                        "removed": ["remove-me"],
                    },
                    "trigger": "Rule: rename label",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": None,
        }

    async def test_label_empty(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rename label",
                    "conditions": [f"base={self.main_branch_name}", "label=stable"],
                    "actions": {
                        "label": {
                            "add": [],
                            "remove": [],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_label(p["number"], "stable")
        await self.run_engine()

        p = await self.get_pull(p["number"])
        self.assertEqual(
            sorted(["stable"]),
            sorted(label["name"] for label in p["labels"]),
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
            "total": None,
        }

    async def test_label_remove_all(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "delete all labels",
                    "conditions": [f"base={self.main_branch_name}", "label=stable"],
                    "actions": {"label": {"remove_all": True}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_label(p["number"], "stable")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("unlabeled")
        self.assertEqual([], p_updated["pull_request"]["labels"])

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.label",
                    "metadata": {"added": [], "removed": ["stable"]},
                    "trigger": "Rule: delete all labels",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": None,
        }

    async def test_label_toggle(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "toggle labels",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-failure=continuous-integration/fake-ci",
                    ],
                    "actions": {
                        "label": {
                            "toggle": [
                                "CI:fail",
                            ],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.create_status(p, state="failure")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("labeled")
        self.assertEqual(
            ["CI:fail"],
            [label["name"] for label in p_updated["pull_request"]["labels"]],
        )

        await self.create_status(p_updated["pull_request"])
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("unlabeled")
        self.assertEqual([], p_updated["pull_request"]["labels"])

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.label",
                    "metadata": {"added": [], "removed": ["CI:fail"]},
                    "trigger": "Rule: toggle labels",
                },
                {
                    "id": anys.ANY_INT,
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.label",
                    "metadata": {"added": ["CI:fail"], "removed": []},
                    "trigger": "Rule: toggle labels",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": None,
        }

    async def test_label_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "add branch label",
                    "conditions": [],
                    "actions": {
                        "label": {
                            "add": ["branch:{{base}}"],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("labeled")
        self.assertEqual(
            [f"branch:{self.main_branch_name}"],
            [label["name"] for label in p_updated["pull_request"]["labels"]],
        )

    async def _test_label_invalid_template(
        self,
        label: str,
    ) -> github_types.GitHubCheckRun:
        rules = {
            "pull_request_rules": [
                {
                    "name": "add branch label",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "label": {
                            "add": [label],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed",
            status="completed",
            conclusion="action_required",
        )
        assert (
            "The current Mergify configuration is invalid"
            == check_run["check_run"]["output"]["title"]
        )
        return check_run["check_run"]

    async def test_label_invalid_template_syntax_error(self) -> None:
        check_run = await self._test_label_invalid_template("branch:{{")

        assert """In the rule `add branch label`, the action `label` configuration is invalid:
Invalid template in label 'branch:{{'
unexpected 'end of template'
""" == check_run["output"]["summary"]

    async def test_label_invalid_template_attribute_error(self) -> None:
        check_run = await self._test_label_invalid_template("branch:{{test}}")

        assert """In the rule `add branch label`, the action `label` configuration is invalid:
Invalid template in label 'branch:{{test}}'
Unknown pull request attribute: test
""" == check_run["output"]["summary"]
