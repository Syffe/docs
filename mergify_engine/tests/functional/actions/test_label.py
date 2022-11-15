from unittest import mock

import pytest

from mergify_engine import config
from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


@pytest.mark.subscription(
    subscription.Features.EVENTLOGS_SHORT,
    subscription.Features.EVENTLOGS_LONG,
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
                        }
                    },
                }
            ]
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

        # Ensure it's idempotant
        await self.remove_label(p["number"], "unstable")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("labeled")
        self.assertEqual(
            sorted(["unstable", "foobar", "vEcToR"]),
            sorted(label["name"] for label in p_updated["pull_request"]["labels"]),
        )

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "timestamp": mock.ANY,
                    "event": "action.label",
                    "metadata": {
                        "added": ["unstable"],
                        "removed": [],
                    },
                    "trigger": "Rule: rename label",
                },
                {
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "timestamp": mock.ANY,
                    "event": "action.label",
                    "metadata": {
                        "added": ["foobar", "unstable"],
                        "removed": ["remove-me"],
                    },
                    "trigger": "Rule: rename label",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": 2,
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
                        }
                    },
                }
            ]
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

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
            "total": 0,
        }

    async def test_label_remove_all(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "delete all labels",
                    "conditions": [f"base={self.main_branch_name}", "label=stable"],
                    "actions": {"label": {"remove_all": True}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_label(p["number"], "stable")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("unlabeled")
        self.assertEqual([], p_updated["pull_request"]["labels"])

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "timestamp": mock.ANY,
                    "event": "action.label",
                    "metadata": {"added": [], "removed": ["stable"]},
                    "trigger": "Rule: delete all labels",
                }
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
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
                            ]
                        }
                    },
                }
            ]
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

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "timestamp": mock.ANY,
                    "event": "action.label",
                    "metadata": {"added": [], "removed": ["CI:fail"]},
                    "trigger": "Rule: toggle labels",
                },
                {
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                    "pull_request": p_updated["number"],
                    "timestamp": mock.ANY,
                    "event": "action.label",
                    "metadata": {"added": ["CI:fail"], "removed": []},
                    "trigger": "Rule: toggle labels",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": 2,
        }
