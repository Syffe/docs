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
class TestEditAction(base.FunctionalTestBase):
    @pytest.mark.skipif(
        not config.GITHUB_URL.startswith("https://github.com"),
        reason="requires GHES 3.2",
    )
    async def test_pr_to_draft_edit(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "convert Pull Request to Draft",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "edit": {"draft": True},
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        assert p["draft"] is False
        await self.run_engine()

        pulls = await self.get_pulls()
        self.assertEqual(1, len(pulls))

        p = await self.get_pull(p["number"])
        assert p["draft"] is True

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
                    "event": "action.edit",
                    "pull_request": p["number"],
                    "metadata": {"draft": True},
                    "timestamp": mock.ANY,
                    "trigger": "Rule: convert Pull Request to Draft",
                    "repository": p["base"]["repo"]["full_name"],
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    async def test_draft_to_ready_for_review(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "convert Pull Request to Draft",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "edit": {"draft": False},
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(draft=True)
        assert p["draft"] is True
        await self.run_engine()

        pulls = await self.get_pulls()
        self.assertEqual(1, len(pulls))

        p = await self.get_pull(p["number"])
        assert p["draft"] is False

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
                    "event": "action.edit",
                    "pull_request": p["number"],
                    "metadata": {"draft": False},
                    "timestamp": mock.ANY,
                    "trigger": "Rule: convert Pull Request to Draft",
                    "repository": p["base"]["repo"]["full_name"],
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    @pytest.mark.subscription(
        subscription.Features.EVENTLOGS_SHORT,
        subscription.Features.EVENTLOGS_LONG,
    )
    async def test_draft_already_converted(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "convert Pull Request to Draft",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "edit": {"draft": True},
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(draft=True)
        assert p["draft"] is True
        await self.run_engine()

        pulls = await self.get_pulls()
        self.assertEqual(1, len(pulls))

        p = await self.get_pull(p["number"])
        assert p["draft"] is True

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

    async def test_ready_for_review_already_converted(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "convert Pull Request to Draft",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "edit": {"draft": False},
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(draft=False)
        assert p["draft"] is False
        await self.run_engine()

        pulls = await self.get_pulls()
        self.assertEqual(1, len(pulls))

        p = await self.get_pull(p["number"])
        assert p["draft"] is False

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
