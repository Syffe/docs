from unittest import mock

import pytest

from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(
    subscription.Features.EVENTLOGS_SHORT,
    subscription.Features.EVENTLOGS_LONG,
    subscription.Features.WORKFLOW_AUTOMATION,
)
class TestEditAction(base.FunctionalTestBase):
    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
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

        p_updated = await self.wait_for_pull_request("converted_to_draft")
        assert p_updated["pull_request"]["draft"] is True

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
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
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
        reason="requires GHES 3.2",
    )
    async def test_draft_to_ready_for_review(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Remove Draft from Pull Request",
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

        p_updated = await self.wait_for_pull_request("ready_for_review")
        assert p_updated["pull_request"]["draft"] is False

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.edit",
                    "pull_request": p["number"],
                    "metadata": {"draft": False},
                    "timestamp": mock.ANY,
                    "trigger": "Rule: Remove Draft from Pull Request",
                    "repository": p_updated["pull_request"]["base"]["repo"][
                        "full_name"
                    ],
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
        reason="requires GHES 3.2",
    )
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

        p = await self.get_pull(p["number"])
        assert p["draft"] is True

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
            "total": 0,
        }

    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
        reason="requires GHES 3.2",
    )
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

        p = await self.get_pull(p["number"])
        assert p["draft"] is False

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
            "total": 0,
        }

    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
        reason="requires GHES 3.2",
    )
    async def test_edit_on_closed_pr(self) -> None:
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
        await self.merge_pull(p["number"])
        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [],
            "per_page": 10,
            "size": 0,
            "total": 0,
        }
