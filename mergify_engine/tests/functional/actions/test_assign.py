import pytest

from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestAssignAction(base.FunctionalTestBase):
    async def test_assign_with_users(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"users": ["mergify-test1"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("assigned")
        assert sorted(["mergify-test1"]) == sorted(
            user["login"] for user in p_updated["pull_request"]["assignees"]
        )

        await self.client_integration.request(
            "DELETE",
            f"{self.repository_ctxt.base_url}/issues/{p['number']}/assignees",
            json={"assignees": ["mergify-test1"]},
        )
        p_updated = await self.wait_for_pull_request("unassigned")
        assert p_updated["pull_request"]["assignees"] == []

        await self.run_engine()
        p_updated = await self.wait_for_pull_request("assigned")
        assert sorted(["mergify-test1"]) == sorted(
            user["login"] for user in p_updated["pull_request"]["assignees"]
        )

    async def test_assign_with_add_users(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"add_users": ["mergify-test1"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        p = await self.wait_for_pull_request("assigned")
        assert sorted(["mergify-test1"]) == sorted(
            user["login"] for user in p["pull_request"]["assignees"]
        )

    async def test_assign_valid_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"users": ["{{author}}"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")
        await self.run_engine()

        p = await self.wait_for_pull_request("assigned")

        assert sorted(["mergify-test2"]) == sorted(
            user["login"] for user in p["pull_request"]["assignees"]
        )

    async def test_unassign_valid_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"remove_users": ["{{ assignee[0] }}"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        created_pr = await self.create_pr(as_="fork")
        await self.add_assignee(created_pr["number"], "mergify-test1")

        await self.run_engine()

        unassigned_pr = await self.wait_for_pull_request("unassigned")
        assert not unassigned_pr["pull_request"]["assignees"]

    async def test_assign_user_already_assigned(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"add_users": ["mergify-test1"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_assignee(p["number"], "mergify-test1")
        await self.run_engine()

        p = await self.get_pull(p["number"])
        assert sorted(["mergify-test1"]) == sorted(
            user["login"] for user in p["assignees"]
        )

    async def test_remove_assignee(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"remove_users": ["mergify-test1"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_assignee(p["number"], "mergify-test1")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("unassigned")
        assert p_updated["pull_request"]["assignees"] == []
