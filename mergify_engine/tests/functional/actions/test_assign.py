from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestAssignAction(base.FunctionalTestBase):
    async def test_assign_with_users(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"users": ["mergify-test1"]}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()

        await self.run_engine()
        p = await self.get_pull(p["number"])
        self.assertEqual(
            sorted(["mergify-test1"]),
            sorted(user["login"] for user in p["assignees"]),
        )

        await self.client_integration.request(
            "DELETE",
            f"{self.repository_ctxt.base_url}/issues/{p['number']}/assignees",
            json={"assignees": ["mergify-test1"]},
        )
        await self.wait_for("pull_request", {"action": "assigned"})
        p = await self.get_pull(p["number"])
        self.assertEqual([], p["assignees"])

        await self.run_engine()
        p = await self.get_pull(p["number"])
        self.assertEqual(
            sorted(["mergify-test1"]),
            sorted(user["login"] for user in p["assignees"]),
        )

    async def test_assign_with_add_users(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"add_users": ["mergify-test1"]}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()

        await self.run_engine()

        pulls = await self.get_pulls(params={"base": self.main_branch_name})
        self.assertEqual(1, len(pulls))
        self.assertEqual(
            sorted(["mergify-test1"]),
            sorted(user["login"] for user in pulls[0]["assignees"]),
        )

    async def test_assign_valid_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"users": ["{{author}}"]}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")

        await self.run_engine()

        pulls = await self.get_pulls(params={"base": self.main_branch_name})
        self.assertEqual(1, len(pulls))
        self.assertEqual(
            sorted(["mergify-test2"]),
            sorted(user["login"] for user in pulls[0]["assignees"]),
        )

    async def test_assign_user_already_assigned(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"add_users": ["mergify-test1"]}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_assignee(p["number"], "mergify-test1")
        await self.run_engine()

        pulls = await self.get_pulls(params={"base": self.main_branch_name})
        self.assertEqual(1, len(pulls))
        self.assertEqual(
            sorted(["mergify-test1"]),
            sorted(user["login"] for user in pulls[0]["assignees"]),
        )

    async def test_remove_assignee(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"assign": {"remove_users": ["mergify-test1"]}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_assignee(p["number"], "mergify-test1")
        await self.run_engine()

        pulls = await self.get_pulls(params={"base": self.main_branch_name})
        self.assertEqual(1, len(pulls))
        self.assertEqual(
            [],
            pulls[0]["assignees"],
        )
