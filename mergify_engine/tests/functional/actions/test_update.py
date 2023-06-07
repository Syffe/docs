import pytest

from mergify_engine import context
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestUpdateActionWithoutBot(base.FunctionalTestBase):
    async def test_update_action_without_bot(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "update",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"update": {}},
                },
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1
        await self.add_label(p1["number"], "merge")

        await self.run_engine()
        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 2
        assert (
            commits[-1]["commit"]["author"]["name"]
            == self.RECORD_CONFIG["app_user_login"]
        )
        assert commits[-1]["commit"]["message"].startswith("Merge branch")

    async def test_update_action_on_closed_pr_deleted_branch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "update",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"update": {}},
                },
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}, "delete_head_branch": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        assert p2["commits"] == 1
        await self.add_label(p1["number"], "merge")
        await self.run_engine()

        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 2
        assert (
            commits[-1]["commit"]["author"]["name"]
            == self.RECORD_CONFIG["app_user_login"]
        )
        assert commits[-1]["commit"]["message"].startswith("Merge branch")

        # Now merge p2 so p1 is not up to date
        await self.add_label(p2["number"], "merge")
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p1, [])
        checks = await ctxt.pull_engine_check_runs
        for check in checks:
            assert check["conclusion"] == "success", check


class TestUpdateActionWithBot(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_update_action_with_bot(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "update",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"update": {"bot_account": "mergify-test4"}},
                },
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        assert p2["commits"] == 1
        await self.add_label(p1["number"], "merge")
        await self.run_engine()

        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 2
        assert commits[-1]["commit"]["author"]["name"] == "mergify-test4"
        assert commits[-1]["commit"]["message"].startswith("Merge branch")
