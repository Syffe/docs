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
            ],
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
        await self.wait_for_push(branch_name=self.main_branch_name)
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
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        assert p2["commits"] == 1
        await self.add_label(p1["number"], "merge")
        await self.run_engine()

        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]

        await self.wait_for_push(branch_name=self.main_branch_name)
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

    async def test_update_action_on_conflict(self) -> None:
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
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p1, p2 = await self.create_prs_with_conflicts()

        await self.add_label(p1["number"], "merge")
        await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=p1["number"],
            merged=True,
        )

        # FIXME(charly): this won't work for now. GitHub reports the pull request
        # as mergeable, the update action runs and fails as there is a conflict.
        # It happens event though we retrieve fresh pull request data from
        # GitHub (without cache). MRGFY-2315

        # check_runs = await self.get_check_runs(p2)
        # for check_run in check_runs:
        #     assert check_run["conclusion"] == "success"

        await self.create_comment(p2["number"], "@mergifyio update", as_="admin")
        await self.run_engine()
        comment = await self.wait_for_issue_comment(
            test_id=str(p2["number"]),
            action="created",
        )
        assert "Nothing to do" in comment["comment"]["body"]


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
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        assert p2["commits"] == 1
        await self.add_label(p1["number"], "merge")
        await self.run_engine()

        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]

        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 2
        assert commits[-1]["commit"]["author"]["name"] == "mergify-test4"
        assert commits[-1]["commit"]["message"].startswith("Merge branch")
