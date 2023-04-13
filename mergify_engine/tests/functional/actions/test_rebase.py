import pytest

from mergify_engine import context
from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


class TestRebaseAction(base.FunctionalTestBase):
    async def test_rebase_ok(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"rebase": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(as_="admin")
        pr_initial_sha = p["head"]["sha"]

        await self.git("checkout", self.main_branch_name)

        open(self.git.repository + "/an_other_file", "wb").close()
        await self.git("add", "an_other_file")

        await self.git("commit", "--no-edit", "-m", "commit ahead")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        await self.run_engine()
        p_updated = await self.wait_for_pull_request("synchronize")
        final_sha = p_updated["pull_request"]["head"]["sha"]

        self.assertNotEqual(pr_initial_sha, final_sha)

    async def test_rebase_on_closed_pr_deleted_branch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"rebase": {}},
                },
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}, "delete_head_branch": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin")
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1
        await self.add_label(p1["number"], "merge")

        await self.run_engine()
        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]

        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1
        # Now merge p2 so p1 is not up to date
        await self.add_label(p2["number"], "merge")
        await self.run_engine()

        await self.wait_for_check_run(conclusion="success")

    async def test_rebase_on_uptodate_pr_branch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {}},
                },
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}, "delete_head_branch": {}},
                },
                {
                    "name": "update",
                    "conditions": [f"base={self.main_branch_name}", "label=update"],
                    "actions": {"update": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin")
        await self.add_label(p1["number"], "merge")
        await self.run_engine()

        p1_updated = await self.wait_for_pull_request("closed")
        assert p1_updated["pull_request"]["merged"]
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1

        # Now update p2 with a merge from the base branch
        await self.add_label(p2["number"], "update")
        await self.run_engine()
        await self.remove_label(p2["number"], "update")
        p2_updated = await self.get_pull(p2["number"])

        # Now rebase p2
        await self.add_label(p2["number"], "rebase")
        await self.run_engine()
        p2_rebased = await self.wait_for_pull_request("synchronize")
        assert p2_updated["head"]["sha"] != p2_rebased["pull_request"]["head"]["sha"]

        ctxt = context.Context(self.repository_ctxt, p2, [])
        checks = await ctxt.pull_engine_check_runs
        for check in checks:
            assert check["conclusion"] == "success", check

    async def test_rebase_autosquash_false(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": False}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup", as_="admin")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 2

    async def test_rebase_autosquash_fixup_commit(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup", as_="admin")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

    async def test_rebase_autosquash_squash_commit(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit("squash", as_="admin")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

    async def test_rebase_autosquash_squash_commit_with_message(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit(
            "squash",
            commit_body="blablatest",
            autosquash_commit_body="blabla2test2",
            as_="admin",
        )

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1
        assert "blablatest" in fixup_commits[0]["commit"]["message"]
        assert "blabla2test2" in fixup_commits[0]["commit"]["message"]

    async def test_rebase_autosquash_fixup_amend(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit(
            "fixup=amend", autosquash_commit_body="test123", as_="admin"
        )

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

        assert "\n\ntest123" in fixup_commits[0]["commit"]["message"]

    async def test_rebase_autosquash_fixup_reword(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit(
            "fixup=reword", autosquash_commit_body="test123", as_="admin"
        )

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

        assert "\n\ntest123" in fixup_commits[0]["commit"]["message"]

    @pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
    async def test_rebase_forced_with_autosquash_and_squashable_commits(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "rebase",
                    "conditions": [f"base={self.main_branch_name}", "label=rebase"],
                    "actions": {"rebase": {"autosquash": True}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup", as_="admin")
        await self.add_label(pr_fixup["number"], "rebase")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

    async def test_rebase_forced_with_autosquash_and_no_squashable_commits(
        self,
    ) -> None:
        await self.setup_repo()

        pr = await self.create_pr(as_="admin", two_commits=True)
        await self.create_comment_as_admin(pr["number"], "@mergifyio rebase")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert "Nothing to do for rebase action" in comment["comment"]["body"]
