from mergify_engine import context
from mergify_engine import yaml
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

        p = await self.create_pr()
        pr_initial_sha = p["head"]["sha"]

        await self.git("checkout", self.main_branch_name)

        open(self.git.repository + "/an_other_file", "wb").close()
        await self.git("add", "an_other_file")

        await self.git("commit", "--no-edit", "-m", "commit ahead")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        p = await self.get_pull(p["number"])

        final_sha = p["head"]["sha"]

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

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1
        await self.add_label(p1["number"], "merge")

        await self.run_engine()
        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 1
        # Now merge p2 so p1 is not up to date
        await self.add_label(p2["number"], "merge")
        await self.run_engine()
        ctxt = await context.Context.create(self.repository_ctxt, p1, [])
        checks = await ctxt.pull_engine_check_runs
        for check in checks:
            assert check["conclusion"] == "success", check

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

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.add_label(p1["number"], "merge")
        await self.run_engine()
        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]
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
        await self.wait_for("pull_request", {"action": "synchronize"})

        p2_rebased = await self.get_pull(p2["number"])
        assert p2_updated["head"]["sha"] != p2_rebased["head"]["sha"]

        ctxt = await context.Context.create(self.repository_ctxt, p2, [])
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

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

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

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

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

        pr_fixup = await self.create_pr_with_autosquash_commit("squash")

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
            "squash", commit_body="blablatest", autosquash_commit_body="blabla2test2"
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
            "fixup=amend", autosquash_commit_body="test123"
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
            "fixup=reword", autosquash_commit_body="test123"
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
