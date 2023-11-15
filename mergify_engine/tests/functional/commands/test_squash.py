import pytest

from mergify_engine import context
from mergify_engine import subscription
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandSquash(base.FunctionalTestBase):
    async def test_squash_several_commits_ok(self) -> None:
        await self.setup_repo()
        branch_name = self.get_full_branch_name("pr_squash_test")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name,
        )

        for i in range(0, 3):
            open(self.git.repository + f"/file{i}", "wb").close()
            await self.git("add", f"file{i}")
            await self.git("commit", "--no-edit", "-m", f"feat(): add file{i+1}")

        await self.git("push", "--quiet", "fork", branch_name)

        # create a PR with several commits to squash
        await self.client_fork.post(
            f"{self.url_origin}/pulls",
            json={
                "base": self.main_branch_name,
                "head": f"mergify-test2:{branch_name}",
                "title": "squash the PR",
                "body": "This is a squash_test",
                "draft": False,
            },
        )

        pr_opened = await self.wait_for_pull_request("opened")
        assert pr_opened["pull_request"]["commits"] == 3
        await self.run_engine()

        # do the squash
        await self.create_comment_as_admin(pr_opened["number"], "@mergifyio squash")
        await self.run_engine()

        pr_updated = await self.wait_for_pull_request(
            "synchronize",
            pr_opened["number"],
        )
        assert pr_updated["pull_request"]["commits"] == 1

    async def test_squash_several_commits_with_one_merge_commit_and_custom_message(
        self,
    ) -> None:
        await self.setup_repo()
        branch_name1 = self.get_full_branch_name("pr_squash_test_b1")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name1,
        )

        open(self.git.repository + "/file0", "wb").close()
        await self.git("add", "file0")
        await self.git("commit", "--no-edit", "-m", "feat(): add file0")

        await self.git("push", "--quiet", "fork", branch_name1)

        branch_name2 = self.get_full_branch_name("pr_squash_test_b2")

        await self.git(
            "checkout",
            "--quiet",
            f"fork/{branch_name1}",
            "-b",
            branch_name2,
        )

        for i in range(1, 4):
            open(self.git.repository + f"/file{i}", "wb").close()
            await self.git("add", f"file{i}")
            await self.git("commit", "--no-edit", "-m", f"feat(): add file{i}")

        await self.git("push", "--quiet", "fork", branch_name2)

        # create a merge between this 2 branches
        await self.client_fork.post(
            f"{self.url_fork}/merges",
            json={
                "owner": "mergify-test2",
                "repo": self.RECORD_CONFIG["repository_name"],
                "base": branch_name1,
                "head": branch_name2,
            },
        )

        # create a new PR between main & fork
        pr = (
            await self.client_fork.post(
                f"{self.url_origin}/pulls",
                json={
                    "base": self.main_branch_name,
                    "head": f"mergify-test2:{branch_name1}",
                    "title": "squash the PR",
                    "body": """This is a squash_test

# Commit message
Awesome title

Awesome body
""",
                    "draft": False,
                },
            )
        ).json()

        await self.wait_for("pull_request", {"action": "opened"})

        commits = await self.get_commits(pr["number"])
        assert len(commits) > 1
        await self.run_engine()

        # do the squash
        await self.create_comment_as_admin(pr["number"], "@mergifyio squash")
        await self.run_engine()

        # wait after the update & get the PR
        await self.wait_for("pull_request", {"action": "synchronize"})

        pr = await self.client_fork.item(f"{self.url_origin}/pulls/{pr['number']}")
        assert pr["commits"] == 1

        ctxt = context.Context(self.repository_ctxt, pr, [])
        assert (await ctxt.commits)[0].commit_message == "Awesome title\n\nAwesome body"

    async def test_squash_several_commits_with_two_merge_commits(self) -> None:
        await self.setup_repo()
        branch_name1 = self.get_full_branch_name("pr_squash_test_b1")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name1,
        )

        open(self.git.repository + "/file0", "wb").close()
        await self.git("add", "file0")
        await self.git("commit", "--no-edit", "-m", "feat(): add file0")

        await self.git("push", "--quiet", "fork", branch_name1)

        branch_name2 = self.get_full_branch_name("pr_squash_test_b2")

        await self.git(
            "checkout",
            "--quiet",
            f"fork/{branch_name1}",
            "-b",
            branch_name2,
        )

        for i in range(1, 4):
            open(self.git.repository + f"/file{i}", "wb").close()
            await self.git("add", f"file{i}")
            await self.git("commit", "--no-edit", "-m", f"feat(): add file{i}")

        await self.git("push", "--quiet", "fork", branch_name2)

        # create a merge between this 2 branches
        await self.client_fork.post(
            f"{self.url_fork}/merges",
            json={
                "owner": "mergify-test2",
                "repo": self.RECORD_CONFIG["repository_name"],
                "base": branch_name1,
                "head": branch_name2,
            },
        )

        branch_name3 = self.get_full_branch_name("pr_squash_test_b3")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name3,
        )

        open(self.git.repository + "/an_other_file", "wb").close()
        await self.git("add", "an_other_file")
        await self.git("commit", "--no-edit", "-m", "feat(): add an other file")

        await self.git("push", "--quiet", "fork", branch_name3)

        # create a PR between this branche n3 & n1
        await self.client_fork.post(
            f"{self.url_fork}/merges",
            json={
                "owner": "mergify-test2",
                "repo": self.RECORD_CONFIG["repository_name"],
                "base": branch_name1,
                "head": branch_name3,
            },
        )

        # create a new PR between main & fork
        pr = (
            await self.client_fork.post(
                f"{self.url_origin}/pulls",
                json={
                    "base": self.main_branch_name,
                    "head": f"mergify-test2:{branch_name1}",
                    "title": "squash the PR",
                    "body": "This is a squash_test",
                    "draft": False,
                },
            )
        ).json()

        await self.wait_for("pull_request", {"action": "opened"})

        commits = await self.get_commits(pr["number"])
        assert len(commits) > 1
        await self.run_engine()

        # do the squash
        await self.create_comment_as_admin(pr["number"], "@mergifyio squash")
        await self.run_engine()

        # wait after the update & get the PR
        await self.wait_for("pull_request", {"action": "synchronize"})

        pr = await self.client_fork.item(f"{self.url_origin}/pulls/{pr['number']}")
        assert pr["commits"] == 1

    async def test_default_squash_restrictions(self) -> None:
        await self.setup_repo()
        branch_name = self.get_full_branch_name("pr_squash_test")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name,
        )

        for i in range(0, 3):
            open(self.git.repository + f"/file{i}", "wb").close()
            await self.git("add", f"file{i}")
            await self.git("commit", "--no-edit", "-m", f"feat(): add file{i+1}")

        await self.git("push", "--quiet", "origin", branch_name)
        await self.wait_for("push", {"ref": f"refs/heads/{branch_name}"})

        # Create a PR with several commits to squash
        pr = (
            await self.client_admin.post(
                f"{self.url_origin}/pulls",
                json={
                    "base": self.main_branch_name,
                    "head": f"mergifyio-testing:{branch_name}",
                    "title": "squash the PR",
                    "body": "This is a squash_test",
                    "draft": False,
                },
            )
        ).json()
        await self.wait_for("pull_request", {"action": "opened"})
        await self.run_engine()

        # First squash by fork client, who can't do it as he is not the author
        # of the PR
        await self.create_command(pr["number"], "@mergifyio squash", as_="fork")
        # Second squash by admin client, the author of the PR
        await self.create_command(pr["number"], "@mergifyio squash", as_="admin")

        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 4
        # Response to the first squash
        assert "Command disallowed due to [command restrictions]" in comments[1]["body"]
        # Response to the second squash
        assert "Pull request squashed successfully" in comments[3]["body"]
