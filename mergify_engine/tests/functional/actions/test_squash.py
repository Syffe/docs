from mergify_engine import context
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class TestActionSquash(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_squash_several_commits_ok(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"squash": {}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        branch_name = self.get_full_branch_name("pr_squash_test")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name,
        )

        for i in range(3):
            (self.git.repository / f"file{i}").open("wb").close()
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
                "body": """This is a squash_test

# Commit message
Awesome title

Awesome body
""",
            },
        )

        p = await self.wait_for_pull_request("opened")
        assert p["pull_request"]["commits"] == 3

        # do the squash
        await self.run_engine()
        p = await self.wait_for_pull_request("synchronize")
        assert p["pull_request"]["commits"] == 1

        # get the PR
        ctxt = context.Context(self.repository_ctxt, p["pull_request"], [])
        assert (await ctxt.commits)[0].commit_message == "Awesome title\n\nAwesome body"

    async def test_squash_bot_account(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    # NOTE(sileht): wrong case matter to ensure bot_account is case-insensitive
                    "actions": {"squash": {"bot_account": "MERGIFY-TEST1"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        branch_name = self.get_full_branch_name("pr_squash_test")

        await self.git(
            "checkout",
            "--quiet",
            f"origin/{self.main_branch_name}",
            "-b",
            branch_name,
        )

        for i in range(3):
            (self.git.repository / f"file{i}").open("wb").close()
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
                "body": """This is a squash_test

# Commit message
Awesome title

Awesome body
""",
            },
        )

        p = await self.wait_for_pull_request("opened")
        assert p["pull_request"]["commits"] == 3

        # do the squash
        await self.run_engine()
        p = await self.wait_for_pull_request("synchronize")
        assert p["pull_request"]["commits"] == 1

        # get the PR
        ctxt = context.Context(self.repository_ctxt, p["pull_request"], [])
        commits = await ctxt.commits
        assert commits[0].author == "mergify-test1"
        assert commits[0].commit_message == "Awesome title\n\nAwesome body"
