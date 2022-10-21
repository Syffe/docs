from mergify_engine import context
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestActionSquash(base.FunctionalTestBase):
    async def test_squash_several_commits_ok(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "assign",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"squash": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        branch_name = self.get_full_branch_name("pr_squash_test")

        await self.git(
            "checkout", "--quiet", f"origin/{self.main_branch_name}", "-b", branch_name
        )

        for i in range(0, 3):
            open(self.git.repository + f"/file{i}", "wb").close()
            await self.git("add", f"file{i}")
            await self.git("commit", "--no-edit", "-m", f"feat(): add file{i+1}")

        await self.git("push", "--quiet", "fork", branch_name)

        # create a PR with several commits to squash
        pr = (
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
        ).json()

        await self.wait_for("pull_request", {"action": "opened"})

        commits = await self.get_commits(pr["number"])
        assert len(commits) > 1

        # do the squash
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        # get the PR
        pr = await self.client_fork.item(f"{self.url_origin}/pulls/{pr['number']}")
        assert pr["commits"] == 1

        ctxt = context.Context(self.repository_ctxt, pr, [])
        assert (await ctxt.commits)[0].commit_message == "Awesome title\n\nAwesome body"
