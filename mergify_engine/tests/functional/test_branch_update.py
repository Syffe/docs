import base64
import typing

from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestBranchUpdatePublic(base.FunctionalTestBase):
    async def test_command_update(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "auto-rebase-on-conflict",
                    "conditions": ["conflict"],
                    "actions": {"comment": {"message": "nothing"}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules), files={"TESTING": "foobar"})
        p1 = await self.create_pr(files={"TESTING2": "foobar"})
        p2 = await self.create_pr(files={"TESTING3": "foobar"})
        await self.merge_pull(p1["number"])

        await self.wait_for("pull_request", {"action": "closed"})

        await self.create_comment_as_admin(p2["number"], "@mergifyio update")
        await self.run_engine()

        p2_updated = await self.wait_for_pull_request("synchronize")
        oldsha = p2["head"]["sha"]
        assert p2_updated["pull_request"]["commits"] == 2
        assert oldsha != p2_updated["pull_request"]["head"]["sha"]

    async def test_command_rebase_ok(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "auto-rebase-on-label",
                    "conditions": ["label=rebase"],
                    "actions": {"comment": {"message": "nothing"}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules), files={"TESTING": "foobar\n"})
        p1 = await self.create_pr(files={"TESTING": "foobar\n\n\np1"})
        p2 = await self.create_pr(files={"TESTING": "p2\n\nfoobar\n"})
        await self.merge_pull(p1["number"])
        await self.wait_for("pull_request", {"action": "closed"})

        await self.create_comment_as_admin(p2["number"], "@mergifyio rebase")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        oldsha = p2["head"]["sha"]
        await self.merge_pull(p2["number"])

        p2_merged = await self.wait_for_pull_request("closed")
        assert oldsha != p2_merged["pull_request"]["head"]["sha"]
        f = typing.cast(
            github_types.GitHubContentFile,
            await self.client_integration.item(
                f"{self.url_origin}/contents/TESTING?ref={self.main_branch_name}"
            ),
        )
        data = base64.b64decode(bytearray(f["content"], "utf-8"))
        assert data == b"p2\n\nfoobar\n\n\np1"


# FIXME(sileht): This is not yet possible, due to GH restriction ...
# class TestBranchUpdatePrivate(TestBranchUpdatePublic):
#    REPO_NAME = "functional-testing-repo-private"
#    FORK_PERSONAL_TOKEN = config.ORG_USER_PERSONAL_TOKEN
#    SUBSCRIPTION_ACTIVE = True
