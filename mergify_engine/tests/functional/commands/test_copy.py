import pytest

from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandCopy(base.FunctionalTestBase):
    async def test_command_copy(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        feature_branch = self.get_full_branch_name("feature/one")

        await self.setup_repo(
            yaml.dump({}), test_branches=[stable_branch, feature_branch]
        )
        p = await self.create_pr()

        comment_id = await self.create_comment_as_admin(
            p["number"], f"@mergifyio copy {stable_branch} {feature_branch}"
        )
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"}, test_id=p["number"])
        reactions = [
            r
            async for r in self.client_integration.items(
                f"{self.url_origin}/issues/comments/{comment_id}/reactions",
                api_version="squirrel-girl",
                resource_name="reactions",
                page_limit=5,
            )
        ]
        assert len(reactions) == 1
        assert "+1" == reactions[0]["content"]
        await self.run_engine()

        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch}
        )
        assert 1 == len(pulls_stable)
        pulls_feature = await self.get_pulls(
            params={"state": "all", "base": feature_branch}
        )
        assert 1 == len(pulls_feature)

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/copy"])
        ]
        assert f"refs/heads/mergify/copy/{feature_branch}/pr-{p['number']}" in refs
        assert f"refs/heads/mergify/copy/{stable_branch}/pr-{p['number']}" in refs

        await self.merge_pull(pulls_feature[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.merge_pull(pulls_stable[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/copy"])
        ]
        assert f"refs/heads/mergify/copy/{feature_branch}/pr-{p['number']}" not in refs
        assert f"refs/heads/mergify/copy/{stable_branch}/pr-{p['number']}" not in refs
