import pytest

from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandBackport(base.FunctionalTestBase):
    async def test_command_backport(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        feature_branch = self.get_full_branch_name("feature/one")
        await self.setup_repo(test_branches=[stable_branch, feature_branch])
        p = await self.create_pr()

        await self.run_engine()
        await self.create_comment_as_admin(
            p["number"], f"@mergifyio backport {stable_branch} {feature_branch}"
        )
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"}, test_id=p["number"])
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2, comments
        assert "Waiting for conditions" in comments[-1]["body"]
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "edited"}, test_id=p["number"])

        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch}
        )
        assert 1 == len(pulls_stable)
        pulls_feature = await self.get_pulls(
            params={"state": "all", "base": feature_branch}
        )
        assert 1 == len(pulls_feature)
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2
        assert "Backports have been created" in comments[-1]["body"]
        reactions = [
            r
            async for r in self.client_integration.items(
                f"{self.url_origin}/issues/comments/{comments[0]['id']}/reactions",
                api_version="squirrel-girl",
                resource_name="reactions",
                page_limit=5,
            )
        ]
        assert len(reactions) == 1
        assert "+1" == reactions[0]["content"]

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/bp"])
        ]
        assert f"refs/heads/mergify/bp/{feature_branch}/pr-{p['number']}" in refs
        assert f"refs/heads/mergify/bp/{stable_branch}/pr-{p['number']}" in refs

        await self.merge_pull(pulls_feature[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.merge_pull(pulls_stable[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/bp"])
        ]
        assert f"refs/heads/mergify/bp/{feature_branch}/pr-{p['number']}" not in refs
        assert f"refs/heads/mergify/bp/{stable_branch}/pr-{p['number']}" not in refs

    async def test_command_backport_with_defaults(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        feature_branch = self.get_full_branch_name("feature/one")
        rules = {
            "defaults": {
                "actions": {"backport": {"branches": [stable_branch, feature_branch]}},
            },
        }

        await self.setup_repo(
            yaml.dump(rules), test_branches=[stable_branch, feature_branch]
        )
        p = await self.create_pr()
        await self.create_comment_as_admin(
            p["number"], f"@mergifyio backport {stable_branch} {feature_branch}"
        )
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})

        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"}, test_id=p["number"])

        pulls = await self.get_pulls(params={"state": "all", "base": stable_branch})
        assert 1 == len(pulls)
        pulls = await self.get_pulls(params={"state": "all", "base": feature_branch})
        assert 1 == len(pulls)
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2
        reactions = [
            r
            async for r in self.client_integration.items(
                f"{self.url_origin}/issues/comments/{comments[0]['id']}/reactions",
                api_version="squirrel-girl",
                resource_name="reactions",
                page_limit=5,
            )
        ]
        assert len(reactions) == 1
        assert "+1" == reactions[0]["content"]

    async def test_command_backport_without_config(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        feature_branch = self.get_full_branch_name("feature/one")
        await self.setup_repo(test_branches=[stable_branch, feature_branch])
        p = await self.create_pr()

        await self.create_comment_as_admin(
            p["number"], f"@mergifyio backport {stable_branch} {feature_branch}"
        )
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"}, test_id=p["number"])

        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "edited"}, test_id=p["number"])

        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch}
        )
        assert 1 == len(pulls_stable)
        pulls_feature = await self.get_pulls(
            params={"state": "all", "base": feature_branch}
        )
        assert 1 == len(pulls_feature)
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2
        reactions = [
            r
            async for r in self.client_integration.items(
                f"{self.url_origin}/issues/comments/{comments[0]['id']}/reactions",
                api_version="squirrel-girl",
                resource_name="reactions",
                page_limit=5,
            )
        ]
        assert len(reactions) == 1
        assert "+1" == reactions[0]["content"]

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/bp"])
        ]
        assert f"refs/heads/mergify/bp/{feature_branch}/pr-{p['number']}" in refs
        assert f"refs/heads/mergify/bp/{stable_branch}/pr-{p['number']}" in refs

        await self.merge_pull(pulls_feature[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.merge_pull(pulls_stable[0]["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(self.url_origin, ["mergify/bp"])
        ]
        assert f"refs/heads/mergify/bp/{feature_branch}/pr-{p['number']}" not in refs
        assert f"refs/heads/mergify/bp/{stable_branch}/pr-{p['number']}" not in refs

        # Ensure summary have been posted
        ctxt = await self.repository_ctxt.get_pull_request_context(p["number"], p)
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1

    async def test_command_backport_conflicts(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self.setup_repo(test_branches=[stable_branch])

        # add the coming conflict to stable
        await self.push_file("conflicts", "conflicts incoming", stable_branch)

        p = await self.create_pr(files={"conflicts": "ohoh"})
        await self.run_engine()
        await self.create_comment_as_admin(
            p["number"], f"@mergifyio backport {stable_branch}"
        )

        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert "Waiting for conditions to match" in comment["comment"]["body"]

        # triggers backport
        await self.merge_pull(p["number"])
        await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()
        comment_1 = await self.wait_for_issue_comment(str(p["number"]), "edited")
        assert "in progress" in comment_1["comment"]["body"]

        comment_2 = await self.wait_for_issue_comment(str(p["number"]), "edited")
        assert "but encountered conflict" in comment_2["comment"]["body"]
