from mergify_engine.tests.functional import base


class TestCommandUpdate(base.FunctionalTestBase):
    async def test_command_update_noop(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        await self.create_comment_as_admin(p["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"})
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2, comments
        assert "Nothing to do" in comments[-1]["body"]

    async def test_command_update_pending(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.create_comment_as_admin(p["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for("issue_comment", {"action": "created"})
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2, comments
        assert "Nothing to do" in comments[-1]["body"]
