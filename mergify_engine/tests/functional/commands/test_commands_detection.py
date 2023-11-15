import asyncio

from mergify_engine import refresher
from mergify_engine import settings
from mergify_engine.tests.functional import base


class TestCommandsDetection(base.FunctionalTestBase):
    async def test_pull_request_locked(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self.setup_repo(test_branches=[stable_branch])
        p = await self.create_pr()
        await self.create_comment_as_admin(
            p["number"],
            f"@mergifyio backport {stable_branch}",
        )
        await self.client_admin.put(
            f"{p['issue_url']}/lock",
            json={"lock_reason": "off-topic"},
        )
        await self.wait_for("pull_request", {"action": "locked"})
        await self.run_engine()

    async def test_hidden_comment_not_detected_twice(self) -> None:
        await self.setup_repo()
        p1 = await self.create_pr()
        comment_id = await self.create_command(
            p1["number"],
            "@mergifyio update",
            as_="admin",
        )
        assert await self.hide_comment(p1["number"], comment_id)
        await self.run_full_engine()

        # NOTE(greesb): We could also just wait_for "issue_comment/created", and check
        # that it times out (with the raised exceptions).
        # It works in RECORD mode but not in non-RECORD mode, because, for some unknown reason,
        # the exception raised in non-RECORD mode is an exception telling it cannot override
        # the cassettes.
        if settings.TESTING_RECORD:
            await asyncio.sleep(15)

        await refresher.send_pull_refresh(
            self.redis_links.stream,
            self.repository_ctxt.repo,
            "user",
            p1["number"],
            "test",
        )
        await self.run_full_engine()

        comments = await self.get_issue_comments(p1["number"])
        assert len(comments) == 2

    async def test_deleted_message_not_detected(self) -> None:
        await self.setup_repo()
        p1 = await self.create_pr()
        comment_id = await self.create_comment_as_admin(
            p1["number"],
            "@mergifyio update",
        )
        await self.run_engine()

        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )
        await self.delete_comment(comment_id)

        await self.wait_for(
            "issue_comment",
            {"action": "deleted"},
            test_id=p1["number"],
        )

        await self.run_engine()

        if settings.TESTING_RECORD:
            await asyncio.sleep(15)

        await refresher.send_pull_refresh(
            self.redis_links.stream,
            self.repository_ctxt.repo,
            "user",
            p1["number"],
            "test",
        )
        await self.run_engine()

        comments = await self.get_issue_comments(p1["number"])
        assert len(comments) == 1
