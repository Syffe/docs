import asyncio

from mergify_engine.tests.functional import base


class TestCommandsDetection(base.FunctionalTestBase):
    async def test_pull_request_locked(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self.setup_repo(test_branches=[stable_branch])
        p = await self.create_pr()
        await self.create_comment_as_admin(
            p["number"], f"@mergifyio backport {stable_branch}"
        )
        await self.client_admin.put(
            f"{p['issue_url']}/lock", json={"lock_reason": "off-topic"}
        )
        await self.wait_for("pull_request", {"action": "locked"})
        await self.run_engine()

    async def test_hidden_comment_not_detected_twice(self) -> None:
        await self.setup_repo()
        p1 = await self.create_pr()
        comment_id = await self.create_comment_as_admin(
            p1["number"], "@mergifyio update"
        )
        await self.run_engine()

        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=p1["number"]
        )
        assert await self.hide_comment(p1["number"], comment_id)

        await self.wait_for("issue_comment", {"action": "edited"}, test_id=p1["number"])
        await self.run_full_engine()

        # NOTE(greesb): We could also just wait_for "issue_comment/created", and check
        # that it times out (with the raised exceptions).
        # It works in RECORD mode but not in non-RECORD mode, because, for some unknown reason,
        # the exception raised in non-RECORD mode is an exception telling it cannot override
        # the cassettes.
        if base.RECORD:
            await asyncio.sleep(15)

        resp = await self.app.post(
            f"/refresh/{p1['base']['repo']['full_name']}/pull/{p1['number']}",
            headers={"X-Hub-Signature": "sha1=" + base.FAKE_HMAC},
        )
        assert resp.status_code == 202, resp.text
        await self.run_full_engine()

        comments = await self.get_issue_comments(p1["number"])
        assert len(comments) == 2

    async def test_deleted_message_not_detected(self) -> None:
        await self.setup_repo()
        p1 = await self.create_pr()
        comment_id = await self.create_comment_as_admin(
            p1["number"], "@mergifyio update"
        )
        await self.run_engine()

        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=p1["number"]
        )
        await self.delete_comment(comment_id)

        await self.wait_for(
            "issue_comment", {"action": "deleted"}, test_id=p1["number"]
        )

        await self.run_engine()

        if base.RECORD:
            await asyncio.sleep(15)

        resp = await self.app.post(
            f"/refresh/{p1['base']['repo']['full_name']}/pull/{p1['number']}",
            headers={"X-Hub-Signature": "sha1=" + base.FAKE_HMAC},
        )
        assert resp.status_code == 202, resp.text
        await self.run_engine()

        comments = await self.get_issue_comments(p1["number"])
        assert len(comments) == 1
