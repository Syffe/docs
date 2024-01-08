from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class TestRequeueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_requeue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "allow_inplace_checks": False,
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        # Queue PR
        p = await self.create_pr()
        await self.create_comment(p["number"], "@mergifyio queue default", as_="admin")
        await self.run_engine()
        queue_comment = await self.wait_for_issue_comment(
            action="created",
            test_id=str(p["number"]),
        )
        assert (
            "🟠 The pull request is the 1st in the queue to be merged"
            in queue_comment["comment"]["body"]
        )

        # Draft PR fails CI, PR is unqueued
        draft_pr = await self.wait_for_pull_request(action="opened")
        await self.create_status(draft_pr["pull_request"], state="failure")
        await self.run_engine()

        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2
        assert (
            "🛑 The pull request has been removed from the queue `default`"
            in comments[-1]["body"]
        )

        # Requeue PR
        await self.create_comment(
            p["number"],
            "@mergifyio requeue default",
            as_="admin",
        )
        await self.run_engine()
        requeue_comment = await self.wait_for_issue_comment(
            action="created",
            test_id=str(p["number"]),
        )
        assert (
            "✅ This pull request will be re-embarked automatically"
            in requeue_comment["comment"]["body"]
        )
        assert (
            "The followup `queue default` command will be automatically executed to re-embark the pull request"
            in requeue_comment["comment"]["body"]
        )

        queue_comment_2 = await self.wait_for_issue_comment(
            action="created",
            test_id=str(p["number"]),
        )
        assert (
            "🟠 The pull request is the 1st in the queue to be merged"
            in queue_comment_2["comment"]["body"]
        )

        draft_pr_2 = await self.wait_for_pull_request("opened")

        await self.create_status(draft_pr_2["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_2["number"])
        await self.wait_for_pull_request("closed", p["number"], merged=True)
