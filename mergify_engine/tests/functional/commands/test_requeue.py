from mergify_engine import yaml
from mergify_engine.tests.functional import base


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
                }
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        # Queue PR
        p = await self.create_pr()
        await self.create_comment(p["number"], "@mergifyio queue default", as_="admin")
        await self.run_engine()
        queue_comment = await self.wait_for_issue_comment(
            action="created", test_id=str(p["number"])
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
        await self.create_comment(p["number"], "@mergifyio requeue", as_="admin")
        await self.run_engine()
        requeue_comment = await self.wait_for_issue_comment(
            action="created", test_id=str(p["number"])
        )
        assert (
            "✅ The queue state of this pull request has been cleaned. It can be re-embarked automatically"
            in requeue_comment["comment"]["body"]
        )
