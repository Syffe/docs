import pytest

from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandUpdate(base.FunctionalTestBase):
    async def test_command_update_noop(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        await self.create_comment_as_admin(p["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for_issue_comment(p["number"], "created")
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2, comments
        assert "Nothing to do" in comments[-1]["body"]

    async def test_command_update_pending(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        await self.merge_pull(p["number"], remove_pr_from_events=False)
        await self.create_comment_as_admin(p["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for_issue_comment(p["number"], "created")
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 2, comments
        assert "Nothing to do" in comments[-1]["body"]

    @pytest.mark.subscription(
        subscription.Features.MERGE_QUEUE,
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_update_pr_in_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("opened")

        await self.create_comment_as_admin(p["number"], "@mergifyio update")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(p["number"], "created")
        assert (
            "It's not possible to update this pull request because it is queued for merge"
            in comment["comment"]["body"]
        )
