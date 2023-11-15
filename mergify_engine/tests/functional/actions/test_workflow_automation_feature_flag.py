import pytest

from mergify_engine import constants
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestWorkflowAutomationFeatureFlag(base.FunctionalTestBase):
    async def test_pull_request_rules_without_feature(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Comment on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"comment": {"message": "Hello there"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            conclusion="action_required",
            name=constants.SUMMARY_NAME,
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Cannot use the pull request workflow automation"
        )

        p_comments = await self.get_issue_comments(p["number"])
        assert len(p_comments) == 0

    @pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
    async def test_pull_request_rules_with_feature(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Comment on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"comment": {"message": "Hello there"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert comment["comment"]["body"] == "Hello there"
