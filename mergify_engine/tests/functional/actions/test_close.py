import pytest

from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCloseAction(base.FunctionalTestBase):
    async def test_close(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "close",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"close": {"message": "WTF?"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.wait_for_pull_request("closed")

        comment = await self.wait_for_issue_comment(p["number"], "created")
        assert comment["comment"]["body"] == "WTF?"

    async def test_close_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "close",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"close": {"message": "Thank you {{author}}"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.wait_for_pull_request("closed")

        comment = await self.wait_for_issue_comment(p["number"], "created")
        assert (
            comment["comment"]["body"]
            == f"Thank you {self.RECORD_CONFIG['app_user_login']}"
        )

    async def _test_close_template_error(self, msg: str) -> github_types.GitHubCheckRun:
        rules = {
            "pull_request_rules": [
                {
                    "name": "close",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"close": {"message": msg}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed",
            status="completed",
            conclusion="failure",
        )

        assert (
            check_run["check_run"]["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        return check_run["check_run"]

    async def test_close_template_syntax_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{",
        )
        assert (
            check["output"]["summary"]
            == """Template syntax error @ pull_request_rules → item 0 → actions → close → message → line 1
```
unexpected 'end of template'
```"""
        )

    async def test_close_template_attribute_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{hello}}",
        )
        assert (
            check["output"]["summary"]
            == """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → close → message
```
Unknown pull request attribute: hello
```"""
        )
