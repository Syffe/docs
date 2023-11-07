import pytest

from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCloseAction(base.FunctionalTestBase):
    async def test_close(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "close",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"close": {"message": "WTF?"}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.wait_for_pull_request("closed")

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert comment["comment"]["body"] == "WTF?"

    async def test_close_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "close",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"close": {"message": "Thank you {{author}}"}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.wait_for_pull_request("closed")

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
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
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed", status="completed", conclusion="failure"
        )

        assert (
            "The current Mergify configuration is invalid"
            == check_run["check_run"]["output"]["title"]
        )
        return check_run["check_run"]

    async def test_close_template_syntax_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{",
        )
        assert """Template syntax error @ pull_request_rules → item 0 → actions → close → message → line 1
```
unexpected 'end of template'
```""" == check["output"]["summary"]

    async def test_close_template_attribute_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{hello}}",
        )
        assert """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → close → message
```
Unknown pull request attribute: hello
```""" == check["output"]["summary"]
