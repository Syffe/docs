import yaml

from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.tests.functional import base


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

        p = await self.get_pull(p["number"])
        self.assertEqual("closed", p["state"])
        comments = await self.get_issue_comments(p["number"])
        self.assertEqual("WTF?", comments[-1]["body"])

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

        p = await self.get_pull(p["number"])
        self.assertEqual("closed", p["state"])
        comments = await self.get_issue_comments(p["number"])
        self.assertEqual(f"Thank you {config.BOT_USER_LOGIN}", comments[-1]["body"])

    async def _test_close_template_error(
        self, msg: str
    ) -> github_types.CachedGitHubCheckRun:
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

        p = await self.create_pr()

        await self.run_engine()

        p = await self.get_pull(p["number"])

        ctxt = await context.Context.create(self.repository_ctxt, p, [])

        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert "failure" == checks[0]["conclusion"]
        assert (
            "The current Mergify configuration is invalid"
            == checks[0]["output"]["title"]
        )
        return checks[0]

    async def test_close_template_syntax_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{",
        )
        assert (
            """Template syntax error @ pull_request_rules → item 0 → actions → close → message → line 1
```
unexpected 'end of template'
```"""
            == check["output"]["summary"]
        )

    async def test_close_template_attribute_error(self) -> None:
        check = await self._test_close_template_error(
            msg="Thank you {{hello}}",
        )
        assert (
            """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → close → message
```
Unknown pull request attribute: hello
```"""
            == check["output"]["summary"]
        )
