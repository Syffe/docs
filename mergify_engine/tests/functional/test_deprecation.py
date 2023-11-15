import yaml

from mergify_engine import constants
from mergify_engine import context
from mergify_engine.tests.functional import base


class TestDeprecation(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_deprecated_multiple_rules_with_same_name(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "comment": {"message": "WTF?", "bot_account": "{{ body }}"},
                    },
                },
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "comment": {
                            "message": "Ola quetal?",
                            "bot_account": "{{ body }}",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            summary["output"]["summary"]
            == """pull_request_rules names must be unique, found `comment` twice for dictionary value @ pull_request_rules"""
        )
