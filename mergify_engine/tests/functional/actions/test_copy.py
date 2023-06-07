import pytest

from mergify_engine import context
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class CopyActionTestBase(base.FunctionalTestBase):
    async def test_copy_with_no_matching_branch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "copy",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {
                        "copy": {
                            "regexes": ["whatever-that-will-never-ever-match"],
                        }
                    },
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr(two_commits=True)
        await self.add_label(p["number"], "backport-#3.1")
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p, [])
        check = await ctxt.get_engine_check_run("Rule: copy (copy)")
        assert check is not None
        assert check["conclusion"] == "failure"
        assert check["output"]["title"] == "No copy have been created"
        assert check["output"]["summary"] == "No destination branches found"
