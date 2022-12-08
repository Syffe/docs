import typing

from mergify_engine import context
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestBranchProtection(base.FunctionalTestBase):

    SUBSCRIPTION_ACTIVE = True

    async def setup_repo_with_queue(self, queue_rules: dict[str, typing.Any]) -> None:
        default_queue_rules = {
            "name": "default",
            "conditions": ["status-success=continuous-integration/fake-ci"],
            "speculative_checks": 1,
            "allow_inplace_checks": True,
            "batch_size": 1,
        }
        default_queue_rules.update(queue_rules)
        rules = {
            "queue_rules": [default_queue_rules],
            "pull_request_rules": [
                {
                    "name": "Merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

    async def protect_main_branch_with_required_status_checks_strict(self) -> None:
        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

    async def test_required_status_checks_strict_incompatibility_with_batch_size(
        self,
    ) -> None:
        await self.setup_repo_with_queue(queue_rules={"batch_size": 2})
        await self.protect_main_branch_with_required_status_checks_strict()

        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p)
        check_name = "Rule: Merge (queue)"
        expected_conclusion = "failure"
        expected_title = "Configuration not compatible with a branch protection setting"
        expected_summary = (
            "The branch protection setting `Require branches to be up to date before merging` "
            "is not compatible with `batch_size>1` and must be unset."
        )
        await self.assert_check_run(
            ctxt, check_name, expected_conclusion, expected_title, expected_summary
        )

    async def test_required_status_checks_strict_compatibility_with_batch_size_and_queue_branch_merge_method(
        self,
    ) -> None:
        await self.setup_repo_with_queue(
            queue_rules={
                "allow_inplace_checks": False,
                "batch_size": 3,
                "speculative_checks": 5,
                "batch_max_wait_time": "0 s",
                "queue_branch_merge_method": "fast-forward",
            }
        )
        await self.protect_main_branch_with_required_status_checks_strict()

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        for _ in (p1, p2):
            p_merged = await self.wait_for_pull_request("closed")
            assert p_merged["pull_request"]["number"] in (p1["number"], p2["number"])
            assert p_merged["pull_request"]["merged"]
