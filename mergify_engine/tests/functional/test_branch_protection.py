import typing

from mergify_engine import context
from mergify_engine import github_graphql_types
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils
from mergify_engine.yaml import yaml


class TestBranchProtection(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def setup_repo_with_queue(self, queue_rules: dict[str, typing.Any]) -> None:
        default_queue_rules = {
            "name": "default",
            "merge_conditions": ["status-success=continuous-integration/fake-ci"],
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
        expected_conclusion = "cancelled"
        expected_title = "Configuration not compatible with a branch protection setting"
        expected_summary = (
            "The branch protection setting `Require branches to be up to date before merging` "
            "is not compatible with `batch_size>1` and must be unset."
        )
        await self.assert_check_run(
            ctxt,
            check_name,
            expected_conclusion,
            expected_title,
            expected_summary,
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
            },
        )
        await self.protect_main_branch_with_required_status_checks_strict()

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()
        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p1["number"],
                        merged=True,
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p2["number"],
                        merged=True,
                    ),
                },
            ],
        )

    async def test_draft_pr_with_branch_protection_on_everything(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "#review-requested=0",
                        "#changes-requested-reviews-by=0",
                        "label=ready-to-merge",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=ready-to-merge",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_method": "merge",
                            "update_method": "rebase",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        all_protection = github_graphql_types.CreateGraphqlBranchProtectionRule(
            {
                "allowsDeletions": True,
                "allowsForcePushes": True,
                "dismissesStaleReviews": False,
                "isAdminEnforced": False,
                "pattern": "**/*",
                "requiredStatusCheckContexts": [],
                "requiresApprovingReviews": False,
                "requiresCodeOwnerReviews": False,
                "requiresCommitSignatures": False,
                "requiresConversationResolution": False,
                "requiresLinearHistory": False,
                "requiresStatusChecks": False,
                "requiresStrictStatusChecks": False,
                "restrictsPushes": False,
                "restrictsReviewDismissals": False,
            },
        )
        mergify_protection = github_graphql_types.CreateGraphqlBranchProtectionRule(
            {
                "allowsDeletions": True,
                "allowsForcePushes": True,
                "blocksCreations": False,
                "dismissesStaleReviews": False,
                "isAdminEnforced": False,
                "pattern": "tmp-*/**/*",
                "requiredStatusCheckContexts": [],
                "requiresApprovingReviews": False,
                "requiresCodeOwnerReviews": False,
                "requiresCommitSignatures": False,
                "requiresConversationResolution": False,
                "requiresLinearHistory": False,
                "requiresStatusChecks": False,
                "requiresStrictStatusChecks": False,
                "restrictsPushes": False,
                "restrictsReviewDismissals": False,
            },
        )
        await self.create_branch_protection_rule(mergify_protection)
        # The default queue_branch_prefix is mocked in `mergify_engine.tests.functional.base`
        mergify_protection.update({"pattern": f"{self._testMethodName[:50]}/**/*"})
        await self.create_branch_protection_rule(mergify_protection)
        await self.create_branch_protection_rule(all_protection)

        p1 = await self.create_pr(two_commits=True)

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"], "rebase")

        await self.add_label(p1["number"], "ready-to-merge")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.wait_for_pull_request("closed", draft_pr["number"])
        await self.wait_for_pull_request("closed", p1["number"], merged=True)

    async def test_strict_status_check_with_rebase_update_method_and_no_bot_account(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        await self.protect_main_branch_with_required_status_checks_strict()

        pr = await self.create_pr()
        pr2 = await self.create_pr(as_="fork")

        pr3 = await self.create_pr()
        await self.merge_pull_as_admin(pr3["number"])

        await self.add_label(pr["number"], "queue")
        await self.run_engine()

        # pr should not be merged because created by a bot
        check_run = await self.wait_for_check_run(
            name="Rule: Queue on label (queue)",
            status="completed",
            conclusion="cancelled",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Configuration not compatible with a branch protection setting"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == "The branch protection setting `Require branches to be up to date before merging` is not compatible with `update_method=rebase` if `update_bot_account` isn't set."
        )

        await self.add_label(pr2["number"], "queue")
        await self.run_engine()

        # pr2 should be merged because not created by a bot
        await self.wait_for_pull_request("synchronize", pr2["number"])
        await self.wait_for_pull_request("closed", pr2["number"], merged=True)
        await self.wait_for_push(branch_name=self.main_branch_name)

        # Pull changes for pr4 to be uptodate
        await self.git("pull")
        pr4 = await self.create_pr()

        # Make sure p4, an up to date pr created by a bot, is merged
        ctxt = context.Context(self.repository_ctxt, pr4)
        assert not await ctxt.is_behind

        await self.add_label(pr4["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("closed", pr4["number"], merged=True)
