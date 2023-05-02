import typing
from unittest import mock

import httpx

from mergify_engine import context
from mergify_engine import github_graphql_types
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.clients import http
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

    async def test_draft_pr_with_branch_protection_on_everything(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "#review-requested=0",
                        "#changes-requested-reviews-by=0",
                        "label=ready-to-merge",
                    ],
                }
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
                            "method": "merge",
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
                "requireLastPushApproval": False,
                "requiredDeploymentEnvironments": [],
                "requiredStatusCheckContexts": [],
                "requiresApprovingReviews": False,
                "requiresCodeOwnerReviews": False,
                "requiresCommitSignatures": False,
                "requiresConversationResolution": False,
                "requiresDeployments": False,
                "requiresLinearHistory": False,
                "requiresStatusChecks": False,
                "requiresStrictStatusChecks": False,
                "restrictsPushes": False,
                "restrictsReviewDismissals": False,
            }
        )
        mergify_protection = github_graphql_types.CreateGraphqlBranchProtectionRule(
            {
                "allowsDeletions": True,
                "allowsForcePushes": True,
                "blocksCreations": False,
                "dismissesStaleReviews": False,
                "isAdminEnforced": False,
                "pattern": "tmp-*/**/*",
                "requireLastPushApproval": False,
                "requiredDeploymentEnvironments": [],
                "requiredStatusCheckContexts": [],
                "requiresApprovingReviews": False,
                "requiresCodeOwnerReviews": False,
                "requiresCommitSignatures": False,
                "requiresConversationResolution": False,
                "requiresDeployments": False,
                "requiresLinearHistory": False,
                "requiresStatusChecks": False,
                "requiresStrictStatusChecks": False,
                "restrictsPushes": False,
                "restrictsReviewDismissals": False,
            }
        )
        await self.create_branch_protection_rule(mergify_protection)
        # The default queue_branch_prefix is mocked in `mergify_engine.tests.functional.base`
        mergify_protection.update({"pattern": f"{self._testMethodName[:50]}/**/*"})
        await self.create_branch_protection_rule(mergify_protection)
        await self.create_branch_protection_rule(all_protection)

        p1 = await self.create_pr(two_commits=True)

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"], "rebase")
        p2_closed = await self.wait_for_pull_request("closed", p2["number"])
        assert p2_closed["pull_request"]["merged"]

        await self.add_label(p1["number"], "ready-to-merge")

        class HTTPForbiddenRename(http.HTTPClientSideError):
            status_code = 403
            message = "Resource not accessible by integration"

        real_client_post = github.AsyncGithubClient.post

        async def mock_client_post(  # type: ignore[no-untyped-def]
            self, url: str, **kwargs: typing.Any
        ) -> httpx.Response:
            if url.endswith("/rename"):
                raise HTTPForbiddenRename(
                    message="Resource not accessible by integration",
                    request=mock.Mock(),
                    response=mock.Mock(),
                )

            return await real_client_post(self, url, **kwargs)

        # Mock only in non-record mode to keep the real request from github in record mode
        if not base.RECORD:
            with mock.patch.object(github.AsyncGithubClient, "post", mock_client_post):
                await self.run_engine()
        else:
            await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.wait_for_pull_request("closed", draft_pr["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]
