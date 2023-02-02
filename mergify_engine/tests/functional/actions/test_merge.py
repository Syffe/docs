import logging
import typing
from unittest import mock

import pytest

from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.tests.functional import base


LOG = logging.getLogger(__name__)


class TestMergeAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    @mock.patch.object(config, "ALLOW_REBASE_FALLBACK_ATTRIBUTE", False)
    async def test_rebase_fallback_brownout(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"merge": {"rebase_fallback": "merge"}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "failure" == checks[0]["conclusion"]
        assert (
            "The current Mergify configuration is invalid"
            == checks[0]["output"]["title"]
        )
        assert (
            "`merge` is invalid for dictionary value @ pull_request_rules → item 0 → actions → merge → rebase_fallback"
            in checks[0]["output"]["summary"]
        )
        assert (
            "The configuration uses the deprecated `rebase_fallback` attribute"
            in checks[0]["output"]["summary"]
        )

    async def test_rebase_fallback_deprecation_notice(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"merge": {"rebase_fallback": "merge"}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "success" == checks[0]["conclusion"]
        assert (
            "**The configuration uses the deprecated `rebase_fallback` mode"
            in checks[0]["output"]["summary"]
        )

    async def test_rebase_fallback_deprecation_notice_not_present_in_summary(
        self,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"merge": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "success" == checks[0]["conclusion"]
        assert (
            "**The configuration uses the deprecated `rebase_fallback` mode"
            not in checks[0]["output"]["summary"]
        )

    async def test_merge_with_installation_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        self.assertEqual(True, p["merged"])
        assert p["merged_by"]
        assert self.RECORD_CONFIG["app_user_id"] == p["merged_by"]["id"]

    async def test_merge_with_oauth_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {"merge_bot_account": "{{ body }}"}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        self.assertEqual(True, p["merged"])
        assert p["merged_by"]
        assert "mergify-test4" == p["merged_by"]["login"]

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        check = checks[1]
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )
        assert (
            check["output"]["summary"]
            == f"The pull request has been merged automatically at *{p['merge_commit_sha']}*"
        )

    @pytest.mark.skipif(
        not config.GITHUB_URL.startswith("https://github.com"),
        reason="required_conversation_resolution requires GHES 3.2",
    )
    async def test_merge_branch_protection_conversation_resolution(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": None,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "required_conversation_resolution": True,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr(
            files={"my_testing_file": "foo", "super_original_testfile": "42\ntest\n"}
        )

        await self.create_review_thread(
            p1["number"],
            "Don't like this line too much either",
            path="super_original_testfile",
            line=2,
        )

        thread = (await self.get_review_threads(p1["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p1, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None

        assert (
            "- [ ] `#review-threads-unresolved=0` [🛡 GitHub branch protection]"
            in summary["output"]["summary"]
        )

        is_resolved = await self.resolve_review_thread(thread_id=thread["id"])
        assert is_resolved

        thread = (await self.get_review_threads(p1["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]
        assert thread["isResolved"]

        await self.wait_for("pull_request_review_thread", {"action": "resolved"})
        await self.run_engine()

        ctxt._caches.pull_check_runs.delete()
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None

        assert (
            "- [X] `#review-threads-unresolved=0` [🛡 GitHub branch protection]"
            in summary["output"]["summary"]
        )

    async def test_merge_branch_protection_linear_history(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": None,
            "required_linear_history": True,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.run_engine()
        await self.wait_for("check_run", {"check_run": {"conclusion": "failure"}})

        ctxt = context.Context(self.repository_ctxt, p1, [])
        checks = [
            c
            for c in await ctxt.pull_engine_check_runs
            if c["name"] == "Rule: merge (merge)"
        ]
        assert "failure" == checks[0]["conclusion"]
        assert (
            "Branch protection setting 'linear history' conflicts with Mergify configuration"
            == checks[0]["output"]["title"]
        )

    async def test_merge_template_with_empty_body(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {
                            "commit_message_template": """{{ title }} (#{{ number }})

{{body}}
""",
                        }
                    },
                },
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        self.assertEqual(True, p["merged"])
        assert p["merge_commit_sha"]
        c = await self.get_commit(p["merge_commit_sha"])
        assert (
            f"""test_merge_template_with_empty_body: pull request n1 from integration (#{p['number']})"""
            == c["commit"]["message"]
        )

    async def test_merge_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {
                            "commit_message_template": """{{ title }} (#{{ number }})
{{body}}
superRP!
""",
                        }
                    },
                },
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p2 = await self.get_pull(p["number"])
        self.assertEqual(True, p2["merged"])
        assert p2["merge_commit_sha"]
        p3 = await self.get_commit(p2["merge_commit_sha"])
        assert (
            f"""test_merge_template: pull request n1 from integration (#{p2['number']})

mergify-test4
superRP!"""
            == p3["commit"]["message"]
        )
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary
        assert (
            """
:bangbang: **Action Required** :bangbang:

> **The configuration uses the deprecated `commit_message` mode of the merge action.**
> A brownout is planned for the whole March 21th, 2022 day.
> This option will be removed on April 25th, 2022.
> For more information: https://docs.mergify.com/actions/merge/

"""
            not in summary["output"]["summary"]
        )

    async def test_merge_branch_protection_strict(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        # Check policy of that branch is the expected one
        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.merge_pull(p1["number"])

        await self.branch_protection_protect(self.main_branch_name, protection)

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        await self.create_status(p2)
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p2, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert "[ ] `#commits-behind=0`" in summary["output"]["summary"]

        await self.create_comment_as_admin(p2["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=p2["number"]
        )
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        p2 = await self.get_pull(p2["number"])
        await self.create_status(p2)
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p2, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert "[X] `#commits-behind=0`" in summary["output"]["summary"]

        p2 = await self.get_pull(p2["number"])
        assert p2["merged"]

    async def test_merge_fastforward_basic(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {
                            "method": "fast-forward",
                        }
                    },
                },
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        assert p["merged"]
        assert p["merged_by"]
        assert self.RECORD_CONFIG["app_user_id"] == p["merged_by"]["id"]

        branch = typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}"
            ),
        )
        assert p["head"]["sha"] == branch["commit"]["sha"]

        assert branch["commit"]["committer"] is not None
        assert (
            branch["commit"]["committer"]["login"]
            == self.RECORD_CONFIG["app_user_login"]
        )

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        check = checks[1]
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )
        assert (
            check["output"]["summary"]
            == f"The pull request has been merged automatically at *{p['head']['sha']}*"
        )

    async def test_merge_fastforward_bot_account(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {
                            "method": "fast-forward",
                            "merge_bot_account": "{{ body }}",
                        }
                    },
                },
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        assert p["merged"]
        assert p["merged_by"]
        assert p["merged_by"]["login"] == "mergify-test4"

        branch = typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}"
            ),
        )
        assert p["head"]["sha"] == branch["commit"]["sha"]

        assert branch["commit"]["committer"] is not None
        assert (
            branch["commit"]["committer"]["login"]
            == self.RECORD_CONFIG["app_user_login"]
        )
