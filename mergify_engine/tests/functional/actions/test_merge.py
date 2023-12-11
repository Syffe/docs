import logging
import typing

import pytest

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.actions import merge_base
from mergify_engine.tests.functional import base


LOG = logging.getLogger(__name__)


class TestMergeAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_merge_with_installation_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        assert True is p["merged"]
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
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        assert True is p["merged"]
        assert p["merged_by"]
        assert p["merged_by"]["login"] == "mergify-test4"

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

    async def test_report_queue_merge_without_admin_bot_account_and_branch_protections_bypassing(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "status-success=continuous-integration/fake-ci-queue",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci-merge",
                    ],
                    "branch_protection_injection_mode": "none",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": None,
            "required_linear_history": True,
            "required_pull_request_reviews": {
                "require_code_owner_reviews": True,
                "required_approving_review_count": 1,
            },
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p = await self.create_pr()
        await self.create_status(p, "continuous-integration/fake-ci-queue")
        await self.create_status(p, "continuous-integration/fake-ci-merge")
        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            conclusion="cancelled",
        )

        p = await self.get_pull(p["number"])
        assert not p["merged"]

        ctxt = context.Context(self.repository_ctxt, p, [])
        check = await ctxt.get_engine_check_run("Rule: Merge default (queue)")
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "`mergify-test4` account used as `bot_account` must have `admin` permission"
        )
        assert (
            check["output"]["summary"]
            == "To allow merge with branch protection enabled on the repository, please make sure the bot used has the necessary permissions."
        )

        updated_rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "status-success=continuous-integration/fake-ci-queue",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci-merge",
                    ],
                    "branch_protection_injection_mode": "none",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_bot_account": "mergify-test1",
                        },
                    },
                },
            ],
        }

        # set admin bot account
        p2 = await self.create_pr(files={".mergify.yml": yaml.dump(updated_rules)})
        await self.merge_pull_as_admin(p2["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()
        p = await self.get_pull(p["number"])
        await self.create_status(p, "continuous-integration/fake-ci-queue")
        await self.create_status(p, "continuous-integration/fake-ci-merge")
        await self.run_engine()
        merged_p = await self.wait_for_pull_request("closed", p["number"], merged=True)
        assert merged_p["pull_request"]["merged_by"] is not None
        assert merged_p["pull_request"]["merged_by"]["login"] == "mergify-test1"

    async def test_report_error_bot_account_wrong_permissions(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "status-success=continuous-integration/fake-ci-queue",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci-merge",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_bot_account": "anakin",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.create_status(p, "continuous-integration/fake-ci-queue")
        await self.create_status(p, "continuous-integration/fake-ci-merge")
        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            conclusion="cancelled",
        )

        p = await self.get_pull(p["number"])
        assert not p["merged"]

        ctxt = context.Context(self.repository_ctxt, p, [])
        check = await ctxt.get_engine_check_run("Rule: Merge default (queue)")

        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "`anakin` account used as `bot_account` must have `write`, `maintain` or `admin` permission, not `read`"
        )

    async def test_report_error_bot_account_is_a_bot(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "status-success=continuous-integration/fake-ci-queue",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci-merge",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_bot_account": "mergify-test4 [bot]",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.create_status(p, "continuous-integration/fake-ci-queue")
        await self.create_status(p, "continuous-integration/fake-ci-merge")
        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for_check_run(
            name="Rule: Merge default (queue)",
            conclusion="cancelled",
        )

        p = await self.get_pull(p["number"])
        assert not p["merged"]

        ctxt = context.Context(self.repository_ctxt, p, [])
        check = await ctxt.get_engine_check_run("Rule: Merge default (queue)")
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "Unable to merge queued pull request: GitHub App bot `mergify-test4 [bot]` can't be impersonated."
        )
        assert check["output"]["summary"] == ""

    @pytest.mark.skipif(
        not settings.GITHUB_URL.startswith("https://github.com"),
        reason="required_conversation_resolution requires GHES 3.2",
    )
    async def test_merge_branch_protection_conversation_resolution(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
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
            files={"my_testing_file": "foo", "super_original_testfile": "42\ntest\n"},
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
                    "actions": {"merge": {"method": "merge"}},
                },
            ],
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
        assert checks[0]["conclusion"] == "failure"
        assert (
            checks[0]["output"]["title"]
            == "Branch protection setting 'linear history' conflicts with Mergify configuration"
        )

    async def test_merge_branch_protection_linear_history_without_merge_method(
        self,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
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
        await self.wait_for_pull_request(
            action="closed",
            pr_number=p1["number"],
            merged=True,
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
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p = await self.get_pull(p["number"])
        assert True is p["merged"]
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
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        p2 = await self.get_pull(p["number"])
        assert True is p2["merged"]
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
        assert """
:bangbang: **Action Required** :bangbang:

> **The configuration uses the deprecated `commit_message` mode of the merge action.**
> A brownout is planned for the whole March 21th, 2022 day.
> This option will be removed on April 25th, 2022.
> For more information: https://docs.mergify.com/actions/merge/

""" not in summary["output"]["summary"]

    async def test_merge_template_with_co_authors(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {
                            "commit_message_template": """{{ title }}

{{ body | get_section("## Description", "") }}

Pull request: #{{ number }}
{% for co_author in co_authors %}
Co-Authored-By: {{ co_author.name }} <{{ co_author.email }}>
{% endfor %}
""",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(
            message="## Description\n\nHello there",
            commit_author="General Grievous <general.grievous@confederacy.org>",
        )
        await self.run_engine()
        merged_pull = await self.wait_for_pull_request(
            "closed",
            p["number"],
            merged=True,
        )

        merge_commit_sha = merged_pull["pull_request"]["merge_commit_sha"]
        assert merge_commit_sha is not None
        commit = await self.get_commit(merge_commit_sha)
        assert (
            f"""test_merge_template_with_co_authors: pull request n1 from integration

Hello there

Pull request: #{merged_pull['number']}

Co-Authored-By: General Grievous <general.grievous@confederacy.org>"""
            == commit["commit"]["message"]
        )

    async def test_merge_branch_protection_strict(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
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

        await self.create_status(p2)
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p2, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert "[ ] `#commits-behind=0`" in summary["output"]["summary"]

        await self.create_comment_as_admin(p2["number"], "@mergifyio update")
        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p2["number"],
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
                        },
                    },
                },
            ],
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
                f"{self.url_origin}/branches/{self.main_branch_name}",
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
                        },
                    },
                },
            ],
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
                f"{self.url_origin}/branches/{self.main_branch_name}",
            ),
        )
        assert p["head"]["sha"] == branch["commit"]["sha"]

        assert branch["commit"]["committer"] is not None
        assert (
            branch["commit"]["committer"]["login"]
            == self.RECORD_CONFIG["app_user_login"]
        )

    async def test_merge_conflicting_pr(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}", "label=merge"],
                    "actions": {"merge": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"conflicts": "well"})
        p2 = await self.create_pr(files={"conflicts": "boom"})

        await self.merge_pull(p1["number"])
        await self.run_engine()

        await self.add_label(p2["number"], "merge")
        await self.run_engine()

        ctxt_p2 = context.Context(self.repository_ctxt, p2)
        check = await ctxt_p2.get_engine_check_run("Rule: merge (merge)")
        assert check is None
        check = await ctxt_p2.get_engine_check_run("Summary")
        assert check is not None
        assert "[ ] `-conflict`" in check["output"]["summary"]

    async def test_default_merge_method(self) -> None:
        async def assert_cached_merge_method(expected_method: bytes) -> None:
            owner_id = self.RECORD_CONFIG["organization_id"]
            repository_id = self.RECORD_CONFIG["repository_id"]
            cached_method = await self.redis_links.cache.get(
                f"merge-method/{owner_id}/{repository_id}",
            )
            assert cached_method == expected_method

        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        # Allow merge
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("merge",),
        ):
            await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )
        await assert_cached_merge_method(b"merge")

        # Allow squash
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("squash",),
        ):
            await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )
        await assert_cached_merge_method(b"squash")

        # Allow rebase
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("rebase",),
        ):
            await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )
        await assert_cached_merge_method(b"rebase")

        # Disallow every merge methods
        pull = await self.create_pr()
        async with self.allow_merge_methods(self.url_origin, pull["number"]):
            await self.run_engine()
        check_run = await self.wait_for_check_run(
            conclusion="cancelled",
            name="Rule: merge on main (merge)",
        )
        assert check_run["check_run"]["output"]["title"] in (
            merge_base.FORBIDDEN_MERGE_COMMITS_MSG,
            merge_base.FORBIDDEN_REBASE_MERGE_MSG,
            merge_base.FORBIDDEN_SQUASH_MERGE_MSG,
        )
