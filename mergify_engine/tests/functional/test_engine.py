import asyncio
import logging
import typing
from unittest import mock

import pytest

from mergify_engine import cache
from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import refresher
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.engine import commands_runner
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import live_resolvers
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


LOG = logging.getLogger(__name__)


@pytest.mark.subscription(
    subscription.Features.WORKFLOW_AUTOMATION,
    subscription.Features.MERGE_QUEUE,
)
class TestEngineV2Scenario(base.FunctionalTestBase):
    """Mergify engine tests.

    Tests user github resource and are slow, so we must reduce the number
    of scenario as much as possible for now.
    """

    async def test_merge_squash(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [f"base={self.main_branch_name}", "label=squash"],
                    "actions": {"merge": {"method": "squash"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"foo": "bar"})
        p2 = await self.create_pr(two_commits=True)
        await self.merge_pull(p1["number"])

        await self.add_label(p2["number"], "squash")
        await self.run_engine()

        p2_merged = await self.wait_for_pull_request("closed", pr_number=p2["number"])
        assert p2_merged["pull_request"]["merged"]
        assert p2_merged["pull_request"]["commits"] == 2

    async def test_teams(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "valid teams",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "approved-reviews-by=@mergifyio-testing/testing",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "short teams",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "approved-reviews-by=@testing",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "not exists teams",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "approved-reviews-by=@mergifyio-testing/noexists",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "invalid organization",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "approved-reviews-by=@another-org/testing",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])

        logins = await live_resolvers.teams(
            self.repository_ctxt,
            [
                "user",
                "@mergifyio-testing/testing",
            ],
        )

        assert sorted(logins) == sorted(
            [
                "user",
                "mergify-test1",
                "mergify-test3",
            ],
        )

        logins = await live_resolvers.teams(
            self.repository_ctxt,
            [
                "user",
                "@testing",
            ],
        )

        assert sorted(logins) == sorted(
            [
                "user",
                "mergify-test1",
                "mergify-test3",
            ],
        )

        with pytest.raises(live_resolvers.LiveResolutionFailureError):
            await live_resolvers.teams(self.repository_ctxt, ["@unknown/team"])

        with pytest.raises(live_resolvers.LiveResolutionFailureError):
            await live_resolvers.teams(
                self.repository_ctxt,
                ["@mergifyio-testing/not-exists"],
            )

        with pytest.raises(live_resolvers.LiveResolutionFailureError):
            await live_resolvers.teams(
                self.repository_ctxt,
                ["@invalid/team/break-here"],
            )

        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["title"] == "2 faulty rules and 2 potential rules"
        assert summary["output"]["summary"] is not None
        for message in (
            "Team `@mergifyio-testing/noexists` does not exist",
            "Team `@another-org/testing` is not part of the organization `mergifyio-testing`",
        ):
            assert message in summary["output"]["summary"]

    async def _test_merge_custom_msg(
        self,
        header: str,
        method: str = "squash",
        msg: str | None = None,
        commit_msg: str | None = None,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"merge": {"method": method}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        if msg is None:
            msg = "This is the title\n\nAnd this is the message"

        p = await self.create_pr(message=f"It fixes it\n\n## {header}{msg}")
        await self.create_status(p)

        await self.run_engine()

        await self.wait_for_pull_request("closed", pr_number=p["number"], merged=True)

        commit = (await self.get_head_commit())["commit"]
        if commit_msg is None:
            commit_msg = msg

        assert commit_msg == commit["message"]

    async def test_merge_custom_msg(self) -> None:
        await self._test_merge_custom_msg("Commit Message:\n")

    async def test_merge_custom_msg_case(self) -> None:
        await self._test_merge_custom_msg("Commit message\n")

    async def test_merge_custom_msg_rn(self) -> None:
        await self._test_merge_custom_msg("Commit Message\r\n")

    async def test_merge_custom_msg_merge(self) -> None:
        await self._test_merge_custom_msg("Commit Message:\n", "merge")

    async def test_merge_custom_msg_template(self) -> None:
        await self._test_merge_custom_msg(
            "Commit Message:\n",
            "merge",
            msg="{{title}}\n\nThanks to {{author}}",
            commit_msg=f"test_merge_custom_msg_template: pull request n1 from integration\n\nThanks to {self.RECORD_CONFIG['app_user_login']}",
        )

    async def test_merge_invalid_custom_msg(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"merge": {"method": "merge"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        msg = "This is the title\n\nAnd this is the message {{invalid}}"
        p = await self.create_pr(message=f"It fixes it\n\n## Commit Message\n{msg}")
        await self.create_status(p)

        await self.run_engine()

        assert not await self.is_pull_merged(p["number"])

        check_run_p = await self.wait_for_check_run(
            conclusion="failure",
            status="completed",
            name="Rule: merge (merge)",
        )
        assert check_run_p["check_run"]["pull_requests"][0]["number"] == p["number"]
        assert (
            check_run_p["check_run"]["output"]["summary"]
            == "Unknown pull request attribute: invalid"
        )
        assert check_run_p["check_run"]["output"]["title"] == "Invalid commit message"

        # Edit and fixes the typo
        await self.edit_pull(
            p["number"],
            body="It fixes it\n\n## Commit Message\n\nHere it is valid now",
        )
        await self.wait_for_pull_request("edited", p["number"])
        await self.create_comment_as_admin(p["number"], "@mergifyio refresh")
        await self.run_engine()

        await self.wait_for_pull_request("closed", p["number"], merged=True)
        await self.wait_for_check_run(
            conclusion="success",
            status="completed",
            name="Rule: merge (merge)",
        )

    async def test_rebase(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "#approved-reviews-by>=1",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.create_status(p)
        await self.create_review(p["number"])

        await self.run_engine()

        p_merged = await self.wait_for_pull_request("closed")
        assert p_merged["pull_request"]["merged"]

    async def test_merge_branch_protection_ci(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "merge": {"method": "rebase"},
                        "comment": {"message": "yo"},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        # Check policy of that branch is the expected one
        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                    "neutral-ci",
                    "skipped-ci",
                ],
            },
            "required_pull_request_reviews": {
                "require_code_owner_reviews": True,
                "required_approving_review_count": 1,
            },
            "required_linear_history": False,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p = await self.create_pr()
        await self.run_engine()

        check_run_summary_p = await self.wait_for_check_run(
            conclusion="success",
            status="completed",
            name="Summary",
        )
        assert check_run_summary_p["check_run"]["output"]["summary"] is not None
        assert (
            f"""### Rule: merge (merge)
- [ ] `#approved-reviews-by>=1` [🛡 GitHub branch protection]
- [ ] `branch-protection-review-decision=APPROVED` [🛡 GitHub branch protection]
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=neutral-ci`
  - [ ] `check-skipped=neutral-ci`
  - [ ] `check-success=neutral-ci`
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=skipped-ci`
  - [ ] `check-skipped=skipped-ci`
  - [ ] `check-success=skipped-ci`
- [X] `#changes-requested-reviews-by=0` [🛡 GitHub branch protection]
- [X] `-conflict` [📌 merge requirement]
- [X] `-draft` [📌 merge requirement]
- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]
- [X] `base={self.main_branch_name}`

### ✅ Rule: merge (comment)
- [X] `base={self.main_branch_name}`
"""
            in check_run_summary_p["check_run"]["output"]["summary"]
        )

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = [
            c
            for c in await ctxt.pull_engine_check_runs
            if c["name"] == "Rule: merge (merge)"
        ]
        assert checks == []

        await self.create_status(p)
        await self.create_review(p["number"])
        await check_api.set_check_run(
            ctxt,
            "neutral-ci",
            check_api.Result(check_api.Conclusion.NEUTRAL, title="bla", summary=""),
        )
        await check_api.set_check_run(
            ctxt,
            "skipped-ci",
            check_api.Result(
                check_api.Conclusion.SKIPPED,
                title="bla-skipped",
                summary="",
            ),
        )

        await self.run_engine()

        p_merged = await self.wait_for_pull_request("closed", pr_number=p["number"])
        assert p_merged["pull_request"]["merged"]

    async def test_refresh_via_check_suite_rerequest(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "nothing",
                    "conditions": [f"base!={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        await self.run_engine()

        check = typing.cast(
            github_types.GitHubCheckRun,
            await self.client_integration.items(
                f"{self.url_origin}/commits/{p['head']['sha']}/check-runs",
                api_version="antiope",
                list_items="check_runs",
                resource_name="check runs",
                page_limit=5,
                params={"name": "Summary"},
            ).__anext__(),
        )
        assert check is not None
        check_suite_id = check["check_suite"]["id"]

        check_suite = (
            await self.installation_ctxt.client.get(
                f"{self.repository_ctxt.base_url}/check-suites/{check_suite_id}",
                api_version="antiope",
            )
        ).json()
        assert check_suite["status"] == "completed"

        # Click on rerequest btn
        await self.installation_ctxt.client.post(
            f"{self.repository_ctxt.base_url}/check-suites/{check_suite_id}/rerequest",
            api_version="antiope",
        )
        await self.wait_for("check_suite", {"action": "rerequested"})

        check_suite = (
            await self.installation_ctxt.client.get(
                f"{self.repository_ctxt.base_url}/check-suites/{check_suite_id}",
                api_version="antiope",
            )
        ).json()
        assert check_suite["status"] == "queued"

        await self.run_engine()
        check_suite = (
            await self.installation_ctxt.client.get(
                f"{self.repository_ctxt.base_url}/check-suites/{check_suite_id}",
                api_version="antiope",
            )
        ).json()
        assert check_suite["status"] == "completed"

    async def test_command_refresh(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "nothing",
                    "conditions": [f"base!={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        await self.run_engine()

        ctxt = context.Context(
            self.repository_ctxt,
            p,
        )
        await ctxt.set_summary_check(
            check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="whatever",
                summary="erased",
            ),
        )

        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        completed_at = summary["completed_at"]

        await self.create_comment_as_admin(p["number"], "@mergifyio refresh")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(p["number"], "created")
        assert (
            f"""> refresh

#### ✅ Pull request refreshed



{utils.serialize_hidden_payload(commands_runner.CommandPayload({"command": "refresh", "conclusion": "success", "action_is_running": True}))}"""
            == comment["comment"]["body"]
        )

        ctxt = context.Context(self.repository_ctxt, p, [])
        check_run_summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert check_run_summary is not None
        assert check_run_summary["conclusion"] == "success"
        assert check_run_summary["status"] == "completed"
        assert completed_at != check_run_summary["completed_at"]

    async def test_refresh_on_conflict(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment-on-conflict",
                    "conditions": ["conflict"],
                    "actions": {"comment": {"message": "It conflict!"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), files={"TESTING": "foobar"})
        p1 = await self.create_pr(files={"TESTING": "p1"})
        p2 = await self.create_pr(files={"TESTING": "p2"})
        await self.merge_pull(p1["number"], wait_for_main_push=True)

        # Wait a bit than GitHub refresh the mergeable_state before running the
        # engine
        if settings.TESTING_RECORD:
            await asyncio.sleep(3)

        await self.run_engine()
        await self.wait_for_issue_comment(
            p2["number"],
            "created",
            comment_body="It conflict!",
        )

    async def test_refresh_on_draft_conflict(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment-on-conflict",
                    "conditions": ["conflict"],
                    "actions": {"comment": {"message": "It conflict!"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), files={"TESTING": "foobar"})
        p1 = await self.create_pr(files={"TESTING": "p1"})
        p2 = await self.create_pr(files={"TESTING": "p2"}, draft=True)
        await self.merge_pull(p1["number"], wait_for_main_push=True)

        # Wait a bit than GitHub refresh the mergeable_state before running the
        # engine
        if settings.TESTING_RECORD:
            await asyncio.sleep(3)

        await self.run_engine()
        await self.wait_for_issue_comment(
            p2["number"],
            "created",
            comment_body="It conflict!",
        )

    async def test_set_summary_with_broken_checks(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        ctxt = context.Context(self.repository_ctxt, p, [])

        with mock.patch(
            "mergify_engine.context.Context.pull_check_runs",
            new_callable=mock.PropertyMock,
            side_effect=github.TooManyPages("foobar", 1, 1, 1),
        ):
            with pytest.raises(github.TooManyPages):
                await ctxt.pull_check_runs

            await ctxt.set_summary_check(
                check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "damn",
                    "but we was able to set the summary ;)",
                ),
            )

        assert ctxt._caches.pull_check_runs.get() is cache.Unset

        check = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert check is not None
        assert check["output"]["title"] == "damn"
        assert check["output"]["summary"] == "but we was able to set the summary ;)"

    async def test_requested_reviews(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "user",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "review-requested=mergify-test4",
                    ],
                    "actions": {"comment": {"message": "review-requested user"}},
                },
                {
                    "name": "team",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "review-requested=@testing",
                    ],
                    "actions": {"comment": {"message": "review-requested team"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.create_review_request(p1["number"], reviewers=["mergify-test4"])
        await self.run_engine()

        comment = await self.wait_for_issue_comment(p1["number"], "created")
        assert comment["comment"]["body"] == "review-requested user"

        # FIXME(sileht): This doesn't work anymore MRGFY-227
        # p2 = await self.create_pr()
        # p2.create_review_request(team_reviewers=[team.slug])
        # await self.wait_for("pull_request", {"action": "review_requested"})
        # await self.run_engine()
        # await self.wait_for("issue_comment", {"action": "created"})

    async def test_truncated_check_output(self) -> None:
        # not used anyhow
        rules = {
            "pull_request_rules": [{"name": "noop", "conditions": [], "actions": {}}],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.run_engine()
        pull = context.Context(self.repository_ctxt, pr, [])
        check = await check_api.set_check_run(
            pull,
            "Test",
            check_api.Result(
                check_api.Conclusion.SUCCESS,
                title="bla",
                summary="a" * 70000,
            ),
        )
        assert check["output"]["summary"] == ("a" * 65532 + "…")

    async def test_pull_request_init_summary(self) -> None:
        rules = {
            "pull_request_rules": [{"name": "noop", "conditions": [], "actions": {}}],
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        # Run the engine once, to initialize the config location cache
        await self.create_pr()
        await self.run_engine()

        # Check initial summary is submitted
        pr = await self.create_pr()
        check_run_p2 = await self.wait_for_check_run(
            action="created",
            status="in_progress",
        )

        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "Your rules are under evaluation"
        )

        assert (
            check_run_p2["check_run"]["output"]["summary"]
            == "Be patient, the page will be updated soon."
        )
        assert (
            check_run_p2["check_run"]["details_url"]
            == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{pr['base']['user']['login']}"
            f"/repo/{pr['base']['repo']['name']}/event-logs?pullRequestNumber={pr['number']}"
        )

        # Check that initial summary is not created on merge queue pr
        await self.create_command(pr["number"], "@mergifyio queue", as_="admin")
        draft_pr = await self.wait_for_pull_request("opened")
        assert queue_utils.is_merge_queue_pr(draft_pr["pull_request"])
        assert [] == (await self.get_check_runs(draft_pr["pull_request"]))

    async def test_pull_refreshed_after_config_change(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "default",
                    "conditions": ["base=other"],
                    "actions": {"comment": {"message": "it works"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(files={"foo": "bar"})
        await self.run_engine()

        rules["pull_request_rules"][0]["conditions"][  # type: ignore[index]
            0
        ] = f"base={self.main_branch_name}"
        p_config = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.merge_pull(p_config["number"], wait_for_main_push=True)

        await self.run_engine()
        comment = await self.wait_for_issue_comment(p["number"], "created")
        assert comment["comment"]["body"] == "it works"

    async def test_check_run_api(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        ctxt = context.Context(self.repository_ctxt, p, [])

        await check_api.set_check_run(
            ctxt,
            "Test",
            check_api.Result(
                check_api.Conclusion.PENDING,
                title="PENDING",
                summary="PENDING",
            ),
        )
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "in_progress"
        assert checks[0]["conclusion"] is None
        assert checks[0]["completed_at"] is None
        assert checks[0]["output"]["title"] == "PENDING"
        assert checks[0]["output"]["summary"] == "PENDING"

        await check_api.set_check_run(
            ctxt,
            "Test",
            check_api.Result(
                check_api.Conclusion.CANCELLED,
                title="CANCELLED",
                summary="CANCELLED",
            ),
        )
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "completed"
        assert checks[0]["conclusion"] == "cancelled"
        assert checks[0]["completed_at"] is not None
        assert checks[0]["output"]["title"] == "CANCELLED"
        assert checks[0]["output"]["summary"] == "CANCELLED"

        # clear cache and retry
        ctxt._caches = context.ContextCaches()
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "completed"
        assert checks[0]["conclusion"] == "cancelled"
        assert checks[0]["completed_at"] is not None
        assert checks[0]["output"]["title"] == "CANCELLED"
        assert checks[0]["output"]["summary"] == "CANCELLED"

        await check_api.set_check_run(
            ctxt,
            "Test",
            check_api.Result(
                check_api.Conclusion.PENDING,
                title="PENDING",
                summary="PENDING",
            ),
        )
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "in_progress"
        assert checks[0]["conclusion"] is None
        assert checks[0]["completed_at"] is None
        assert checks[0]["output"]["title"] == "PENDING"
        assert checks[0]["output"]["summary"] == "PENDING"

        # Clear cache and retry
        ctxt._caches = context.ContextCaches()
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "in_progress"
        assert checks[0]["conclusion"] is None
        assert checks[0]["completed_at"] is None
        assert checks[0]["output"]["title"] == "PENDING"
        assert checks[0]["output"]["summary"] == "PENDING"

        # same tests with skip_cache=True
        ctxt._caches = context.ContextCaches()
        await check_api.set_check_run(
            ctxt,
            "Test",
            check_api.Result(
                check_api.Conclusion.CANCELLED,
                title="CANCELLED",
                summary="CANCELLED",
            ),
            skip_cache=True,
        )
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "completed"
        assert checks[0]["conclusion"] == "cancelled"
        assert checks[0]["completed_at"] is not None
        assert checks[0]["output"]["title"] == "CANCELLED"
        assert checks[0]["output"]["summary"] == "CANCELLED"

        # clear cache and retry
        ctxt._caches = context.ContextCaches()
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "completed"
        assert checks[0]["conclusion"] == "cancelled"
        assert checks[0]["completed_at"] is not None
        assert checks[0]["output"]["title"] == "CANCELLED"
        assert checks[0]["output"]["summary"] == "CANCELLED"

        ctxt._caches = context.ContextCaches()
        await check_api.set_check_run(
            ctxt,
            "Test",
            check_api.Result(
                check_api.Conclusion.PENDING,
                title="PENDING",
                summary="PENDING",
            ),
            skip_cache=True,
        )
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "in_progress"
        assert checks[0]["conclusion"] is None
        assert checks[0]["completed_at"] is None
        assert checks[0]["output"]["title"] == "PENDING"
        assert checks[0]["output"]["summary"] == "PENDING"

        # Clear cache and retry
        ctxt._caches = context.ContextCaches()
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["status"] == "in_progress"
        assert checks[0]["conclusion"] is None
        assert checks[0]["completed_at"] is None
        assert checks[0]["output"]["title"] == "PENDING"
        assert checks[0]["output"]["summary"] == "PENDING"

    async def test_get_repository_by_id(self) -> None:
        repo = await self.installation_ctxt.get_repository_by_id(
            self.RECORD_CONFIG["repository_id"],
        )
        assert repo.repo["name"] == self.RECORD_CONFIG["repository_name"]
        assert repo.repo["name"] == self.repository_ctxt.repo["name"]

    async def test_sha_collision(self) -> None:
        await self.setup_repo()
        prs = await self.create_prs_with_same_head_sha()

        await self.run_engine()
        await self.wait_for_issue_comment(prs[1]["number"], "created")

        await self.create_comment_as_admin(prs[1]["number"], "@mergifyio refresh")

        await self.run_engine()
        # Wait for the refresh comment
        await self.wait_for_issue_comment(prs[1]["number"], "created")

        await refresher.send_pull_refresh(
            self.redis_links.stream,
            self.repository_ctxt.repo,
            "user",
            prs[1]["number"],
            "test",
        )
        await self.run_engine()

        # Ensure the sha collision message is not posted twice
        comments = await self.get_issue_comments(prs[1]["number"])
        assert len(comments) == 3

    @pytest.mark.subscription(
        subscription.Features.MERGE_QUEUE,
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_sha_collision_after_pr_synchronization(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        {
                            "or": [
                                "status-success=continuous-integration/fake-ci",
                                "label=merge",
                            ],
                        },
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        other_branch = self.get_full_branch_name("other")
        await self.setup_repo(yaml.dump(rules), test_branches=(other_branch,))

        # Create 2 PRs from the same branch
        pr1, pr2 = await self.create_prs_with_same_head_sha()

        # Make them point to different base to avoid merging one result in merging
        # the other one automatically
        await self.edit_pull(pr2["number"], base=other_branch)
        await self.wait_for_pull_request("edited", pr2["number"])

        # Change the headsha on pr1
        await self.push_file(destination_branch=pr1["head"]["ref"])
        pr1 = (await self.wait_for_pull_request("synchronize", pr1["number"]))[
            "pull_request"
        ]
        await self.add_label(pr1["number"], "queue")

        # Add both to the cache and include pr1 in the train
        await self.run_engine()
        await self.wait_for_check_run(name=constants.MERGE_QUEUE_SUMMARY_NAME)

        # Synchronize pr2 to have same headsha as pr1
        await self.git("rebase", f"origin/{pr1['head']['ref']}", pr2["head"]["ref"])
        await self.git("push", "--quiet", "origin", pr2["head"]["ref"])
        pr2 = (await self.wait_for_pull_request("synchronize", pr2["number"]))[
            "pull_request"
        ]

        assert pr2["head"]["sha"] == pr1["head"]["sha"]

        await self.run_engine()
        comment = await self.wait_for_issue_comment(pr2["number"], "created")
        assert (
            comment["comment"]["body"]
            == f"⚠️ The sha of the head commit of this PR conflicts with #{pr1['number']}. Mergify cannot evaluate rules on this PR. ⚠️"
        )

        # Pr2 is not merged even if it fulfill the requirements
        await self.add_label(pr2["number"], "queue")
        await self.add_label(pr2["number"], "merge")
        await self.create_command(pr2["number"], "@Mergifyio refresh", as_="admin")
        pr2 = await self.get_pull(pr2["number"])

        assert pr2["merged"] is False

        # Merge automaticaly pr1 even if pr2 has a sha collision
        await self.add_label(pr1["number"], "merge")
        await self.run_engine()
        await self.wait_for_pull_request("closed", pr1["number"], merged=True)

        # Now pr2 can be merged
        await self.create_command(pr2["number"], "@Mergifyio refresh", as_="admin")
        await self.wait_for_pull_request("closed", pr2["number"], merged=True)

    @pytest.mark.subscription(
        subscription.Features.MERGE_QUEUE,
        subscription.Features.QUEUE_ACTION,
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_pull_request_implicitly_merged(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "priority_rules": [
                        {
                            "name": "high priority",
                            "conditions": ["label=urgent"],
                            "priority": "high",
                        },
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": ""},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        pr_1 = await self.create_pr()
        await self.add_label(pr_1["number"], "standard")

        pr_2 = await self.create_pr(base=pr_1["head"]["ref"])
        await self.edit_pull(pr_2["number"], base=self.main_branch_name)
        await self.add_label(pr_2["number"], "urgent")

        await self.run_engine()

        found = 0
        while 1:
            check = await self.wait_for_check_run(
                "completed",
                name="Rule: default merge (queue)",
            )
            assert check["check_run"]["head_sha"] in [
                pr_1["head"]["sha"],
                pr_2["head"]["sha"],
            ]
            if check["check_run"]["head_sha"] == pr_1["head"]["sha"]:
                assert (
                    check["check_run"]["output"]["title"]
                    == "The pull request has been merged implicitly by merging another pull request"
                )
                found += 1
            elif check["check_run"]["head_sha"] == pr_2["head"]["sha"]:
                assert (
                    check["check_run"]["output"]["title"]
                    == "The pull request has been merged automatically"
                )
                found += 1

            if found == 2:
                break
