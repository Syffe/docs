import datetime
import logging
import operator
from unittest import mock

import pytest

from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel


LOG = logging.getLogger(__name__)


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestAttributes(base.FunctionalTestBase):
    async def test_merged_attribute(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no manual merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "merged",
                        "#approved-reviews-by=0",
                    ],
                    "actions": {"comment": {"message": "no way"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.merge_pull(pr["number"])
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "no way"

    @pytest.mark.subscription(
        subscription.Features.WORKFLOW_AUTOMATION,
        subscription.Features.MERGE_QUEUE,
    )
    async def test_jit_schedule_on_queue_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": ["schedule: MON-FRI 08:00-17:00"],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "fast queue",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        with time_travel("2021-09-22T08:00:02", tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr()
            pr_force_rebase = await self.create_pr()

            await self.merge_pull(pr_force_rebase["number"])
            await self.wait_for_push(branch_name=self.main_branch_name)

            await self.run_full_engine()
            pr_queue = await self.wait_for_pull_request("opened")

            await self.run_full_engine()
            await self.wait_for(
                "pull_request",
                {"action": "closed", "number": pr_queue["number"]},
            )
            await self.wait_for_pull_request(
                "closed",
                pr_number=pr["number"],
                merged=True,
            )

    async def test_disabled(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "disabled": {"reason": "code freeze"},
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "-closed",
                        "label!=foo",
                    ],
                    "actions": {"close": {}},
                },
                {
                    "name": "nothing",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "closed",
                        "label=foo",
                    ],
                    "actions": {},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()
        ctxt = context.Context(self.repository_ctxt, pr)
        await self.run_engine()

        assert (await self.get_pull(pr["number"]))["state"] == "open"
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        expected = (
            "### Rule: ~~merge (close)~~\n:no_entry_sign: **Disabled: code freeze**\n"
        )
        assert expected in summary["output"]["summary"]

    async def test_schedule_no_timezone(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["schedule=MON-FRI 12:00-15:00"],
                    "actions": {"comment": {"message": "it's time"}},
                },
            ],
        }
        # Sunday
        with time_travel("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            await self.run_full_engine()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            assert await self.redis_links.cache.zcard("delayed-refresh") == 1

        # Wednesday
        with time_travel("2021-06-02T14:00:00", tick=True):
            await self.run_full_engine()

            comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
            assert comment["comment"]["body"] == "it's time"

    async def test_schedule_with_1_day_timezone_diff(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["schedule=MON-FRI 08:00-17:00[Pacific/Auckland]"],
                    "actions": {"comment": {"message": "it's time"}},
                },
            ],
        }
        # Sunday 19:00 for UTC
        # Monday 07:00 for NZST (Auckland)
        with time_travel("2022-09-04T19:00:00+00:00", tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            await self.run_full_engine()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            assert await self.redis_links.cache.zcard("delayed-refresh") == 1

        # Sunday 21:00 for UTC
        # Monday 09:00 for NZST (Auckland)
        with time_travel("2022-09-04T21:00:00+00:00", tick=True):
            await self.run_full_engine()
            comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
            assert comment["comment"]["body"] == "it's time"

    async def test_updated_relative_not_match(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["created-at<9999 days ago"],
                    "actions": {"comment": {"message": "it's time"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 0
        await self.run_engine()
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 0

        assert await self.redis_links.cache.zcard("delayed-refresh") == 1

    async def test_commits_behind_conditions_pr_open_before(self) -> None:
        await self.setup_repo()
        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr_branch = self.get_full_branch_name("esca#p.me")
        p = await self.create_pr(branch=pr_branch)

        pr_force_rebase = await self.create_pr(two_commits=True)
        await self.merge_pull_as_admin(pr_force_rebase["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)

        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p)
        assert await ctxt.is_behind
        assert await ctxt.commits_behind_count == 3

        await self.client_integration.put(
            f"{self.repository_ctxt.base_url}/pulls/{p['number']}/update-branch",
            api_version="lydian",
            json={"expected_head_sha": p["head"]["sha"]},
        )
        p_synced = await self.wait_for_pull_request("synchronize")
        ctxt = context.Context(self.repository_ctxt, p_synced["pull_request"])
        assert not await ctxt.is_behind
        assert await ctxt.commits_behind_count == 0

    async def test_commits_behind_conditions_pr_open_after(self) -> None:
        await self.setup_repo()

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr_force_rebase = await self.create_pr(two_commits=True)
        await self.merge_pull_as_admin(pr_force_rebase["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)

        await self.git("reset", "--hard", "HEAD^^")
        p = await self.create_pr(git_tree_ready=True)

        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p)
        assert await ctxt.is_behind
        assert await ctxt.commits_behind_count == 3

        await self.client_integration.put(
            f"{self.repository_ctxt.base_url}/pulls/{p['number']}/update-branch",
            api_version="lydian",
            json={"expected_head_sha": p["head"]["sha"]},
        )
        p_synced = await self.wait_for_pull_request("synchronize")
        ctxt = context.Context(self.repository_ctxt, p_synced["pull_request"])
        assert not await ctxt.is_behind
        assert await ctxt.commits_behind_count == 0

    async def test_updated_relative_match(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["created-at>=9999 days ago"],
                    "actions": {"comment": {"message": "it's time"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "it's time"

    async def test_draft_attribute(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["draft"],
                    "actions": {"comment": {"message": "draft pr"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        assert not pr["draft"]

        pr = await self.create_pr(draft=True)
        assert pr["draft"]
        assert pr["head"]["repo"] is not None

        pr_ahead = await self.create_pr()
        await self.merge_pull_as_admin(pr_ahead["number"])

        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "draft pr"

        await self.reload_repository_ctxt_configuration()
        ctxt = context.Context(self.repository_ctxt, pr)

        # Test underscore/dash attributes
        assert await condition_value_querier.PullRequest(ctxt).review_requested == []
        assert (
            await condition_value_querier.PullRequest(
                ctxt,
            ).branch_protection_review_decision
            is None
        )
        assert (
            await condition_value_querier.PullRequest(ctxt).review_threads_resolved
            == []
        )

        with pytest.raises(AttributeError):
            assert await condition_value_querier.PullRequest(ctxt).foobar

        # Test items
        assert list(condition_value_querier.PullRequest(ctxt)) == list(
            condition_value_querier.PullRequest.STRING_ATTRIBUTES
            | condition_value_querier.PullRequest.NUMBER_ATTRIBUTES
            | condition_value_querier.PullRequest.BOOLEAN_ATTRIBUTES
            | condition_value_querier.PullRequest.LIST_ATTRIBUTES
            | condition_value_querier.PullRequest.LIST_ATTRIBUTES_WITH_LENGTH_OPTIMIZATION,
        )
        commit = (await ctxt.commits)[0]
        assert {
            attr: await getattr(condition_value_querier.PullRequest(ctxt), attr)
            for attr in sorted(condition_value_querier.PullRequest(ctxt))
        } == {
            "#commits": 1,
            "#commits-behind": 2,
            "#files": 1,
            "number": pr["number"],
            "queue-dequeue-reason": None,
            "closed": False,
            "locked": False,
            "assignee": [],
            "assignees": [],
            "approved-reviews-by": [],
            "files": ["test2"],
            "added-files": ["test2"],
            "modified-files": [],
            "removed-files": [],
            "check-neutral": [],
            "status-neutral": [],
            "co-authors": set(),
            "commented-reviews-by": [],
            "commits-unverified": [
                "test_draft_attribute: pull request n2 from integration",
            ],
            "milestone": "",
            "label": [],
            "linear-history": True,
            "body": "test_draft_attribute: pull request n2 from integration",
            "body-raw": "test_draft_attribute: pull request n2 from integration",
            "base": self.main_branch_name,
            "review-requested": [],
            "review-threads-resolved": [],
            "review-threads-unresolved": [],
            "check-success": ["Summary"],
            "status-success": ["Summary"],
            "branch-protection-review-decision": None,
            "changes-requested-reviews-by": [],
            "merged": False,
            "commits": [commit],
            "head": self.get_full_branch_name("integration/pr2"),
            "head-repo-full-name": pr["head"]["repo"]["full_name"],
            "author": self.RECORD_CONFIG["app_user_login"],
            "dismissed-reviews-by": [],
            "merged-by": "",
            "queue-position": -1,
            "repository-full-name": self.repository_ctxt.repo["full_name"],
            "repository-name": self.repository_ctxt.repo["name"],
            "check-failure": [],
            "status-failure": [],
            "title": "test_draft_attribute: pull request n2 from integration",
            "conflict": False,
            "check-pending": ["continuous-integration/fake-ci"],
            "check-stale": [],
            "check-success-or-neutral": ["Summary"],
            "check-skipped": [],
            "check-timed-out": [],
            "queue-name": None,
        }

    async def test_repo_name_full_right(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": [
                        f"repository-full-name={self.repository_ctxt.repo['full_name']}",
                    ],
                    "actions": {"comment": {"message": "repository name full"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "repository name full"

    async def test_repo_name_full_wrong(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": [
                        f"repository-full-name!={self.repository_ctxt.repo['name']}",
                    ],
                    "actions": {"comment": {"message": "repository name full (wrong)"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "repository name full (wrong)"

    async def test_repo_name_short_wrong(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": [
                        f"repository-name!={self.repository_ctxt.repo['full_name']}",
                    ],
                    "actions": {
                        "comment": {"message": "repository name short (wrong)"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "repository name short (wrong)"

    async def test_repo_name_short_right(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": [
                        f"repository-name={self.repository_ctxt.repo['name']}",
                    ],
                    "actions": {"comment": {"message": "repository name short"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": ["continuous-integration/fake-ci"],
            },
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "repository name short"

    async def test_and_or(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": [
                        {
                            "or": [
                                {
                                    "and": [
                                        f"base={self.main_branch_name}",
                                        "closed",
                                        "label=foo",
                                    ],
                                },
                                "merged",
                                {"not": {"and": ["label=foo", "label=bar"]}},
                            ],
                        },
                    ],
                    "actions": {"comment": {"message": "and or pr"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()
        await self.add_label(pr["number"], "foo")
        await self.edit_pull(pr["number"], state="closed")

        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "and or pr"

    async def test_commits_list_condition(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "list_commits",
                    "conditions": [
                        "commits~=test_commits_list_condition: pull request n1 from integration",
                    ],
                    "actions": {"comment": {"message": "list commits not empty"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "list commits not empty"

    async def test_commits_attributes_list_condition_str(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "test-commits",
                    "conditions": ["commits[-1].date_committer < 1 day ago"],
                    "actions": {"comment": {"message": "long time no see"}},
                },
            ],
        }
        start_date = datetime.datetime(2022, 12, 12, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr(commit_date=start_date)
            await self.run_full_engine()

        with time_travel(start_date + datetime.timedelta(days=2), tick=True):
            await self.run_full_engine()

            comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
            assert comment["comment"]["body"] == "long time no see"

    async def test_commits_attributes_list_condition_bool(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "verified",
                    "conditions": ["commits[-1].commit_verification_verified"],
                    "actions": {"comment": {"message": "verified is good"}},
                },
                {
                    "name": "not verified",
                    "conditions": ["-commits[-1].commit_verification_verified"],
                    "actions": {"comment": {"message": "verified is not good"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr1 = await self.create_pr(verified=True)
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr1["number"]), "created")
        assert comment["comment"]["body"] == "verified is good"

        pr2 = await self.create_pr(verified=False)
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr2["number"]), "created")
        assert comment["comment"]["body"] == "verified is not good"

    async def test_commits_attributes_list_all_condition_str(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-wip",
                    "conditions": ["commits[*].commit_message~=wip"],
                    "actions": {"comment": {"message": "no wip allowed"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        # Create two prs with the same commits message reversed
        # just to be sure that the detection works when the commit is the first
        # and when it is not.
        pr1 = await self.create_pr_with_specific_commits(
            [
                "wip: test",
                "feat: really useful feature",
            ],
            [
                {"wip/test.txt": "testwip"},
                {
                    "feat/feat.txt": "very feature, much wow",
                },
            ],
        )

        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr1["number"]), "created")
        assert comment["comment"]["body"] == "no wip allowed"

        pr2 = await self.create_pr_with_specific_commits(
            [
                "feat: really useful feature",
                "wip: test",
            ],
            [
                {
                    "feat/feat.txt": "very feature, much wow",
                },
                {"wip/test.txt": "testwip"},
            ],
        )
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr2["number"]), "created")
        assert comment["comment"]["body"] == "no wip allowed"

    async def test_commits_attributes_list_all_condition_bool(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no non verified",
                    "conditions": ["-commits[*].commit_verification_verified"],
                    "actions": {
                        "comment": {"message": "no non-verified commits allowed"},
                    },
                },
                {
                    "name": "yes verified",
                    "conditions": ["commits[*].commit_verification_verified"],
                    "actions": {"comment": {"message": "verified is life"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        pr1 = await self.create_pr(two_commits=True, verified=False)

        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr1["number"]), "created")
        assert comment["comment"]["body"] == "no non-verified commits allowed"

        pr2 = await self.create_pr(two_commits=True, verified=True)
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr2["number"]), "created")
        assert comment["comment"]["body"] == "verified is life"

    async def test_one_commit_unverified(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": ["#commits-unverified=1"],
                    "actions": {"comment": {"message": "commits unverified"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(two_commits=False)
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "commits unverified"

    async def test_two_commits_unverified(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": ["#commits-unverified=2"],
                    "actions": {"comment": {"message": "commits unverified"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(two_commits=True)
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "commits unverified"

    async def test_one_commit_unverified_message(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": [
                        'commits-unverified="test_one_commit_unverified_message: pull request n1 from integration"',
                    ],
                    "actions": {"comment": {"message": "commits unverified"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(two_commits=True)
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "commits unverified"

    async def test_one_commit_unverified_message_wrong(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": ['commits-unverified="foo"'],
                    "actions": {"comment": {"message": "foo test"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(two_commits=True)
        await self.run_engine()
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 0

    async def test_one_commit_verified(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": ["#commits-unverified=0"],
                    "actions": {"comment": {"message": "commits verified"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(verified=True)
        ctxt = context.Context(self.repository_ctxt, pr)
        assert len(await ctxt.commits) == 1
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "commits verified"

    async def test_two_commits_verified(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "commits-unverified",
                    "conditions": ["#commits-unverified=0"],
                    "actions": {"comment": {"message": "commits verified"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(verified=True, two_commits=True)
        ctxt = context.Context(self.repository_ctxt, pr)
        assert len(await ctxt.commits) == 2
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "commits verified"

    async def test_retrieve_zero_resolved_threads(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "resolved-threads",
                    "conditions": ["#review-threads-resolved=0"],
                    "actions": {
                        "comment": {"message": "review-threads-resolved comment"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(files={"yves_testing_file": "foo"})
        await self.create_review_thread(
            pr["number"],
            "you shouldn't write `foo` here",
            path="yves_testing_file",
        )
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "review-threads-resolved comment"

        review_threads = await self.get_review_comments(pull_number=pr["number"])
        assert "you shouldn't write `foo` here" == review_threads[-1]["body"]

    async def test_retrieve_unresolved_threads(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "resolved-threads",
                    "conditions": [
                        'review-threads-unresolved="why are you still writing `foo` here ?"',
                        'review-threads-unresolved="How original"',
                        '''review-threads-unresolved="Don't like this line too much either"''',
                        "#review-threads-unresolved=3",
                    ],
                    "actions": {
                        "comment": {"message": "review-threads-unresolved comment"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(
            files={"yves_testing_file": "foo", "super_original_testfile": "42\ntest\n"},
        )
        await self.create_review_thread(
            pr["number"],
            "why are you still writing `foo` here ?",
            path="yves_testing_file",
        )
        comment_id = await self.create_review_thread(
            pr["number"],
            "How original",
            path="super_original_testfile",
        )
        await self.create_review_thread(
            pr["number"],
            "Don't like this line too much either",
            path="super_original_testfile",
            line=2,
        )
        await self.reply_to_review_comment(pr["number"], "much originality", comment_id)
        await self.reply_to_review_comment(
            pr["number"],
            "...maybe too original",
            comment_id,
        )
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "review-threads-unresolved comment"

        review_threads = await self.get_review_comments(pull_number=pr["number"])
        assert "...maybe too original" == review_threads[4]["body"]
        assert "much originality" == review_threads[3]["body"]
        assert "Don't like this line too much either" == review_threads[2]["body"]
        assert "How original" == review_threads[1]["body"]
        assert "why are you still writing `foo` here ?", review_threads[0]["body"]

    async def test_retrieve_resolved_and_unresolved_threads(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "resolved-threads",
                    "conditions": [
                        "#review-threads-resolved=2",
                        "#review-threads-unresolved=1",
                        'review-threads-resolved="yes, you can"',
                        'review-threads-unresolved="INDEED"',
                    ],
                    "actions": {
                        "comment": {"message": "conditions matched; s u c c e s s"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(
            files={
                "testing_file_number_one": "p e r f e c t i o n",
                "another_one": "can't stop testing",
                "this_file_will_change_your_life": "just kidding",
            },
        )
        await self.create_review_thread(
            pr["number"],
            "yes, you can",
            path="another_one",
        )
        await self.create_review_thread(
            pr["number"],
            "INDEED",
            path="testing_file_number_one",
        )
        await self.create_review_thread(
            pr["number"],
            ":'(",
            path="this_file_will_change_your_life",
        )
        thread = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]
        assert not thread["isResolved"]
        is_resolved = await self.resolve_review_thread(thread_id=thread["id"])
        assert is_resolved

        thread = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]
        assert thread["isResolved"]

        thread_2 = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][2]["node"]
        assert not thread_2["isResolved"]

        is_resolved = await self.resolve_review_thread(thread_id=thread_2["id"])
        assert is_resolved

        thread_2 = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][2]["node"]
        assert thread_2["isResolved"]

        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "conditions matched; s u c c e s s"

    async def test_retrieve_message_resolved_thread(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "resolved-threads",
                    "conditions": [
                        'review-threads-resolved="nothing... I guess"',
                    ],
                    "actions": {
                        "comment": {
                            "message": "review-thread-resolved comment showing success",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(
            files={
                "another_amazing_testing_file": "what did you expect ?",
                "love to test": "noice",
            },
        )
        await self.create_review_thread(
            pr["number"],
            "nothing... I guess",
            path="another_amazing_testing_file",
        )
        thread = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]
        assert not thread["isResolved"]
        is_resolved = await self.resolve_review_thread(thread_id=thread["id"])
        assert is_resolved
        thread = (await self.get_review_threads(pr["number"]))["repository"][
            "pullRequest"
        ]["reviewThreads"]["edges"][0]["node"]
        assert thread["isResolved"]
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert (
            comment["comment"]["body"]
            == "review-thread-resolved comment showing success"
        )

    async def test_review_decision(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "review decision APPROVE",
                    "conditions": ["branch-protection-review-decision=APPROVED"],
                    "actions": {"comment": {"message": "approved"}},
                },
                {
                    "name": "review decision REVIEW_REQUIRED",
                    "conditions": ["branch-protection-review-decision=REVIEW_REQUIRED"],
                    "actions": {"comment": {"message": "review-required"}},
                },
                {
                    "name": "review decision CHANGES_REQUESTED",
                    "conditions": [
                        "branch-protection-review-decision=CHANGES_REQUESTED",
                    ],
                    "actions": {"comment": {"message": "changes-requested"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        await self.push_file("CODEOWNERS", "*.py @mergify-test1")

        await self.branch_protection_protect(
            self.main_branch_name,
            {
                "required_pull_request_reviews": {
                    "require_code_owner_reviews": True,
                    "required_approving_review_count": 1,
                },
                "enforce_admins": None,
                "restrictions": None,
                "required_status_checks": None,
            },
        )

        pr = await self.create_pr(files={"test.py": "ok"})
        await self.run_engine()

        if len(pr["requested_reviewers"]) > 0:
            assert len(pr["requested_reviewers"]) == 1
            assert pr["requested_reviewers"][0]["login"] == "mergify-test1"
        else:
            pr_updated = await self.wait_for_pull_request("review_requested")
            assert len(pr_updated["pull_request"]["requested_reviewers"]) == 1
            assert (
                pr_updated["pull_request"]["requested_reviewers"][0]["login"]
                == "mergify-test1"
            )

        comment_1 = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment_1["comment"]["body"] == "review-required"

        await self.create_review(
            pr["number"],
            oauth_token=settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
            event="REQUEST_CHANGES",
        )
        await self.run_engine()

        comment_2 = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment_2["comment"]["body"] == "changes-requested"

        await self.create_review(
            pr["number"],
            oauth_token=settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
        )
        await self.run_engine()

        comment_3 = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment_3["comment"]["body"] == "approved"

    async def test_current_datetime(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Comment on New Year's Day 2024",
                    "conditions": ["current-datetime>=2024-01-01T00:00[Europe/Paris]"],
                    "actions": {"comment": {"message": "Happy New Year!"}},
                },
            ],
        }

        with time_travel("2023-12-31T22:00:00Z", tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr()

            await self.run_full_engine()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            assert await self.redis_links.cache.zcard("delayed-refresh") == 1

        with time_travel("2024-01-01T00:00:00Z", tick=True):
            await self.run_full_engine()

            comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
            assert comment["comment"]["body"] == "Happy New Year!"

    async def test_current_datetime_range(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Comment on New Year's Day 2024",
                    "conditions": [
                        "current-datetime=2024-01-01T00:00/2024-01-01T23:59[Europe/Paris]",
                    ],
                    "actions": {"comment": {"message": "Happy New Year!"}},
                },
            ],
        }

        with time_travel("2023-12-31T23:59+01", tick=True):
            await self.setup_repo(yaml.dump(rules))
            pr = await self.create_pr()

            await self.run_full_engine()
            comments = await self.get_issue_comments(pr["number"])
            assert len(comments) == 0

            assert await self.redis_links.cache.zcard("delayed-refresh") == 1

        with time_travel("2024-01-01T00:00+01", tick=True):
            await self.run_full_engine()

            comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
            assert comment["comment"]["body"] == "Happy New Year!"

    async def test_head_repo_full_name(self) -> None:
        head_repo_full_name_og = f"{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}"
        head_repo_full_name_fork = (
            f"mergify-test2/{self.RECORD_CONFIG['repository_name']}"
        )
        rules = {
            "pull_request_rules": [
                {
                    "name": "test-on-origin",
                    "conditions": [f"head-repo-full-name={head_repo_full_name_og}"],
                    "actions": {
                        "comment": {
                            "message": "head-repo-full-name={{ head_repo_full_name }}",
                        },
                    },
                },
                {
                    "name": "test-on-fork",
                    "conditions": [f"head-repo-full-name={head_repo_full_name_fork}"],
                    "actions": {
                        "comment": {
                            "message": "head-repo-full-name={{ head_repo_full_name }}",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(p1["number"]), "created")
        assert (
            comment["comment"]["body"]
            == f"head-repo-full-name={head_repo_full_name_og}"
        )

        p2 = await self.create_pr(as_="fork")
        await self.run_engine()
        comment = await self.wait_for_issue_comment(str(p2["number"]), "created")
        assert (
            comment["comment"]["body"]
            == f"head-repo-full-name={head_repo_full_name_fork}"
        )


class TestAttributesWithSub(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_depends_on(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=automerge",
                    ],
                    "actions": {"merge": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        pr1 = await self.create_pr()
        pr2 = await self.create_pr()
        await self.merge_pull(pr2["number"])

        body = f"Awesome body\nDepends-On: #{pr1['number']}\ndepends-on: #{pr2['number']}\ndepends-On: #9999999"
        pr = await self.create_pr(message=body)
        pr_labeled = await self.add_label(pr["number"], "automerge")
        await self.run_engine()

        await self.reload_repository_ctxt_configuration()
        ctxt = context.Context(self.repository_ctxt, pr_labeled["pull_request"])
        assert ctxt.get_depends_on() == [pr1["number"], pr2["number"], 9999999]
        assert await condition_value_querier.PullRequest(ctxt)._get_consolidated_data(
            ctxt,
            "depends-on",
        ) == [f"#{pr2['number']}"]

        repo_url = ctxt.pull["base"]["repo"]["html_url"]
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        expected = f"""### Rule: merge (merge)
- [ ] `depends-on=#{pr1['number']}` [⛓️ **test_depends_on: pull request n1 from integration** ([#{pr1['number']}]({repo_url}/pull/{pr1['number']}))]
- [ ] `depends-on=#9999999` [⛓️ ⚠️ *pull request not found* (#9999999)]
- [X] `-conflict` [:pushpin: merge requirement]
- [X] `-draft` [:pushpin: merge requirement]
- [X] `-mergify-configuration-changed` [:pushpin: merge -> allow_merging_configuration_change setting requirement]
- [X] `base={self.main_branch_name}`
- [X] `depends-on=#{pr2['number']}` [⛓️ **test_depends_on: pull request n2 from integration** ([#{pr2['number']}]({repo_url}/pull/{pr2['number']}))]
- [X] `label=automerge`
"""
        assert expected in summary["output"]["summary"]

    async def test_merge_after(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=automerge",
                    ],
                    "actions": {"merge": {}},
                },
            ],
        }
        # 12:00 18th April 2023
        start_date = datetime.datetime(2023, 4, 18, 12, tzinfo=date.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            body = "Awesome body\nMerge-After: 2023-04-19"
            pr = await self.create_pr(message=body)
            pr_labeled = await self.add_label(pr["number"], "automerge")
            await self.run_engine()

            ctxt = context.Context(self.repository_ctxt, pr_labeled["pull_request"])
            assert ctxt.get_merge_after() == datetime.datetime(
                2023,
                4,
                19,
                tzinfo=date.UTC,
            )

            summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
            assert summary is not None
            expected = f"""### Rule: merge (merge)
- [ ] `current-datetime>=2023-04-19T00:00:00` [🕒 Merge-After: 2023-04-19T00:00:00+00:00]
- [X] `-conflict` [:pushpin: merge requirement]
- [X] `-draft` [:pushpin: merge requirement]
- [X] `-mergify-configuration-changed` [:pushpin: merge -> allow_merging_configuration_change setting requirement]
- [X] `base={self.main_branch_name}`
- [X] `label=automerge`
"""
            assert expected in summary["output"]["summary"]

            pr = await self.get_pull(pr["number"])
            assert not pr["merged"]

        with time_travel(start_date + datetime.timedelta(days=1), tick=True):
            await self.run_full_engine()

            p_merged = await self.wait_for_pull_request("closed", pr["number"])
            assert p_merged["pull_request"]["merged"]

    async def test_statuses_error(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "statuses-error-state",
                    "conditions": ["check-failure=sick-ci"],
                    "actions": {"post_check": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 2
        assert "failure" == sorted_checks[0]["conclusion"]

        await self.create_status(p, "sick-ci", "error")
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 2
        assert "success" == sorted_checks[0]["conclusion"]

    async def test_check_run_failure(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "check-run-failure-state",
                    "conditions": ["check-failure=sick-ci"],
                    "actions": {"post_check": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 2
        assert "failure" == sorted_checks[0]["conclusion"]

        await self.create_check_run(p, "sick-ci", "failure")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 3
        assert sorted_checks[0]["name"] == "Rule: check-run-failure-state (post_check)"
        assert sorted_checks[0]["conclusion"] == "success"

    async def test_check_run_timeout(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "check-run-timeout-state",
                    "conditions": ["check-timed-out=lazy-ci"],
                    "actions": {"post_check": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 2
        assert "failure" == sorted_checks[0]["conclusion"]

        await self.create_check_run(p, "lazy-ci", "timed_out")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 3
        assert sorted_checks[0]["name"] == "Rule: check-run-timeout-state (post_check)"
        assert sorted_checks[0]["conclusion"] == "success"

    async def test_linear_history(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "noways",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "-linear-history",
                    ],
                    "actions": {"close": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.merge_pull(p1["number"])

        await self.client_integration.put(
            f"{self.repository_ctxt.base_url}/pulls/{p2['number']}/update-branch",
            api_version="lydian",
            json={"expected_head_sha": p2["head"]["sha"]},
        )
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

    async def test_queued(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=ready-to-merge",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Add queued label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "queue-position>=0",
                    ],
                    "actions": {"label": {"add": ["queued"]}},
                },
                {
                    "name": "Remove queued label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "queue-position=-1",
                    ],
                    "actions": {"label": {"remove": ["queued"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()

        await self.add_label(p["number"], "ready-to-merge")
        await self.run_engine()

        p_labeled = await self.wait_for_pull_request("labeled")
        assert "queued" in [
            label["name"] for label in p_labeled["pull_request"]["labels"]
        ]

        await self.remove_label(p["number"], "ready-to-merge")
        await self.run_engine()

        p_unlabeled = await self.wait_for_pull_request("unlabeled")
        assert "queued" not in [
            label["name"] for label in p_unlabeled["pull_request"]["labels"]
        ]

    async def test_dependabot_attributes(self) -> None:
        rules_config = [
            ("dependabot-dependency-name", "bootstrap"),
            ("dependabot-dependency-type", " direct:development"),
            ("dependabot-update-type", "version-update:semver-minor"),
        ]
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge if dependabot properties are checked",
                    "conditions": [
                        f"{property_name}={property_value}"
                        for property_name, property_value in rules_config
                    ],
                    "actions": {"comment": {"message": "dependabot was here"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr(
            commit_headline="chore(deps-dev): bump bootstrap from 5.1.3 to 5.2.0 in /docs",
            commit_body="""Bumps [bootstrap](https://github.com/twbs/bootstrap) from 5.1.3 to 5.2.0.
            - [Release notes](https://github.com/twbs/bootstrap/releases)
            - [Commits](https://github.com/twbs/bootstrap/compare/v5.1.3...v5.2.0)

            ---
            updated-dependencies:
            - dependency-name: bootstrap
              dependency-type: direct:development
              update-type: version-update:semver-minor
            ...

            Signed-off-by: dependabot[bot] <support@github.com>
            """,
        )

        with mock.patch(
            "mergify_engine.constants.DEPENDABOT_PULL_REQUEST_AUTHOR_LOGIN",
            self.RECORD_CONFIG["app_user_login"],
        ):
            await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert comment["comment"]["body"] == "dependabot was here"

    async def test_queue_attributes(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": ["label=hotfix"],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Add to merge queue",
                    "conditions": ["label=queue"],
                    "actions": {"queue": {}},
                },
                {
                    "name": "Label queue name",
                    "conditions": ["queue-position>=0"],
                    "actions": {"label": {"toggle": ["queued: {{queue_name}}"]}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()
        labelled_p1 = await self.wait_for_pull_request(action="labeled")
        assert "queued: default" in [
            label["name"] for label in labelled_p1["pull_request"]["labels"]
        ]

        p2 = await self.create_pr()
        await self.add_label(p2["number"], "hotfix")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()
        labelled_p2 = await self.wait_for_pull_request(action="labeled")
        assert "queued: hotfix" in [
            label["name"] for label in labelled_p2["pull_request"]["labels"]
        ]
