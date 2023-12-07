import anys
import pytest

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(
    subscription.Features.WORKFLOW_AUTOMATION,
)
class TestDismissReviewsAction(base.FunctionalTestBase):
    @pytest.mark.subscription(
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_dismiss_reviews(self) -> None:
        await self._test_dismiss_reviews()

    @pytest.mark.subscription(
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_dismiss_reviews_custom_message(self) -> None:
        await self._test_dismiss_reviews(message="Loser")

    async def _push_for_synchronize(
        self,
        branch: str,
        filename: str = "unwanted_changes",
        remote: str = "origin",
    ) -> None:
        open(self.git.repository + f"/{filename}", "wb").close()
        await self.git("add", self.git.repository + f"/{filename}")
        await self.git("commit", "--no-edit", "-m", filename)
        await self.git("push", "--quiet", remote, branch)

    async def _test_dismiss_reviews_fail(self, msg: str) -> github_types.GitHubCheckRun:
        rules = {
            "pull_request_rules": [
                {
                    "name": "dismiss reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "dismiss_reviews": {
                            "message": msg,
                            "approved": True,
                            "changes_requested": ["mergify-test1"],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        branch = self.get_full_branch_name(f"integration/pr{self.pr_counter}")
        await self.create_review(p["number"], "APPROVE")

        assert [("APPROVED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        await self._push_for_synchronize(branch)

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        check_run = await self.wait_for_check_run(conclusion="failure")
        assert (
            "The current Mergify configuration is invalid"
            == check_run["check_run"]["output"]["title"]
        )
        return check_run["check_run"]

    async def test_dismiss_reviews_custom_message_syntax_error(self) -> None:
        check = await self._test_dismiss_reviews_fail("{{Loser")
        assert """Template syntax error @ pull_request_rules → item 0 → actions → dismiss_reviews → message → line 1
```
unexpected end of template, expected 'end of print statement'.
```""" == check["output"]["summary"]

    async def test_dismiss_reviews_custom_message_attribute_error(self) -> None:
        check = await self._test_dismiss_reviews_fail("{{Loser}}")
        assert """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → dismiss_reviews → message
```
Unknown pull request attribute: Loser
```""" == check["output"]["summary"]

    async def _test_dismiss_reviews(
        self,
        message: None | str = None,
    ) -> github_types.GitHubPullRequest:
        rules = {
            "pull_request_rules": [
                {
                    "name": "dismiss reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "dismiss_reviews": {
                            "approved": True,
                            "changes_requested": ["mergify-test1"],
                        },
                    },
                },
            ],
        }

        if message is not None:
            rules["pull_request_rules"][0]["actions"]["dismiss_reviews"][  # type: ignore[index]
                "message"
            ] = message

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.create_review(p["number"], "APPROVE")

        assert [("APPROVED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        await self._push_for_synchronize(p["head"]["ref"])
        await self.wait_for("pull_request", {"action": "synchronize"})

        await self.run_engine()
        await self.wait_for("pull_request_review", {"action": "dismissed"})

        assert [("DISMISSED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        await self.create_review(p["number"], "REQUEST_CHANGES")

        assert [
            ("DISMISSED", "mergify-test1"),
            ("CHANGES_REQUESTED", "mergify-test1"),
        ] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        await self._push_for_synchronize(p["head"]["ref"], "unwanted_changes2")
        await self.wait_for("pull_request", {"action": "synchronize"})

        await self.run_engine()
        await self.wait_for("pull_request_review", {"action": "dismissed"})

        # There's no way to retrieve the dismiss message :(
        assert [("DISMISSED", "mergify-test1"), ("DISMISSED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p['number']}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "repository": p["base"]["repo"]["full_name"],
                    "pull_request": p["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.dismiss_reviews",
                    "metadata": {
                        "users": ["mergify-test1"],
                    },
                    "trigger": "Rule: dismiss reviews",
                },
                {
                    "id": anys.ANY_INT,
                    "repository": p["base"]["repo"]["full_name"],
                    "pull_request": p["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.dismiss_reviews",
                    "metadata": {
                        "users": ["mergify-test1"],
                    },
                    "trigger": "Rule: dismiss reviews",
                },
            ],
            "per_page": 10,
            "size": 2,
        }
        return p

    async def test_dismiss_reviews_ignored(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "dismiss reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "dismiss_reviews": {
                            "approved": True,
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.create_review(p["number"], "APPROVE")

        assert [("APPROVED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

        # Move base branch
        await self.git("checkout", self.main_branch_name)
        await self._push_for_synchronize(self.main_branch_name)
        await self.run_engine()

        await self.create_comment_as_admin(p["number"], "@mergifyio refresh")
        await self.run_engine()

        # Ensure review have not been dismiss
        assert [("APPROVED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

    async def test_dismiss_reviews_from_requested_reviewers(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "dismiss reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "dismiss_reviews": {
                            "when": "always",
                            "approved": "from_requested_reviewers",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.create_review(p["number"], "APPROVE")
        await self.create_review(
            p["number"],
            "APPROVE",
            oauth_token=settings.TESTING_ORG_USER_PERSONAL_TOKEN,
        )

        assert [("APPROVED", "mergify-test1"), ("APPROVED", "mergify-test4")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]
        await self.create_review_request(p["number"], ["mergify-test1"])
        await self.run_engine()

        # Ensure review have been dismiss
        assert [("DISMISSED", "mergify-test1"), ("APPROVED", "mergify-test4")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]

    @pytest.mark.skipif(
        not settings.TESTING_RECORD,
        reason="This test cannot be replayed as it exercises code that relies on a timestamp from GitHub API and the event received_at",
    )
    async def test_dismiss_reviews_timing_race(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "dismiss reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"dismiss_reviews": {}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self._push_for_synchronize(p["head"]["ref"])

        await self.wait_for("pull_request", {"action": "synchronize"})

        await self.client_admin.post(
            f"{self.url_origin}/pulls/{p['number']}/reviews",
            json={"event": "APPROVE", "body": "event: APPROVE"},
        )
        await self.wait_for(
            "pull_request_review",
            {"action": "submitted"},
            forward_to_engine=False,
        )

        await self.run_engine()

        assert [("APPROVED", "mergify-test1")] == [
            (r["state"], r["user"] and r["user"]["login"])
            for r in await self.get_reviews(p["number"])
        ]
