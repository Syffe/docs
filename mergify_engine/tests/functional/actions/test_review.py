import anys

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class TestReviewAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_review_normal(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "approve",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"review": {"type": "APPROVE"}},
                },
                {
                    "name": "requested",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#approved-reviews-by>=1",
                    ],
                    "actions": {
                        "review": {"message": "WTF?", "type": "REQUEST_CHANGES"},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")
        await self.run_engine()
        await self.wait_for_pull_request_review("approved")

        await self.run_engine()
        r2 = await self.wait_for_pull_request_review("changes_requested")
        assert r2["review"]["body"] == "WTF?"

    async def test_review_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "approve",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"review": {"type": "APPROVE"}},
                },
                {
                    "name": "requested",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#approved-reviews-by>=1",
                    ],
                    "actions": {
                        "review": {
                            "message": "WTF {{author}}?",
                            "type": "REQUEST_CHANGES",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")
        await self.run_engine()

        await self.wait_for_pull_request_review("approved")
        await self.run_engine()

        r2 = await self.wait_for_pull_request_review("changes_requested")
        assert r2["review"]["body"] == "WTF mergify-test2?"

    async def _test_review_template_error(
        self,
        msg: str,
    ) -> github_types.CachedGitHubCheckRun:
        rules = {
            "pull_request_rules": [
                {
                    "name": "review",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"review": {"message": msg, "type": "REQUEST_CHANGES"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(as_="fork")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["conclusion"] == "failure"
        assert (
            checks[0]["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        return checks[0]

    async def test_review_template_syntax_error(self) -> None:
        check = await self._test_review_template_error(
            msg="Thank you {{",
        )
        assert (
            check["output"]["summary"]
            == """Template syntax error @ pull_request_rules → item 0 → actions → review → message → line 1
```
unexpected 'end of template'
```"""
        )

    async def test_review_template_attribute_error(self) -> None:
        check = await self._test_review_template_error(
            msg="Thank you {{hello}}",
        )
        assert (
            check["output"]["summary"]
            == """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → review → message
```
Unknown pull request attribute: hello
```"""
        )

    async def test_review_with_oauth_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "approve",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "review": {
                            "type": "APPROVE",
                        },
                    },
                },
                {
                    "name": "requested",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "review": {
                            "message": "WTF?",
                            "type": "REQUEST_CHANGES",
                            "bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(as_="fork", message="mergify-test4")
        await self.run_engine()

        await self.wait_for_pull_request_review("approved")
        await self.run_engine()

        r2 = await self.wait_for_pull_request_review("changes_requested")
        assert r2["review"]["body"] == "WTF?"
        assert (
            r2["review"]["user"] and r2["review"]["user"]["login"]
        ) == "mergify-test4"

        # ensure review don't get posted twice
        await self.create_comment(p["number"], "@mergifyio refresh")
        await self.run_engine()
        reviews = await self.get_reviews(p["number"])
        assert len(reviews) == 2

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
                    "type": "action.review",
                    "metadata": {
                        "message": "WTF?",
                        "review_type": "REQUEST_CHANGES",
                        "reviewer": "mergify-test4",
                    },
                    "trigger": "Rule: requested",
                },
                {
                    "id": anys.ANY_INT,
                    "repository": p["base"]["repo"]["full_name"],
                    "pull_request": p["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "type": "action.review",
                    "metadata": {
                        "message": None,
                        "review_type": "APPROVE",
                        "reviewer": self.RECORD_CONFIG["app_user_login"],
                    },
                    "trigger": "Rule: approve",
                },
            ],
            "per_page": 10,
            "size": 2,
        }
