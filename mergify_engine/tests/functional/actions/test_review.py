import anys

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.tests.functional import base


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
                        "review": {"message": "WTF?", "type": "REQUEST_CHANGES"}
                    },
                },
            ]
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
                        }
                    },
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")
        await self.run_engine()

        await self.wait_for_pull_request_review("approved")
        await self.run_engine()

        r2 = await self.wait_for_pull_request_review("changes_requested")
        assert r2["review"]["body"] == "WTF mergify-test2?"

    async def _test_review_template_error(
        self, msg: str
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
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(as_="fork")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert "failure" == checks[0]["conclusion"]
        assert (
            "The current Mergify configuration is invalid"
            == checks[0]["output"]["title"]
        )
        return checks[0]

    async def test_review_template_syntax_error(self) -> None:
        check = await self._test_review_template_error(
            msg="Thank you {{",
        )
        assert """Template syntax error @ pull_request_rules → item 0 → actions → review → message → line 1
```
unexpected 'end of template'
```""" == check["output"]["summary"]

    async def test_review_template_attribute_error(self) -> None:
        check = await self._test_review_template_error(
            msg="Thank you {{hello}}",
        )
        assert """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → review → message
```
Unknown pull request attribute: hello
```""" == check["output"]["summary"]

    async def test_review_with_oauth_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "approve",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "review": {
                            "type": "APPROVE",
                        }
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
                        }
                    },
                },
            ]
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
        self.assertEqual(2, len(reviews))

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "repository": p["base"]["repo"]["full_name"],
                    "pull_request": p["number"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "event": "action.review",
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
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "event": "action.review",
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
            "total": None,
        }
