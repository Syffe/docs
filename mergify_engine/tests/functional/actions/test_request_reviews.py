from unittest import mock

import pytest

from mergify_engine import context
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.actions import request_reviews
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestRequestReviewsAction(base.FunctionalTestBase):
    async def test_request_reviews_users(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    # The random case matter
                    "actions": {"request_reviews": {"users": ["MeRgiFy-teSt1"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        pr = await self.wait_for_pull_request("review_requested")
        assert sorted(["mergify-test1"]) == sorted(
            user["login"] for user in pr["pull_request"]["requested_reviewers"]
        )

        for review_type in ("APPROVE", "REQUEST_CHANGES"):
            await self.create_review(pr["number"], review_type)  # type: ignore[arg-type]
            await self.run_engine()

            requests = await self.get_review_requests(pr["number"])
            assert len(requests["users"]) == 0

    async def test_request_reviews_teams(self) -> None:
        team = (await self.get_teams())[0]

        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    # The wrong team case matter
                    "actions": {"request_reviews": {"teams": [team["slug"].upper()]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        pr = await self.wait_for_pull_request("review_requested")
        assert sorted([team["slug"]]) == sorted(
            team["slug"] for team in pr["pull_request"]["requested_teams"]
        )

    async def test_request_reviews_teams_with_organization(self) -> None:
        team = (await self.get_teams())[0]

        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    # The wrong team case matter
                    "actions": {
                        "request_reviews": {
                            "teams": [
                                f"{settings.TESTING_ORGANIZATION_NAME}/{team['slug'].upper()}",
                            ],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        pr = await self.wait_for_pull_request("review_requested")
        assert sorted([team["slug"]]) == sorted(
            team["slug"] for team in pr["pull_request"]["requested_teams"]
        )

    async def test_request_reviews_unknown_reviewer(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"request_reviews": {"users": ["octocat"]}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        await self.create_pr()
        await self.run_engine()

        failed_check_run = await self.wait_for_check_run(
            conclusion="failure",
            name="Rule: request_reviews (request_reviews)",
        )
        assert (
            failed_check_run["check_run"]["output"]["title"]
            == "Unable to create review request"
        )
        assert (
            failed_check_run["check_run"]["output"]["summary"]
            == f"GitHub error: [422] `Reviews may only be requested from collaborators. One or more of the users or "
            f"teams you specified is not a collaborator of the {self.RECORD_CONFIG['organization_name']}"
            f"/{self.RECORD_CONFIG['repository_name']} repository.`"
        )

    @mock.patch.object(
        request_reviews.RequestReviewsExecutor,
        "GITHUB_MAXIMUM_REVIEW_REQUEST",
        new=1,
    )
    async def test_request_reviews_already_max(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "approve",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"review": {"type": "APPROVE"}},
                },
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "request_reviews": {"users": ["mergify-test1", "mergify-test"]},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr(as_="fork")
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("review_requested")
        assert ["mergify-test1"] == [
            user["login"] for user in p_updated["pull_request"]["requested_reviewers"]
        ]

        ctxt = context.Context(self.repository_ctxt, p_updated["pull_request"], [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        for check in checks:
            if check["name"] == "Rule: request_reviews (request_reviews)":
                assert check["conclusion"] == "neutral"
                assert (
                    check["output"]["title"]
                    == "Maximum number of reviews already requested"
                )
                assert (
                    check["output"]["summary"]
                    == "The maximum number of 1 reviews has been reached.\n"
                    "Unable to request reviews for additional users."
                )
                break
        else:
            pytest.fail("Unable to find request review check run")

    @mock.patch.object(
        request_reviews.RequestReviewsExecutor,
        "GITHUB_MAXIMUM_REVIEW_REQUEST",
        new=2,
    )
    async def test_request_reviews_going_above_max(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#review-requested>0",
                    ],
                    "actions": {
                        "request_reviews": {
                            "users": ["mergify-test1", "mergify-test4"],
                            "teams": ["mergifyio-testing/testing"],
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.create_review_request(p["number"], ["mergify-test1"])
        await self.run_engine()

        p_updated = await self.wait_for_pull_request("review_requested")
        assert sorted(["mergify-test1", "mergify-test4"]) == sorted(
            user["login"] for user in p_updated["pull_request"]["requested_reviewers"]
        )

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        for check in checks:
            if check["name"] == "Rule: request_reviews (request_reviews)":
                assert check["conclusion"] == "neutral"
                assert (
                    check["output"]["title"]
                    == "Maximum number of reviews already requested"
                )
                assert (
                    check["output"]["summary"]
                    == "The maximum number of 2 reviews has been reached.\n"
                    "Unable to request reviews for additional users."
                )
                break
        else:
            pytest.fail("Unable to find request review check run")


class TestRequestReviewsSubAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_request_reviews_users_from_teams(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "request_reviews",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "request_reviews": {
                            "users_from_teams": ["testing", "bot"],
                            "random_count": 2,
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        pr = await self.wait_for_pull_request("review_requested")
        assert len(pr["pull_request"]["requested_reviewers"]) == 2
        assert len(pr["pull_request"]["requested_teams"]) == 0
