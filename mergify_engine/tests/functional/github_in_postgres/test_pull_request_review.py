from unittest import mock

import pytest
import sqlalchemy

from mergify_engine import context
from mergify_engine import database
from mergify_engine.clients import github
from mergify_engine.models.github import pull_request_review as pr_review_model
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


@pytest.mark.usefixtures("_enable_github_in_postgres")
class TestGitHubPullRequestReview(base.FunctionalTestBase):
    async def test_pull_request_reviews_stored_and_used(self) -> None:
        await self.setup_repo(yaml.dump({}))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.create_review(p1["number"], "APPROVE")
        await self.create_review(p2["number"], "APPROVE")
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            reviews_from_db = (
                await session.scalars(
                    sqlalchemy.select(pr_review_model.PullRequestReview),
                )
            ).all()

            assert len(reviews_from_db) == 2

        last_review_id_p1 = await self.create_review(p1["number"], "REQUEST_CHANGES")
        await self.create_review(p2["number"], "APPROVE")
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            reviews_from_db = (
                await session.scalars(
                    sqlalchemy.select(pr_review_model.PullRequestReview),
                )
            ).all()

            assert len(reviews_from_db) == 4

        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            # There's 2 reviews for p1 but it should only return one since
            # they're both from the same actor
            reviews = await ctxt_p1.reviews
            assert len(reviews) == 1
            assert reviews[0]["state"] == "CHANGES_REQUESTED"

        await self.dismiss_review(p1["number"], last_review_id_p1)
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            reviews_from_db = (
                await session.scalars(
                    sqlalchemy.select(pr_review_model.PullRequestReview),
                )
            ).all()

            assert len(reviews_from_db) == 4

        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            reviews = await ctxt_p1.reviews
            assert len(reviews) == 1
            assert reviews[0]["state"] == "DISMISSED"
