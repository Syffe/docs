import typing

import anys
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models.github import account as gh_account_mod
from mergify_engine.models.github import pull_request as gh_pull_request_mod
from mergify_engine.tests.functional import base


class TestGitHubPullRequestInPg(base.FunctionalTestBase):
    async def test_pull_request_inserted_and_updated(self) -> None:
        await self.setup_repo()

        p1 = await self.create_pr()

        p1_typing_validated = (
            gh_pull_request_mod.PullRequest.type_adapter.validate_python(p1)
        )

        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                await session.scalars(
                    sqlalchemy.select(gh_pull_request_mod.PullRequest),
                )
            ).all()
            assert len(pulls_in_db) == 1
            assert pulls_in_db[0].as_github_dict() == p1_typing_validated

        p1_labeled = await self.add_label(p1["number"], "test")
        p1_typing_validated = (
            gh_pull_request_mod.PullRequest.type_adapter.validate_python(
                p1_labeled["pull_request"],
            )
        )
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                await session.scalars(
                    sqlalchemy.select(gh_pull_request_mod.PullRequest),
                )
            ).all()
            assert len(pulls_in_db) == 1
            assert pulls_in_db[0].as_github_dict() == p1_typing_validated
            assert pulls_in_db[0].labels == [
                {
                    "id": anys.ANY_INT,
                    "name": "test",
                    "color": anys.ANY_STR,
                    "default": anys.ANY_BOOL,
                },
            ]

        p1_with_review_req = await self.create_review_request(
            p1["number"],
            ["mergify-test1", "mergify-test4"],
        )
        p1_typing_validated = (
            gh_pull_request_mod.PullRequest.type_adapter.validate_python(
                p1_with_review_req["pull_request"],
            )
        )
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                await session.scalars(
                    sqlalchemy.select(gh_pull_request_mod.PullRequest),
                )
            ).all()
            assert len(pulls_in_db) == 1
            assert pulls_in_db[0].as_github_dict() == p1_typing_validated

            assert isinstance(
                pulls_in_db[0].requested_reviewers[0],
                gh_account_mod.GitHubAccount,
            )
            assert pulls_in_db[0].as_github_dict()["requested_reviewers"] == [
                typing.cast(
                    github_types.GitHubAccount,
                    {
                        "id": anys.ANY_INT,
                        "login": "mergify-test1",
                        "type": "User",
                        "avatar_url": anys.ANY_STR,
                    },
                ),
                typing.cast(
                    github_types.GitHubAccount,
                    {
                        "id": anys.ANY_INT,
                        "login": "mergify-test4",
                        "type": "User",
                        "avatar_url": anys.ANY_STR,
                    },
                ),
            ]

    async def test_pull_request_first_insertion_with_reviewers(self) -> None:
        await self.setup_repo()

        p1 = await self.create_pr(forward_event_to_engine=False)
        p1_with_review_req = await self.create_review_request(
            p1["number"],
            ["mergify-test1", "mergify-test4"],
        )

        p1_typing_validated = (
            gh_pull_request_mod.PullRequest.type_adapter.validate_python(
                p1_with_review_req["pull_request"],
            )
        )

        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                await session.scalars(
                    sqlalchemy.select(gh_pull_request_mod.PullRequest),
                )
            ).all()
            assert len(pulls_in_db) == 1
            assert pulls_in_db[0].as_github_dict() == p1_typing_validated

            assert isinstance(
                pulls_in_db[0].requested_reviewers[0],
                gh_account_mod.GitHubAccount,
            )
            assert pulls_in_db[0].as_github_dict()["requested_reviewers"] == [
                typing.cast(
                    github_types.GitHubAccount,
                    {
                        "id": anys.ANY_INT,
                        "login": "mergify-test1",
                        "type": "User",
                        "avatar_url": anys.ANY_STR,
                    },
                ),
                typing.cast(
                    github_types.GitHubAccount,
                    {
                        "id": anys.ANY_INT,
                        "login": "mergify-test4",
                        "type": "User",
                        "avatar_url": anys.ANY_STR,
                    },
                ),
            ]
            assert len(pulls_in_db) == 1
            assert pulls_in_db[0].as_github_dict() == p1_typing_validated
