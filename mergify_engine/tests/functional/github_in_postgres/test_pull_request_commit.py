from unittest import mock

import sqlalchemy
from sqlalchemy import orm

from mergify_engine import context
from mergify_engine import database
from mergify_engine.clients import github
from mergify_engine.models.github import pull_request as gh_pull_request_mod
from mergify_engine.models.github import pull_request_commit as gh_pullcommit_model
from mergify_engine.tests.functional import base


class TestGitHubPullRequestCommitInPg(base.FunctionalTestBase):
    async def test_pull_request_commits_from_fork(self) -> None:
        await self.setup_repo()

        await self.create_pr(as_="fork")
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                (
                    await session.scalars(
                        sqlalchemy.select(gh_pull_request_mod.PullRequest).options(
                            orm.joinedload(
                                gh_pull_request_mod.PullRequest.head_commits,
                            ),
                        ),
                    )
                )
                .unique()
                .all()
            )
            assert len(pulls_in_db) == 1
            assert len(pulls_in_db[0].head_commits) == 1

    async def test_pull_request_commits_stored(self) -> None:
        await self.setup_repo()

        p1 = await self.create_pr()
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                (
                    await session.scalars(
                        sqlalchemy.select(gh_pull_request_mod.PullRequest).options(
                            orm.joinedload(
                                gh_pull_request_mod.PullRequest.head_commits,
                            ),
                        ),
                    )
                )
                .unique()
                .all()
            )
            assert len(pulls_in_db) == 1
            assert len(pulls_in_db[0].head_commits) == 1
            first_commit_sha = pulls_in_db[0].head_commits[0].sha

        ctxt = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            commits = await ctxt.commits
            assert len(commits) == 1

        await self.push_file(destination_branch=p1["head"]["ref"])
        p1_updated = await self.wait_for_pull_request("synchronize", p1["number"])
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            pulls_in_db = (
                (
                    await session.scalars(
                        sqlalchemy.select(gh_pull_request_mod.PullRequest).options(
                            orm.joinedload(
                                gh_pull_request_mod.PullRequest.head_commits,
                            ),
                        ),
                    )
                )
                .unique()
                .all()
            )
            assert len(pulls_in_db) == 1
            assert len(pulls_in_db[0].head_commits) == 2
            new_head_commits_sha = [comm.sha for comm in pulls_in_db[0].head_commits]
            assert first_commit_sha in new_head_commits_sha

        ctxt = context.Context(self.repository_ctxt, p1_updated["pull_request"], [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            commits = await ctxt.commits
            assert len(commits) == 2

        await self.add_label(p1["number"], "test")
        with mock.patch.object(
            gh_pullcommit_model.PullRequestCommit,
            "insert_or_update",
            side_effect=AssertionError(
                "shouldn't have tried to insert or update a new commit, nothing was done on them",
            ),
        ):
            await self.run_engine({"github-in-postgres"})

        await self.change_pull_request_commit_sha(
            p1_updated["pull_request"],
            first_commit_sha,
        )
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            pulls_in_db = (
                (
                    await session.scalars(
                        sqlalchemy.select(gh_pull_request_mod.PullRequest).options(
                            orm.joinedload(
                                gh_pull_request_mod.PullRequest.head_commits,
                            ),
                        ),
                    )
                )
                .unique()
                .all()
            )
            assert len(pulls_in_db) == 1
            assert len(pulls_in_db[0].head_commits) == 2
            assert [
                comm.sha for comm in pulls_in_db[0].head_commits
            ] != new_head_commits_sha
