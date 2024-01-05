from unittest import mock

import pytest
import sqlalchemy

from mergify_engine import context
from mergify_engine import database
from mergify_engine.clients import github
from mergify_engine.models.github import pull_request_file
from mergify_engine.tests.functional import base


@pytest.mark.usefixtures("_enable_github_in_postgres")
class TestGitHubPullRequestFileInPg(base.FunctionalTestBase):
    async def test_pull_request_files_stored_and_used(self) -> None:
        await self.setup_repo()

        p1 = await self.create_pr(files={"testp1": "hello"})
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            files_in_db = (
                await session.scalars(
                    sqlalchemy.select(pull_request_file.PullRequestFile),
                )
            ).all()

            assert len(files_in_db) == 1
            assert files_in_db[0].filename == "testp1"

        p2 = await self.create_pr(files={"testp2": "hello"})
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            files_in_db = (
                await session.scalars(
                    sqlalchemy.select(pull_request_file.PullRequestFile),
                )
            ).all()

            assert len(files_in_db) == 2
            assert sorted([f.filename for f in files_in_db]) == ["testp1", "testp2"]

        ctxt = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            files = await ctxt.files
            assert len(files) == 1
            assert files[0]["filename"] == "testp1"

        await self.push_file(
            filename="testp1",
            content="helloupdated",
            destination_branch=p1["head"]["ref"],
        )
        p1_synced = await self.wait_for_pull_request("synchronize", p1["number"])
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            files_in_db = (
                await session.scalars(
                    sqlalchemy.select(pull_request_file.PullRequestFile),
                )
            ).all()
            assert sorted([f.filename for f in files_in_db]) == ["testp1", "testp2"]

            assert len(files_in_db) == 2

        ctxt = context.Context(self.repository_ctxt, p1_synced["pull_request"], [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            files = await ctxt.files
            assert len(files) == 1
            assert files[0]["filename"] == "testp1"

        await self.push_file(
            filename="testp2_new",
            content="hellonewfile",
            destination_branch=p2["head"]["ref"],
        )
        p2_synced = await self.wait_for_pull_request("synchronize", p2["number"])
        await self.run_engine({"github-in-postgres"})
        async with database.create_session() as session:
            files_in_db = (
                await session.scalars(
                    sqlalchemy.select(pull_request_file.PullRequestFile),
                )
            ).all()
            assert sorted([f.filename for f in files_in_db]) == [
                "testp1",
                "testp2",
                "testp2_new",
            ]

            assert len(files_in_db) == 3

        ctxt = context.Context(self.repository_ctxt, p2_synced["pull_request"], [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            files = await ctxt.files
            assert len(files) == 2
            assert sorted([f["filename"] for f in files]) == ["testp2", "testp2_new"]
