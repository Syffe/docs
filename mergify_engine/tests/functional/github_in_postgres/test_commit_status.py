from unittest import mock

import pytest
import sqlalchemy
import yaml

from mergify_engine import context
from mergify_engine import database
from mergify_engine.clients import github
from mergify_engine.models.github import Status
from mergify_engine.tests.functional import base


@pytest.mark.usefixtures("_enable_github_in_postgres")
class TestGitHubPullRequestCommitInPg(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_commit_status_in_pg(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            status_in_db = (await session.scalars(sqlalchemy.select(Status))).all()

            assert len(status_in_db) == 0

        await self.create_status(p1, state="failure")
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            status_in_db = (await session.scalars(sqlalchemy.select(Status))).all()

            assert len(status_in_db) == 1

        await self.create_status(p2, state="failure")
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            status_in_db = (await session.scalars(sqlalchemy.select(Status))).all()

            assert len(status_in_db) == 2

        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            pull_statuses = await ctxt_p1.pull_statuses
            assert len(pull_statuses) == 1
            assert pull_statuses[0]["state"] == "failure"

        await self.create_status(p1)
        await self.run_engine({"github-in-postgres"})

        await self.wait_for_pull_request("opened")

        async with database.create_session() as session:
            status_in_db = (await session.scalars(sqlalchemy.select(Status))).all()

            # new status = new id, so 2 status for p1 and 1 for p2
            assert len(status_in_db) == 3

        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            pull_statuses = await ctxt_p1.pull_statuses
            assert len(pull_statuses) == 1
            assert pull_statuses[0]["state"] == "success"
