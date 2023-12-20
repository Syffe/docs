from unittest import mock

import pytest
import sqlalchemy
import yaml

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import database
from mergify_engine.clients import github
from mergify_engine.models.github import check_run as gh_checkrun_model
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils


@pytest.mark.usefixtures("_enable_github_in_postgres")
class TestGitHubCheckRun(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_check_run_inserted_and_updated(self) -> None:
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
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.run_engine({"github-in-postgres"})

        await self.wait_for_check_run(
            name=constants.SUMMARY_NAME,
            action="completed",
            status="completed",
            conclusion="success",
            pr_number=p1["number"],
        )
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            check_runs_in_db = (
                await session.scalars(sqlalchemy.select(gh_checkrun_model.CheckRun))
            ).all()

            assert len(check_runs_in_db) == 1

        p2 = await self.create_pr()
        await self.run_engine({"github-in-postgres"})
        await self.wait_for_check_run(
            name=constants.SUMMARY_NAME,
            action="completed",
            status="completed",
            conclusion="success",
            pr_number=p2["number"],
        )
        await self.run_engine({"github-in-postgres"})

        async with database.create_session() as session:
            check_runs_in_db = (
                await session.scalars(
                    sqlalchemy.select(gh_checkrun_model.CheckRun).where(
                        gh_checkrun_model.CheckRun.head_sha == p2["head"]["sha"],
                    ),
                )
            ).all()

            # Sometimes we get 1 check run Summary for p2 and sometimes we get 2
            assert 1 <= len(check_runs_in_db) <= 2

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine({"github-in-postgres"})
        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="opened",
                    ),
                },
                {
                    "event_type": "check_run",
                    "payload": tests_utils.get_check_run_event_payload(
                        name="Queue: Embarked in merge queue",
                        pr_number=p1["number"],
                    ),
                },
                {
                    "event_type": "check_run",
                    "payload": tests_utils.get_check_run_event_payload(
                        name="Rule: Queue (queue)",
                        pr_number=p1["number"],
                    ),
                },
                {
                    "event_type": "check_run",
                    "payload": tests_utils.get_check_run_event_payload(
                        name="Rule: Queue (queue)",
                        pr_number=p2["number"],
                    ),
                },
            ],
        )
        await self.run_engine({"github-in-postgres"})

        ctxt_p1 = context.Context(self.repository_ctxt, p1, [])
        with mock.patch.object(
            github.AsyncGitHubInstallationClient,
            "get",
            side_effect=AssertionError("shouldn't have used the github client"),
        ):
            check_runs = await ctxt_p1.pull_check_runs
            assert len(check_runs) == 3

            names = [c["name"] for c in check_runs]
            assert sorted(names) == sorted(
                [
                    constants.SUMMARY_NAME,
                    "Rule: Queue (queue)",
                    "Queue: Embarked in merge queue",
                ],
            )
