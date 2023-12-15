import typing

import numpy as np
import pytest
import sqlalchemy
import yaml

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models import github as gh_models
from mergify_engine.tests.functional import base
from mergify_engine.worker.manager import ServicesSet


class TestRerunFlakyCheck(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    @pytest.fixture(autouse=True)
    def _add_monkeypatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.monkeypatch = monkeypatch

    @pytest.mark.usefixtures(
        "_prepare_google_cloud_storage_setup",
        "_enable_github_in_postgres",
    )
    async def test_rerun_on_known_flaky(self) -> None:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {
                            "name": "Fail until run_attempt 2",
                            "run": """echo I will fail on sha ${{ github.event.pull_request.head.sha }} and if run_attempt is lower than 2 run_attempt:${{ github.run_attempt }};[[ ${{ github.run_attempt }} -lt 2 ]] && exit 1 || exit 0""",
                        },
                    ],
                },
            },
        }

        config = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        "check-success=unit-tests",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
            "_checks_to_retry_on_failure": {"unit-tests": 2},
        }
        await self.setup_repo(
            yaml.dump(config),
            files={".github/workflows/ci.yml": yaml.dump(ci)},
        )

        # Create manually a known flaky pr
        pr = await self.create_pr()
        job_event = typing.cast(
            github_types.GitHubEventWorkflowJob,
            await self.wait_for("workflow_job", {"action": "completed"}),
        )
        assert job_event["workflow_job"] is not None
        assert job_event["workflow_job"]["conclusion"] == "failure"
        await self.client_integration.post(
            f"{self.url_origin}/actions/jobs/{job_event['workflow_job']['id']}/rerun",
        )
        await self.wait_for_check_run(
            "completed",
            conclusion="success",
            name="unit-tests",
        )
        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))
        await self.wait_for_pull_request("closed", pr["number"], merged=True)

        await self.git("pull")

        # Create the test PR
        pr2 = await self.create_pr()

        await self.wait_for_all(
            [
                {"event_type": "workflow_job", "payload": {"action": "completed"}},
                {
                    "event_type": "check_run",
                    "payload": {
                        "action": "completed",
                        "check_run": {
                            "conclusion": "failure",
                            "name": "unit-tests",
                        },
                    },
                },
            ],
        )

        await self.run_engine(
            additionnal_services=ServicesSet("ci-event-processing,github-in-postgres"),
        )

        async with database.create_session() as session:
            # Set the embedding to the same value on the failed jobs to make them
            # neighbours
            await session.execute(
                sqlalchemy.update(gh_models.WorkflowJob)
                .values(
                    log_extract="Awesome log",
                    log_embedding=np.array(list(map(np.float32, [1] * 1536))),
                    log_embedding_status=gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                )
                .where(
                    gh_models.WorkflowJob.conclusion
                    == gh_models.WorkflowJobConclusion.FAILURE,
                ),
            )

            await session.commit()

        self.monkeypatch.setattr(
            settings,
            "LOG_EMBEDDER_ENABLED_ORGS",
            [self.repository_ctxt.repo["owner"]["login"]],
        )

        # Run engine with log-embedder up since it's it, that will send the
        # send_pull_refresh signal that will make the pending PR reevaluated
        await self.run_engine(additionnal_services=ServicesSet("log-embedder"))

        # Run engine once again to take the send_pull_refresh into account
        await self.run_engine()

        await self.wait_for_check_run(
            "completed",
            conclusion="success",
            name="unit-tests",
        )
        await self.run_engine()
        await self.wait_for_pull_request("closed", pr2["number"], merged=True)
