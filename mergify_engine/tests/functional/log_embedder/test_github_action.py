import typing

import numpy as np
import respx
import sqlalchemy
import yaml

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.log_embedder import github_action as gha_embedder
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github_actions as gha_model
from mergify_engine.tests.functional import base
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)
from mergify_engine.worker.manager import ServicesSet


class TestLogEmbedderGithubAction(base.FunctionalTestBase):
    async def run_github_action(
        self,
    ) -> tuple[github_types.GitHubEventWorkflowJob, github_types.GitHubPullRequest]:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {"uses": "actions/checkout@v2"},
                        {"name": "Succes step 🎉", "run": "echo toto"},
                        {
                            "name": "Failed step but no failure 🛑",
                            "run": "echo I faill but we continue;exit 1",
                            "continue-on-error": True,
                        },
                        {
                            "name": "Failure step with a / in the title ❌",
                            "run": "echo I will fail on sha ${{ github.event.pull_request.head.sha }};exit 1",
                        },
                    ],
                }
            },
        }

        await self.setup_repo(
            files={".github/workflows/ci.yml": yaml.dump(ci)},
        )
        pr = await self.create_pr()
        job_event = await self.wait_for("workflow_job", {"action": "completed"})
        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))

        return typing.cast(github_types.GitHubEventWorkflowJob, job_event), pr

    async def test_log_downloading(self) -> None:
        job_event, pr = await self.run_github_action()

        assert job_event["workflow_job"] is not None

        async with database.create_session() as session:
            job = await session.scalar(
                sqlalchemy.select(gha_model.WorkflowJob).where(
                    gha_model.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )

        assert job is not None

        log = await gha_embedder.download_failed_step_log(job)

        assert f"I will fail on sha {pr['head']['sha']}" in "".join(log)

    async def test_log_embedding(self) -> None:
        job_event, pr = await self.run_github_action()

        assert job_event["workflow_job"] is not None

        async with database.create_session() as session:
            job = await session.scalar(
                sqlalchemy.select(gha_model.WorkflowJob).where(
                    gha_model.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )
            assert job is not None

            # NOTE(Kontrolix): We must set `assert_all_called=True` here beacause
            # pass_through routes to github api will not be called in no recording mode
            with respx.mock(assert_all_called=False) as respx_mock:
                # NOTE(Kontrolix): Let github api pass through the mock to only catch openai api call
                respx_mock.route(
                    host="pipelines.actions.githubusercontent.com"
                ).pass_through()
                respx_mock.route(host="api.github.com").pass_through()

                respx_mock.post(openai_api.OPENAI_EMBEDDINGS_END_POINT).respond(
                    200,
                    json={
                        "object": "list",
                        "data": [
                            {
                                "object": "embedding",
                                "index": 0,
                                "embedding": OPENAI_EMBEDDING_DATASET["toto"],
                            }
                        ],
                        "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
                        "usage": {"prompt_tokens": 2, "total_tokens": 2},
                    },
                )

                await gha_embedder.embed_log(job)

            await session.commit()

        async with database.create_session() as session:
            result = await session.execute(
                sqlalchemy.select(gha_model.WorkflowJob).where(
                    gha_model.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )
            job = result.scalar()

        assert job is not None
        assert job.embedded_log is not None
        assert f"I will fail on sha {pr['head']['sha']}" in job.embedded_log
        assert job.failed_step_number == 5
        assert job.failed_step_name == "Failure step with a / in the title ❌"
        assert job.log_embedding is not None

        assert np.array_equal(
            job.log_embedding, OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"]
        )
