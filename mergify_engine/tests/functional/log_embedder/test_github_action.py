import io
import typing
import zipfile

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
        self, steps: list[dict[str, typing.Any]] | None = None
    ) -> tuple[github_types.GitHubEventWorkflowJob, github_types.GitHubPullRequest]:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": steps,
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
        job_event, pr = await self.run_github_action(
            steps=[
                {
                    "name": "Say hi we count run_attempt here",
                    "run": "echo I will fail on sha ${{ github.event.pull_request.head.sha }} run_attempt:${{ github.run_attempt }};exit 1",
                },
            ],
        )

        async def download_and_check_log(
            job_event: github_types.GitHubEventWorkflowJob, run_attempt: int
        ) -> None:
            assert job_event["workflow_job"] is not None
            assert job_event["workflow_job"]["run_attempt"] == run_attempt

            async with database.create_session() as session:
                job = await session.scalar(
                    sqlalchemy.select(gha_model.WorkflowJob).where(
                        gha_model.WorkflowJob.id == job_event["workflow_job"]["id"]
                    )
                )

            assert job is not None

            log = await gha_embedder.download_failed_step_log(job)

            assert (
                f"I will fail on sha {pr['head']['sha']} run_attempt:{run_attempt}"
                in "".join(log)
            )

        await download_and_check_log(job_event, 1)

        # Rerun the failed job
        assert job_event["workflow_job"] is not None
        await self.client_integration.post(
            f"{self.url_origin}/actions/jobs/{job_event['workflow_job']['id']}/rerun"
        )
        job_event = typing.cast(
            github_types.GitHubEventWorkflowJob,
            await self.wait_for("workflow_job", {"action": "completed"}),
        )
        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))

        await download_and_check_log(job_event, 2)

    async def test_log_embedding(self) -> None:
        job_event, pr = await self.run_github_action(
            steps=[
                {"name": "Success step 🎉", "run": "echo toto"},
                {
                    "name": "Failed step but no failure 🛑",
                    "run": "echo I fail but we continue;exit 1",
                    "continue-on-error": True,
                },
                {
                    "name": 'Failure step with *"/\\<>:|? in the title ❌',
                    "run": "echo I will fail on sha ${{ github.event.pull_request.head.sha }} run_attempt:${{ github.run_attempt }};exit 1",
                },
            ],
        )

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

                respx_mock.post(f"{openai_api.OPENAI_API_BASE_URL}/embeddings").respond(
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

                async with openai_api.OpenAIClient() as openai_client:
                    await gha_embedder.embed_log(openai_client, job)

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
        assert job.failed_step_number == 4
        assert job.failed_step_name == 'Failure step with *"/\\<>:|? in the title ❌'
        assert job.log_embedding is not None
        assert job.log_status is gha_model.WorkflowJobLogStatus.EMBEDDED

        assert np.array_equal(
            job.log_embedding, OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"]
        )

    async def test_log_step_name_to_zip_filenames(self) -> None:
        steps = [
            {"name": "Say hi we count run_attempt here", "run": "echo Hi"},
            {
                # Reproduce ENGINE-3EJ with one digit step number
                "name": "Run # nosemgrep generic.ci.security.use-frozen-lockfile.use-frozen-lockfile-pip.txt",
                "run": "echo toto",
            },
            {
                "name": 'Failure step with *"/\\<>:|? in the title ❌',
                "run": "echo Hi",
            },
            {
                "name": "Failure step with a long utf8 char 𒐫 and too many chars to fit in a GitHub zip filename ❌",
                "run": "echo Hi",
            },
            # To ensure we have step numbers with multiple digits
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {"name": "Success step 🎉", "run": "echo toto"},
            {
                # Reproduce ENGINE-3EJ with two digits step number
                "name": "Run # nosemgrep generic.ci.security.use-frozen-lockfile.use-frozen-lockfile-pip.txt",
                "run": "echo toto",
            },
        ]
        job_event, pr = await self.run_github_action(steps=steps)

        assert job_event["workflow_job"] is not None

        async with database.create_session() as session:
            result = await session.execute(
                sqlalchemy.select(gha_model.WorkflowJob).where(
                    gha_model.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )
            job = result.scalar()

        assert job is not None

        # Get the logs
        resp = await self.client_integration.get(
            f"/repos/{self.repository_ctxt.repo['full_name']}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs"
        )

        for n, step in enumerate(steps):
            # The two first steps are GitHub internal stuffs
            job.failed_step_number = n + 2
            job.failed_step_name = step["name"]

            assert (
                len(
                    gha_embedder.get_lines_from_zip(
                        zipfile.ZipFile(io.BytesIO(resp.content)), job
                    )
                )
                > 0
            )
