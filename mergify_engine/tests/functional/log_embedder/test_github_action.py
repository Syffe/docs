import io
import typing
import zipfile

import numpy as np
import respx
import respx.patterns
import sqlalchemy
import yaml

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.log_embedder import github_action as gha_embedder
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github as gh_models
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
                    sqlalchemy.select(gh_models.WorkflowJob).where(
                        gh_models.WorkflowJob.id == job_event["workflow_job"]["id"]
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

    async def test_log_downloading_with_no_steps(self) -> None:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "dummy_runner",
                    "steps": [
                        {
                            "name": "Say hi we count run_attempt here",
                            "run": "echo I will fail on sha ${{ github.event.pull_request.head.sha }} run_attempt:${{ github.run_attempt }};exit 1",
                        },
                    ],
                }
            },
        }

        await self.setup_repo(
            files={".github/workflows/ci.yml": yaml.dump(ci)},
        )

        await self.create_pr()
        job_event = typing.cast(
            github_types.GitHubEventWorkflowJob,
            await self.wait_for("workflow_job", {"action": "queued"}),
        )
        assert job_event["workflow_job"] is not None

        await self.client_integration.post(
            f"{self.url_origin}/actions/runs/{job_event['workflow_job']['run_id']}/cancel"
        )

        job_event = typing.cast(
            github_types.GitHubEventWorkflowJob,
            await self.wait_for("workflow_job", {"action": "completed"}),
        )
        assert job_event["workflow_job"] is not None

        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))

        async with database.create_session() as session:
            job = await session.scalar(
                sqlalchemy.select(gh_models.WorkflowJob).where(
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )

        assert job is not None
        assert job.failed_step_number is None
        assert job.failed_step_name is None
        assert job.steps == []

        log = await gha_embedder.download_failure_annotations(job)

        assert [
            f"The run was canceled by @{self.installation_ctxt.installation['app_slug']}."
        ] == log

    async def test_log_downloading_with_matrix(self) -> None:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "strategy": {"matrix": {"version": [2.8, 3.5]}},
                    "steps": [
                        {
                            "name": "the matrix",
                            "run": "echo I will fail on sha ${{ github.event.pull_request.head.sha }} version: ${{ matrix.version }};exit 1",
                        },
                    ],
                }
            },
        }

        await self.setup_repo(
            files={".github/workflows/ci.yml": yaml.dump(ci)},
        )

        pr = await self.create_pr()
        await self.wait_for("workflow_job", {"action": "completed"})
        await self.wait_for("workflow_job", {"action": "completed"})

        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))

        async with database.create_session() as session:
            jobs = (
                await session.scalars(sqlalchemy.select(gh_models.WorkflowJob))
            ).all()

        assert len(jobs) == 2

        for job in jobs:
            assert job is not None
            assert job.failed_step_number == 2
            assert job.failed_step_name == "the matrix"

            log = await gha_embedder.download_failed_step_log(job)

            assert (
                f"I will fail on sha {pr['head']['sha']} version: {job.matrix}"
                in "".join(log)
            )

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
                sqlalchemy.select(gh_models.WorkflowJob).where(
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )
            assert job is not None

            # NOTE(Kontrolix): We must set `assert_all_called=True` here beacause
            # pass_through routes to github api will not be called in no recording mode
            with respx.mock(assert_all_called=False) as respx_mock:
                # NOTE(Greesb): We need to mock like this because sometimes we get sent to some weird url
                # like so: https://pipelinesghubeus5.actions.githubusercontent.com
                pipelines_route = respx.patterns.M(
                    host__regex=r"pipelines\w*\.actions\.githubusercontent\.com"
                )
                # NOTE(Kontrolix): Let github api pass through the mock to only catch openai api call
                respx_mock.route(pipelines_route).pass_through()
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
                sqlalchemy.select(gh_models.WorkflowJob).where(
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"]
                )
            )
            job = result.scalar()

        assert job is not None
        assert job.embedded_log is not None
        assert f"I will fail on sha {pr['head']['sha']}" in job.embedded_log
        assert job.failed_step_number == 4
        assert job.failed_step_name == 'Failure step with *"/\\<>:|? in the title ❌'
        assert job.log_embedding is not None
        assert job.log_status is gh_models.WorkflowJobLogStatus.EMBEDDED

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
                sqlalchemy.select(gh_models.WorkflowJob).where(
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"]
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

    async def test_log_embedding_matrix(self) -> None:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "strategy": {
                        "matrix": {"version": [2.8, 3.5], "os": ["windows", "mac"]}
                    },
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {
                            "name": "run version: ${{ matrix.version }} os:${{ matrix.os }}",
                            "run": "echo version: ${{ matrix.version }} os:${{ matrix.os }}",
                        }
                    ],
                },
                "job_without_matrix": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {
                            "name": "No matrix",
                            "run": "echo no matrix",
                        }
                    ],
                },
            },
        }

        await self.setup_repo(
            files={".github/workflows/ci.yml": yaml.dump(ci)},
        )

        await self.create_pr()

        nb_jobs = 5

        for _ in range(nb_jobs):
            await self.wait_for("workflow_job", {"action": "completed"})

        await self.run_engine(additionnal_services=ServicesSet("ci-event-processing"))

        async with database.create_session() as session:
            jobs = (
                await session.scalars(sqlalchemy.select(gh_models.WorkflowJob))
            ).all()

        assert len(jobs) == nb_jobs

        expected_job_name_and_matrix = [
            ("job_without_matrix", None),
            ("unit-tests", "mac, 2.8"),
            ("unit-tests", "mac, 3.5"),
            ("unit-tests", "windows, 2.8"),
            ("unit-tests", "windows, 3.5"),
        ]
        for job in jobs:
            if (job.name, job.matrix) in expected_job_name_and_matrix:
                expected_job_name_and_matrix.remove((job.name, job.matrix))

        assert expected_job_name_and_matrix == []
