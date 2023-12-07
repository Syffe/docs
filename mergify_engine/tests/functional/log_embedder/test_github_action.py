import json
import os
import typing

import daiquiri
import numpy as np
import pytest
import respx
import respx.patterns
import sqlalchemy
from sqlalchemy import orm
import yaml

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import google_cloud_storage
from mergify_engine.log_embedder import github_action as gha_embedder
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import ci_issue
from mergify_engine.models import github as gh_models
from mergify_engine.tests.functional import base
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)
from mergify_engine.worker.manager import ServicesSet


LOG = daiquiri.getLogger(__name__)

PATH_INPUT_JOBS_JSON = (
    "zfixtures/functional/log_embedder/raw_logs/unsorted_logs/input_jobs_json"
)
PATH_INPUT_RAW_LOG_TXT = (
    "zfixtures/functional/log_embedder/raw_logs/unsorted_logs/input_raw_logs_txt"
)

JOB_IDS_BY_ISSUE: dict[int, list[int]] = {
    1: [17901126470, 17901839885],
    2: [17962371786, 17962438196],
    3: [17889769992, 17890012842],
    4: [17865682999],
    # FIXME(Syffe): These two Ids should be together, but in our test they are not yet.
    # so the test is currently failing, this would need to be improved.
    5: [17892796183, 17892931465],
    6: [17901659641],
    7: [17921959372],
    8: [17925396037],
    9: [17949965633],
    10: [17952684380],
    11: [17953176262, 17953176746, 17953198682, 17953198783, 17953364603, 17953417253],
    12: [17956712782, 17956714438],
    13: [17957050782, 17957055281],
    14: [17865016836],
    15: [17901359458],
    16: [17901360892],
    17: [17901361506],
    18: [17901361849],
    19: [17951738999],
    20: [17955513710, 17952831249, 17960638740],
    21: [17954365579],
    22: [17959931265],
    23: [17963656086],
    24: [17890395377, 17890410390, 17890412896],
}

# NOTE(Kontrolix): This is the current result of grouping, NOT the expectation.
# it's there to have a reference of the current accuracy of the grouping and to be able
# to control accuracy variations.
JOB_IDS_BY_ISSUE_GPT: dict[int, list[int]] = {
    1: [17901126470, 17901839885],
    2: [17962371786, 17962438196],
    3: [17889769992],
    4: [17890012842],
    5: [17865682999],
    6: [17892796183],
    7: [17892931465],
    8: [17901659641],
    9: [17901659641],
    10: [17901659641],
    11: [17921959372, 17925396037],
    12: [17925396037],
    13: [17949965633],
    14: [17952684380],
    15: [17953176262, 17953176746, 17953198682, 17953198783, 17953364603, 17953417253],
    16: [17956712782],
    17: [17956714438],
    18: [17957050782, 17957055281],
    19: [17865016836],
    20: [17901359458],
    21: [17901360892],
    22: [17901361506],
    23: [17901361849],
    24: [17951738999],
    25: [17952831249, 17954365579],
    26: [17952831249, 17954365579, 17960638740],
    27: [17954365579, 17960638740],
    28: [17954365579],
    29: [17955513710, 17960638740],
    30: [17955513710, 17960638740],
    31: [17959931265],
    32: [17963656086],
    33: [17963656086],
    34: [17963656086],
    35: [17963656086],
    36: [17890395377, 17890412896],
    37: [17890410390],
}


class TestLogEmbedderGithubAction(base.FunctionalTestBase):
    async def run_github_action(
        self,
        steps: list[dict[str, typing.Any]] | None = None,
    ) -> tuple[github_types.GitHubEventWorkflowJob, github_types.GitHubPullRequest]:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": steps,
                },
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
            job_event: github_types.GitHubEventWorkflowJob,
            run_attempt: int,
        ) -> None:
            assert job_event["workflow_job"] is not None
            assert job_event["workflow_job"]["run_attempt"] == run_attempt

            async with database.create_session() as session:
                job = await session.scalar(
                    sqlalchemy.select(gh_models.WorkflowJob).where(
                        gh_models.WorkflowJob.id == job_event["workflow_job"]["id"],
                    ),
                )

            assert job is not None

            expected_log = (
                f"I will fail on sha {pr['head']['sha']} run_attempt:{run_attempt}"
            ).encode()

            log = await job.download_failed_logs(self.client_integration)
            assert isinstance(job.failed_step_number, int)
            assert expected_log in gha_embedder.get_step_log_from_zipped_content(
                log,
                job.github_name,
                job.failed_step_number,
            )

        await download_and_check_log(job_event, 1)

        # Rerun the failed job
        assert job_event["workflow_job"] is not None
        await self.client_integration.post(
            f"{self.url_origin}/actions/jobs/{job_event['workflow_job']['id']}/rerun",
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
                },
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
            f"{self.url_origin}/actions/runs/{job_event['workflow_job']['run_id']}/cancel",
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
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"],
                ),
            )

        assert job is not None
        assert job.failed_step_number is None
        assert job.failed_step_name is None
        assert job.steps == []

        log = await job.download_failure_annotations(self.client_integration)

        assert [
            f"The run was canceled by @{self.installation_ctxt.installation['app_slug']}.",
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
                },
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

            log = await job.download_failed_logs(self.client_integration)

            assert (
                f"I will fail on sha {pr['head']['sha']} version: {job.matrix}".encode()
                in gha_embedder.get_step_log_from_zipped_content(
                    log,
                    job.github_name,
                    job.failed_step_number,
                )
            )

    @pytest.mark.usefixtures("_prepare_google_cloud_storage_setup")
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
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"],
                ),
            )
            assert job is not None

            # NOTE(Kontrolix): We must not set `assert_all_called=True` here beacause
            # pass_through routes to github api will not be called in no recording mode
            with respx.mock(assert_all_called=False) as respx_mock:
                # NOTE(Greesb): We need to mock like this because sometimes we get sent to some weird url
                # like so: https://pipelinesghubeus5.actions.githubusercontent.com
                pipelines_route = respx.patterns.M(
                    host__regex=r"pipelines\w*\.actions\.githubusercontent\.com",
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
                            },
                        ],
                        "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
                        "usage": {"prompt_tokens": 2, "total_tokens": 2},
                    },
                )

                gcs_client = google_cloud_storage.GoogleCloudStorageClient(
                    settings.LOG_EMBEDDER_GCS_CREDENTIALS,
                )

                log_content = await gha_embedder.get_log(gcs_client, job)

                async with openai_api.OpenAIClient() as openai_client:
                    await gha_embedder.embed_log(openai_client, job, log_content)

            await session.commit()

        async with database.create_session() as session:
            result = await session.execute(
                sqlalchemy.select(gh_models.WorkflowJob).where(
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"],
                ),
            )
            job = result.scalar()

        assert job is not None
        assert job.embedded_log is not None
        assert f"I will fail on sha {pr['head']['sha']}" in job.embedded_log
        assert job.failed_step_number == 4
        assert job.failed_step_name == 'Failure step with *"/\\<>:|? in the title ❌'
        assert job.log_embedding is not None
        assert job.log_status is gh_models.WorkflowJobLogStatus.DOWNLOADED
        assert (
            job.log_embedding_status is gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED
        )

        assert np.array_equal(
            job.log_embedding,
            OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"],
        )

        gcs_client = google_cloud_storage.GoogleCloudStorageClient(
            settings.LOG_EMBEDDER_GCS_CREDENTIALS,
        )
        blobs = list(gcs_client.list_blobs(settings.LOG_EMBEDDER_GCS_BUCKET))
        assert len(blobs) == 2
        assert (
            blobs[0].name
            == f"{self.installation_ctxt.owner_id}/{self.repository_ctxt.repo['id']}/{job.id}/jobs.json"
        )
        assert (
            blobs[1].name
            == f"{self.installation_ctxt.owner_id}/{self.repository_ctxt.repo['id']}/{job.id}/logs.gz"
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
                    gh_models.WorkflowJob.id == job_event["workflow_job"]["id"],
                ),
            )
            job = result.scalar()

        assert job is not None

        # Get the logs
        resp = await self.client_integration.get(
            f"/repos/{self.repository_ctxt.repo['full_name']}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
        )

        for n, step in enumerate(steps):
            # The two first steps are GitHub internal stuffs
            job.failed_step_number = n + 2
            job.failed_step_name = step["name"]

            assert gha_embedder.get_step_log_from_zipped_content(
                resp.content,
                job.github_name,
                job.failed_step_number,
            )

    async def test_log_embedding_matrix(self) -> None:
        ci = {
            "name": "Continuous Integration",
            "on": {"pull_request": {"branches": self.main_branch_name}},
            "jobs": {
                "unit-tests": {
                    "strategy": {
                        "matrix": {"version": [2.8, 3.5], "os": ["windows", "mac"]},
                    },
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {
                            "name": "run version: ${{ matrix.version }} os:${{ matrix.os }}",
                            "run": "echo version: ${{ matrix.version }} os:${{ matrix.os }}",
                        },
                    ],
                },
                "job_without_matrix": {
                    "timeout-minutes": 5,
                    "runs-on": "ubuntu-20.04",
                    "steps": [
                        {
                            "name": "No matrix",
                            "run": "echo no matrix",
                        },
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
            if (job.name_without_matrix, job.matrix) in expected_job_name_and_matrix:
                expected_job_name_and_matrix.remove(
                    (job.name_without_matrix, job.matrix),
                )

        assert expected_job_name_and_matrix == []

    @pytest.mark.xfail(
        reason="two Ids (issues 5 and 6) are currently being issue separated, where they should be grouped",
    )
    @pytest.mark.make_real_openai_calls()
    async def test_ci_issue_grouping_accuracy(
        self,
    ) -> None:
        jobs_to_compare = []
        job_ids_to_compare = []

        async with database.create_session() as session:
            # loop the job files and fill the DB
            # NOTE(Syffe): we sort here since depending on the OS the order of the files might not be the same
            for job_json_filename in sorted(os.listdir(PATH_INPUT_JOBS_JSON)):
                with open(
                    f"{PATH_INPUT_JOBS_JSON}/{job_json_filename}",
                ) as job_json_file:
                    job_json = json.load(job_json_file)

                    job_json.update(
                        {
                            "name": job_json["github_name"],
                            "run_id": job_json["workflow_run_id"],
                            "conclusion": "failure",
                        },
                    )
                    job = await gh_models.WorkflowJob.insert(
                        session,
                        job_json,
                        job_json["repository"],
                    )

                    with open(
                        f"{PATH_INPUT_RAW_LOG_TXT}/{job.repository.owner_id}_{job.repository_id}_{job.id}_logs.txt",
                    ) as log_file:
                        (
                            tokens,
                            truncated_log,
                        ) = await gha_embedder.get_tokenized_cleaned_log(
                            log_file.read(),
                        )
                    async with openai_api.OpenAIClient() as openai_client:
                        embedding = await openai_client.get_embedding(tokens)

                    job.log_embedding = embedding
                    job.embedded_log = truncated_log
                    job.log_embedding_status = (
                        gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED
                    )

                    await ci_issue.CiIssue.link_job_to_ci_issue(session, job)

                    jobs_to_compare.append(job)
                    job_ids_to_compare.append(job.id)

            await session.commit()
            session.expunge_all()

            # loop over every computed job and assert the values against our mapping structure of expected values
            for job in jobs_to_compare:
                job_with_issue = await session.get_one(
                    gh_models.WorkflowJob,
                    job.id,
                    options=[
                        orm.joinedload(gh_models.WorkflowJob.ci_issue).selectinload(
                            ci_issue.CiIssue.jobs,
                        ),
                    ],
                )
                assert job_with_issue.ci_issue_id is not None
                assert JOB_IDS_BY_ISSUE[job_with_issue.ci_issue_id] == [
                    job.id for job in job_with_issue.ci_issue.jobs
                ]

    @pytest.mark.make_real_openai_calls()
    async def test_ci_issue_gpt_grouping_accuracy(
        self,
    ) -> None:
        jobs_to_compare = []
        job_ids_to_compare = []

        async with database.create_session() as session:
            # loop the job files and fill the DB
            # NOTE(Syffe): we sort here since depending on the OS the order of the files might not be the same
            for job_json_filename in sorted(os.listdir(PATH_INPUT_JOBS_JSON)):
                with open(
                    f"{PATH_INPUT_JOBS_JSON}/{job_json_filename}",
                ) as job_json_file:
                    job_json = json.load(job_json_file)

                    job_json.update(
                        {
                            "name": job_json["github_name"],
                            "run_id": job_json["workflow_run_id"],
                            "conclusion": "failure",
                        },
                    )
                    job = await gh_models.WorkflowJob.insert(
                        session,
                        job_json,
                        job_json["repository"],
                    )

                    with open(
                        f"{PATH_INPUT_RAW_LOG_TXT}/{job.repository.owner_id}_{job.repository_id}_{job.id}_logs.txt",
                    ) as log_file:
                        log_lines = log_file.readlines()

                    async with openai_api.OpenAIClient() as openai_client:
                        await gha_embedder.extract_data_from_log(
                            openai_client,
                            session,
                            job,
                            log_lines,
                        )
                    job.log_metadata_extracting_status = (
                        gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED
                    )

                    await ci_issue.CiIssueGPT.link_job_to_ci_issues(session, job)

                    jobs_to_compare.append(job)
                    job_ids_to_compare.append(job.id)

            await session.commit()
            session.expunge_all()

            issues = (
                (
                    await session.execute(
                        sqlalchemy.select(ci_issue.CiIssueGPT).options(
                            orm.joinedload(ci_issue.CiIssueGPT.jobs),
                        ),
                    )
                )
                .unique()
                .scalars()
            )

            assert JOB_IDS_BY_ISSUE_GPT == {
                issue.id: [job.id for job in issue.jobs] for issue in issues
            }

    @pytest.mark.make_real_openai_calls()
    async def test_extract_data_from_log(
        self,
    ) -> None:
        async with database.create_session() as session:
            with open(
                f"{PATH_INPUT_JOBS_JSON}/37838584_575916772_17890412896_jobs.json",
            ) as job_json_file:
                job_json = json.load(job_json_file)

            job_json.update(
                {
                    "name": job_json["github_name"],
                    "run_id": job_json["workflow_run_id"],
                    "conclusion": "failure",
                },
            )
            job = await gh_models.WorkflowJob.insert(
                session,
                job_json,
                job_json["repository"],
            )

            with open(
                f"{PATH_INPUT_RAW_LOG_TXT}/{job.repository.owner_id}_{job.repository_id}_{job.id}_logs.txt",
            ) as log_file:
                log_lines = log_file.readlines()

            async with openai_api.OpenAIClient() as openai_client:
                await gha_embedder.extract_data_from_log(
                    openai_client,
                    session,
                    job,
                    log_lines,
                )

            await session.commit()
            session.expunge_all()

            job = await session.get_one(
                gh_models.WorkflowJob,
                job.id,
                options=[orm.joinedload(gh_models.WorkflowJob.log_metadata)],
            )

            for metadata in job.log_metadata:
                assert metadata.language == "JavaScript"
                assert metadata.lineno == "15"
                assert (
                    metadata.stack_trace
                    == """TypeError: Cannot read properties of undefined (reading 'id')\n    at eval (eval at callAsyncFunction (/home/runner/work/_actions/actions/github-script/v6/dist/index.js:15143:16), <anonymous>:15:31)\n    at processTicksAndRejections (node:internal/process/task_queues:96:5)\n    at async main (/home/runner/work/_actions/actions/github-script/v6/dist/index.js:15236:20)"""
                )
                assert (
                    metadata.error
                    == "TypeError: Cannot read properties of undefined (reading 'id')"
                )
                assert metadata.problem_type == "Accessing property of undefined object"
