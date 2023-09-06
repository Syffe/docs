import datetime
import os
import typing

import numpy as np
import numpy.typing as npt
import pytest
import respx
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)
from mergify_engine.tests.unit.test_utils import add_workflow_job


with open(os.path.join(os.path.dirname(__file__), "gha-ci-logs.zip"), "rb") as f:
    GHA_CI_LOGS_ZIP = f.read()


async def test_embed_logs(
    respx_mock: respx.MockRouter,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = github_account.GitHubAccount(id=1, login="owner_login")
    db.add(owner)
    repo = github_repository.GitHubRepository(id=1, owner=owner, name="repo_name")
    db.add(repo)

    respx_mock.get(f"https://api.github.com/users/{owner.login}/installation").respond(
        200,
        json=github_types.GitHubInstallation(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubInstallationIdType(0),
                "target_type": "Organization",
                "suspended_at": None,
                "permissions": {},
                "account": sample_ci_events_to_process[
                    "workflow_job.completed.json"
                ].slim_event["repository"]["owner"],
            }
        ),
    )
    respx_mock.post("https://api.github.com/app/installations/0/access_tokens").respond(
        200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"}
    )
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/embeddings",
    ).respond(
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

    # NOTE(Kontrolix): Reduce batch size to speed up test
    github_action.LOG_EMBEDDER_JOBS_BATCH_SIZE = 2

    # Create 3 jobs (LOG_EMBEDDER_JOBS_BATCH_SIZE + 1)
    for i in range(3):
        job = add_workflow_job(
            db,
            {
                "id": i,
                "repository": repo,
                "conclusion": github_actions.WorkflowJobConclusion.FAILURE,
                "name": "job_toto",
                "failed_step_name": "toto",
                "failed_step_number": 1,
            },
        )
        respx_mock.get(
            f"https://api.github.com/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs"
        ).respond(
            200, stream=GHA_CI_LOGS_ZIP  # type: ignore[arg-type]
        )

    await db.commit()

    pending_work = await github_action.embed_logs()
    assert not pending_work
    jobs = (await db.scalars(sqlalchemy.select(github_actions.WorkflowJob))).all()
    assert len(jobs) == 3
    assert all(job.log_embedding is None for job in jobs)

    db.expunge_all()

    monkeypatch.setattr(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        [owner.login],
    )
    pending_work = await github_action.embed_logs()
    assert pending_work
    pending_work = await github_action.embed_logs()
    assert not pending_work

    jobs = (await db.scalars(sqlalchemy.select(github_actions.WorkflowJob))).all()
    assert len(jobs) == 3
    assert all(
        np.array_equal(
            # FIXME(sileht): This typing issue is annoying, sqlalchemy.Vector is not recognized as ndarray by mypy
            typing.cast(npt.NDArray[np.float32], job.log_embedding),
            OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"],
        )
        for job in jobs
    )
    assert all(job.neighbours_computed_at is not None for job in jobs)

    # NOTE(Kontolix): Validate that if embedding has been computed for a job but neighbours
    # have not been computed, if we call the service, only neighbours are computed for that job.

    embedding_control_value = np.array([np.float32(-1)] * 1536)
    # Change embedding value to be sure that it is not compute again
    jobs[0].log_embedding = embedding_control_value
    jobs[0].neighbours_computed_at = None
    await db.commit()

    pending_work = await github_action.embed_logs()
    assert not pending_work

    db.expunge_all()

    a_job = await db.scalar(
        sqlalchemy.select(github_actions.WorkflowJob).where(
            github_actions.WorkflowJob.id == jobs[0].id
        )
    )
    assert a_job is not None
    assert np.array_equal(
        typing.cast(npt.NDArray[np.float32], a_job.log_embedding),
        embedding_control_value,
    )
    assert a_job.neighbours_computed_at is not None


async def test_workflow_job_log_life_cycle(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = github_account.GitHubAccount(id=1, login="owner")
    repo = github_repository.GitHubRepository(id=1, owner=owner, name="repo1")
    job1 = github_actions.WorkflowJob(
        id=1,
        repository=repo,
        workflow_run_id=1,
        name="job_toto",
        started_at=datetime.datetime.now(),
        completed_at=datetime.datetime.now(),
        conclusion=github_actions.WorkflowJobConclusion.FAILURE,
        labels=[],
        run_attempt=1,
        failed_step_name="toto",
        failed_step_number=1,
    )
    job2 = github_actions.WorkflowJob(
        id=2,
        repository=repo,
        workflow_run_id=2,
        name="job_toto",
        started_at=datetime.datetime.now(),
        completed_at=datetime.datetime.now(),
        conclusion=github_actions.WorkflowJobConclusion.FAILURE,
        labels=[],
        run_attempt=1,
        failed_step_name="toto",
        failed_step_number=1,
    )

    db.add(owner)
    db.add(repo)
    db.add(job1)
    db.add(job2)
    await db.commit()
    db.expunge_all()

    respx_mock.get(f"https://api.github.com/users/{owner.login}/installation").respond(
        200,
        json=github_types.GitHubInstallation(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubInstallationIdType(0),
                "target_type": "Organization",
                "suspended_at": None,
                "permissions": {},
                "account": {
                    "id": github_types.GitHubAccountIdType(1),
                    "login": github_types.GitHubLogin("owner"),
                    "type": "User",
                    "avatar_url": "",
                },
            }
        ),
    )
    respx_mock.post("https://api.github.com/app/installations/0/access_tokens").respond(
        200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"}
    )
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/embeddings",
    ).respond(
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
    respx_mock.get(
        f"https://api.github.com/repos/{owner.login}/{repo.name}/actions/runs/{job1.workflow_run_id}/attempts/{job1.run_attempt}/logs"
    ).respond(410)
    respx_mock.get(
        f"https://api.github.com/repos/{owner.login}/{repo.name}/actions/runs/{job2.workflow_run_id}/attempts/{job2.run_attempt}/logs"
    ).respond(
        200,
        stream=GHA_CI_LOGS_ZIP,  # type: ignore[arg-type]
    )

    monkeypatch.setattr(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        [owner.login],
    )

    jobs = list((await db.scalars(sqlalchemy.select(github_actions.WorkflowJob))).all())
    assert len(jobs) == 2
    assert jobs[0].log_status == github_actions.WorkflowJobLogStatus.UNKNOWN
    assert jobs[1].log_status == github_actions.WorkflowJobLogStatus.UNKNOWN
    db.expunge_all()

    await github_action.embed_logs()

    jobs = list((await db.scalars(sqlalchemy.select(github_actions.WorkflowJob))).all())
    assert len(jobs) == 2
    assert jobs[0].log_status == github_actions.WorkflowJobLogStatus.GONE
    assert jobs[1].log_status == github_actions.WorkflowJobLogStatus.EMBEDDED
