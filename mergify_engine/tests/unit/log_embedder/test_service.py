import base64
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
        openai_api.OPENAI_EMBEDDINGS_END_POINT,
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
            200,
            stream=base64.b64decode(  # type: ignore[arg-type]
                b"UEsDBBQAAAAIALxTGVdrx2MWrgEAAAQYAAAJAAAALkRTX1N0b3Jl7ZjBTsIwGIDbMaULMdlBE+OpiVcSGWIM8TLnvBkOzugBzWSMwHSshA13ICQ8gD6Bb+LdV/BBvHlzYz/JlBE1HiDaL2k+WP92++nWriCEsDawFYRkhBBBiQUJZUKgzCCA81HBkz4CxOJihX4vuy/OkhGPHUE30bhZyEyPn+UyK6p7snqu4wel0gsWcuLKap7kSYFcGR0WGkEjGPhao1+Pv50x5lrTzw3r3GmFprx+xLyg4Xit/qSBY7eikMsLx7NZqLGBZ/v1VAWRiGTKW8NhRakqRVqu7I6KdKgou+Ui3a/ujUYS2djeOTg1b92ux+6TBDCGTNY+ZfaQlZntN10rulKcVem2FaPJuulbdyamy3QdfhryWnt8q508H2bH2d+K63W+Pued0fdc5rXR5DHjcDic3wOTCSks9jI4HM4SEs8PFKyCx4kx1AtgMdVGBlOwCh4nxhAngEUwActgClbB48QwaWHYfGA4M4YdCpbBFKz+KGUO59+QSyTH6//x/P0/h8P5w2BRN3QNzf93IV5raVSupw3QxxcBOCZCbLwUb6aOU7AKHifmLwIczqJ4B1BLAwQUAAAACADIUxlX8aTMEAYAAAAEAAAAEwAAAGpvYl90b3RvLzFfdG90by50eHQryS/JBwBQSwECFAMUAAAACAC8UxlXa8djFq4BAAAEGAAACQAAAAAAAAAAAAAApIEAAAAALkRTX1N0b3JlUEsBAhQDFAAAAAgAyFMZV/GkzBAGAAAABAAAABMAAAAAAAAAAAAAAKSB1QEAAGpvYl90b3RvLzFfdG90by50eHRQSwUGAAAAAAIAAgB4AAAADAIAAAAA"
            ),
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
