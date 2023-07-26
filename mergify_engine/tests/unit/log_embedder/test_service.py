import typing

import msgpack
import numpy as np
import numpy.typing as npt
import pytest
import respx
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.ci import event_processing
from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import openai_embedding
from mergify_engine.models import github_actions
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)


async def test_embed_logs(
    respx_mock: respx.MockRouter,
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    respx_mock.get(
        "https://api.github.com/users/mergifyio-testing/installation"
    ).respond(
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
    respx_mock.get(
        "https://api.github.com/repos/mergifyio-testing/functional-testing-repo-DouglasBlackwood/actions/jobs/13403743463/logs"
    ).respond(200, text="toto")

    respx_mock.post(
        openai_embedding.OPENAI_EMBEDDINGS_END_POINT,
        json={
            "input": "toto",
            "model": openai_embedding.OPENAI_EMBEDDINGS_MODEL,
        },
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
            "model": openai_embedding.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(
            sample_ci_events_to_process["workflow_job.completed.json"].slim_event
        ),
    }
    await redis_links.stream.xadd("gha_workflow_job", stream_event)

    await event_processing.process_event_streams(redis_links)

    pending_work = await github_action.embed_logs()
    assert not pending_work
    jobs = list(await db.scalars(sqlalchemy.select(github_actions.WorkflowJob)))
    assert len(jobs) == 1
    assert jobs[0].log_embedding is None

    db.expunge_all()

    monkeypatch.setattr(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        [
            sample_ci_events_to_process["workflow_job.completed.json"].slim_event[
                "repository"
            ]["owner"]["login"]
        ],
    )
    pending_work = await github_action.embed_logs()
    assert not pending_work

    jobs = list(await db.scalars(sqlalchemy.select(github_actions.WorkflowJob)))
    assert len(jobs) == 1
    assert np.array_equal(
        # FIXME(sileht): This typing issue is annoying, sqlalchemy.Vector is not recognized as ndarray by mypy
        typing.cast(npt.NDArray[np.float32], jobs[0].log_embedding),
        OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"],
    )
