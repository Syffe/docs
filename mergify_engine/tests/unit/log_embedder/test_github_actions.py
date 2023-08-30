import typing

import numpy as np
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine.log_embedder import openai_embedding
from mergify_engine.log_embedder.github_action import get_tokenized_cleaned_log
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository
from mergify_engine.tests.unit.test_utils import add_workflow_job


async def get_cosine_similarity_for_job(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: github_actions.WorkflowJob,
) -> typing.Sequence[github_actions.WorkflowJobLogNeighbours]:
    return (
        await session.scalars(
            sqlalchemy.select(github_actions.WorkflowJobLogNeighbours)
            .where(github_actions.WorkflowJobLogNeighbours.job_id == job.id)
            .order_by(github_actions.WorkflowJobLogNeighbours.neighbour_job_id)
        )
    ).all()


async def test_compute_log_embedding_cosine_similarity(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    # Create dataset ===================================================================
    owner = github_account.GitHubAccount(id=1, login="owner")
    db.add(owner)
    repo1 = github_repository.GitHubRepository(id=1, owner=owner, name="repo1")
    db.add(repo1)
    repo2 = github_repository.GitHubRepository(id=2, owner=owner, name="repo2")
    db.add(repo2)

    # Jobs
    job_pep8_1_repo1 = add_workflow_job(
        db, {"id": 1, "name": "pep8", "repository": repo1, "log_embedding": [1] * 1536}
    )
    job_pep8_2_repo1 = add_workflow_job(
        db, {"id": 2, "name": "pep8", "repository": repo1, "log_embedding": [2] * 1536}
    )
    job_pep8_3_repo1 = add_workflow_job(
        db, {"id": 3, "name": "pep8", "repository": repo1, "log_embedding": [-1] * 1536}
    )
    job_pep8_1_repo2 = add_workflow_job(
        db, {"id": 4, "name": "pep8", "repository": repo2, "log_embedding": [1] * 1536}
    )
    job_pep8_2_repo2 = add_workflow_job(
        db, {"id": 5, "name": "pep8", "repository": repo2, "log_embedding": [2] * 1536}
    )
    job_docker_1_repo2 = add_workflow_job(
        db,
        {"id": 6, "name": "docker", "repository": repo2, "log_embedding": [1] * 1536},
    )
    job_docker_2_repo2 = add_workflow_job(
        db,
        {"id": 7, "name": "docker", "repository": repo2, "log_embedding": [2] * 1536},
    )
    # ==================================================================================

    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo1)
    assert len(results) == 0

    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_1_repo1.id]
    )
    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo1)

    assert len(results) == 2
    assert results[0].job_id == job_pep8_1_repo1.id
    assert results[0].neighbour_job_id == job_pep8_2_repo1.id
    assert results[0].cosine_similarity == 1
    assert results[1].job_id == job_pep8_1_repo1.id
    assert results[1].neighbour_job_id == job_pep8_3_repo1.id
    assert results[1].cosine_similarity == -1

    results = await get_cosine_similarity_for_job(db, job_pep8_2_repo1)
    assert len(results) == 0
    results = await get_cosine_similarity_for_job(db, job_pep8_3_repo1)
    assert len(results) == 0

    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_2_repo1.id, job_pep8_3_repo1.id]
    )

    results = await get_cosine_similarity_for_job(db, job_pep8_2_repo1)
    assert len(results) == 2
    assert results[0].job_id == job_pep8_2_repo1.id
    assert results[0].neighbour_job_id == job_pep8_1_repo1.id
    assert results[0].cosine_similarity == 1
    assert results[1].job_id == job_pep8_2_repo1.id
    assert results[1].neighbour_job_id == job_pep8_3_repo1.id
    assert results[1].cosine_similarity == -1

    results = await get_cosine_similarity_for_job(db, job_pep8_3_repo1)
    assert len(results) == 2
    assert results[0].job_id == job_pep8_3_repo1.id
    assert results[0].neighbour_job_id == job_pep8_1_repo1.id
    assert results[0].cosine_similarity == -1
    assert results[1].job_id == job_pep8_3_repo1.id
    assert results[1].neighbour_job_id == job_pep8_2_repo1.id
    assert results[1].cosine_similarity == -1

    # Test filter repo and name
    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 0
    results = await get_cosine_similarity_for_job(db, job_pep8_2_repo2)
    assert len(results) == 0
    results = await get_cosine_similarity_for_job(db, job_docker_1_repo2)
    assert len(results) == 0
    results = await get_cosine_similarity_for_job(db, job_docker_2_repo2)
    assert len(results) == 0

    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db,
        [
            job_pep8_1_repo2.id,
            job_pep8_2_repo2.id,
            job_docker_1_repo2.id,
            job_docker_2_repo2.id,
        ],
    )

    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_1_repo2.id
    assert results[0].neighbour_job_id == job_pep8_2_repo2.id
    assert results[0].cosine_similarity == 1

    results = await get_cosine_similarity_for_job(db, job_pep8_2_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_2_repo2.id
    assert results[0].neighbour_job_id == job_pep8_1_repo2.id
    assert results[0].cosine_similarity == 1

    results = await get_cosine_similarity_for_job(db, job_docker_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_docker_1_repo2.id
    assert results[0].neighbour_job_id == job_docker_2_repo2.id
    assert results[0].cosine_similarity == 1

    results = await get_cosine_similarity_for_job(db, job_docker_2_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_docker_2_repo2.id
    assert results[0].neighbour_job_id == job_docker_1_repo2.id
    assert results[0].cosine_similarity == 1

    # Test upsert
    job_pep8_1_repo2.log_embedding = np.array([np.float32(-1)] * 1536)
    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_1_repo2.id]
    )
    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_1_repo2.id
    assert results[0].neighbour_job_id == job_pep8_2_repo2.id
    assert results[0].cosine_similarity == -1


@pytest.mark.parametrize(
    "raw_log,expected_lenght,expected_cleaned_log,expected_embedded_log",
    [
        (["hello\n"], 1, "hello", ["hello\n"]),
        (
            ["hello\n"] * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * (openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN),
            ["hello\n"] * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
        ),
        (
            (["hello\n"] * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN)
            + [
                "before the end\n",
                "extra token at the end",
            ],  # NOTE(Kontrolix): When this part is cleaned, it leaves 4 tokens
            openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            ("hello" * (openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 4))
            + "endextra token end",
            (["hello\n"] * (openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 4))
            + ["before the end\n", "extra token at the end"],
        ),
    ],
    ids=["one_token_string", "max_input_token_string", "too_long_input_token_string"],
)
async def test_get_tokenized_cleaned_log(
    raw_log: list[str],
    expected_lenght: int,
    expected_cleaned_log: str,
    expected_embedded_log: list[str],
) -> None:
    tokens, first_line, last_line = await get_tokenized_cleaned_log(raw_log)

    assert len(tokens) == expected_lenght

    assert openai_embedding.TIKTOKEN_ENCODING.decode(tokens) == expected_cleaned_log
    assert raw_log[first_line:last_line] == expected_embedded_log
