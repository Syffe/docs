import typing

import httpx
import numpy as np
import pytest
import respx
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.log_embedder import openai_embedding
from mergify_engine.log_embedder.github_action import get_tokenized_cleaned_log
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository
from mergify_engine.tests.unit.log_embedder import utils


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
    job_pep8_1_repo1 = await utils.add_job(
        db, {"id": 1, "name": "pep8", "repository": repo1, "log_embedding": [1] * 1536}
    )
    job_pep8_2_repo1 = await utils.add_job(
        db, {"id": 2, "name": "pep8", "repository": repo1, "log_embedding": [2] * 1536}
    )
    job_pep8_3_repo1 = await utils.add_job(
        db, {"id": 3, "name": "pep8", "repository": repo1, "log_embedding": [-1] * 1536}
    )
    job_pep8_1_repo2 = await utils.add_job(
        db, {"id": 4, "name": "pep8", "repository": repo2, "log_embedding": [1] * 1536}
    )
    job_pep8_2_repo2 = await utils.add_job(
        db, {"id": 5, "name": "pep8", "repository": repo2, "log_embedding": [2] * 1536}
    )
    job_docker_1_repo2 = await utils.add_job(
        db,
        {"id": 6, "name": "docker", "repository": repo2, "log_embedding": [1] * 1536},
    )
    job_docker_2_repo2 = await utils.add_job(
        db,
        {"id": 7, "name": "docker", "repository": repo2, "log_embedding": [2] * 1536},
    )
    # ==================================================================================

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_1_repo1)
    assert len(results) == 0

    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_1_repo1.id]
    )
    results = await utils.get_cosine_similarity_for_job(db, job_pep8_1_repo1)

    assert len(results) == 2
    assert results[0].job_id == job_pep8_1_repo1.id
    assert results[0].neighbour_job_id == job_pep8_2_repo1.id
    assert results[0].cosine_similarity == 1
    assert results[1].job_id == job_pep8_1_repo1.id
    assert results[1].neighbour_job_id == job_pep8_3_repo1.id
    assert results[1].cosine_similarity == -1

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_2_repo1)
    assert len(results) == 0
    results = await utils.get_cosine_similarity_for_job(db, job_pep8_3_repo1)
    assert len(results) == 0

    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_2_repo1.id, job_pep8_3_repo1.id]
    )

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_2_repo1)
    assert len(results) == 2
    assert results[0].job_id == job_pep8_2_repo1.id
    assert results[0].neighbour_job_id == job_pep8_1_repo1.id
    assert results[0].cosine_similarity == 1
    assert results[1].job_id == job_pep8_2_repo1.id
    assert results[1].neighbour_job_id == job_pep8_3_repo1.id
    assert results[1].cosine_similarity == -1

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_3_repo1)
    assert len(results) == 2
    assert results[0].job_id == job_pep8_3_repo1.id
    assert results[0].neighbour_job_id == job_pep8_1_repo1.id
    assert results[0].cosine_similarity == -1
    assert results[1].job_id == job_pep8_3_repo1.id
    assert results[1].neighbour_job_id == job_pep8_2_repo1.id
    assert results[1].cosine_similarity == -1

    # Test filter repo and name
    results = await utils.get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 0
    results = await utils.get_cosine_similarity_for_job(db, job_pep8_2_repo2)
    assert len(results) == 0
    results = await utils.get_cosine_similarity_for_job(db, job_docker_1_repo2)
    assert len(results) == 0
    results = await utils.get_cosine_similarity_for_job(db, job_docker_2_repo2)
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

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_1_repo2.id
    assert results[0].neighbour_job_id == job_pep8_2_repo2.id
    assert results[0].cosine_similarity == 1

    results = await utils.get_cosine_similarity_for_job(db, job_pep8_2_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_2_repo2.id
    assert results[0].neighbour_job_id == job_pep8_1_repo2.id
    assert results[0].cosine_similarity == 1

    results = await utils.get_cosine_similarity_for_job(db, job_docker_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_docker_1_repo2.id
    assert results[0].neighbour_job_id == job_docker_2_repo2.id
    assert results[0].cosine_similarity == 1

    results = await utils.get_cosine_similarity_for_job(db, job_docker_2_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_docker_2_repo2.id
    assert results[0].neighbour_job_id == job_docker_1_repo2.id
    assert results[0].cosine_similarity == 1

    # Test upsert
    job_pep8_1_repo2.log_embedding = np.array([np.float32(-1)] * 1536)
    await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_1_repo2.id]
    )
    results = await utils.get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_1_repo2.id
    assert results[0].neighbour_job_id == job_pep8_2_repo2.id
    assert results[0].cosine_similarity == -1


@pytest.mark.parametrize(
    "raw_log,expected_lenght,expected_cleaned_log",
    [
        (b"hello", 1, "hello"),
        (
            b"hello" * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
        ),
        (
            b"hello" * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN + b"the end",
            openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * (openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 2)
            + "the end",
        ),
    ],
    ids=["one_token_string", "max_input_token_string", "too_long_input_token_string"],
)
async def test_get_tokenized_cleaned_log(
    respx_mock: respx.MockRouter,
    raw_log: bytes,
    expected_lenght: int,
    expected_cleaned_log: str,
) -> None:
    owner = github_account.GitHubAccount(id=1, login="owner")
    repo = github_repository.GitHubRepository(id=1, owner=owner, name="repo")
    job = github_actions.WorkflowJob(id=1, repository=repo)

    respx_mock.get(f"https://api.github.com/users/{owner.login}/installation").respond(
        200,
        json=github_types.GitHubInstallation(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubInstallationIdType(0),
                "target_type": "Organization",
                "suspended_at": None,
                "permissions": {},
                "account": {
                    "login": owner.login,
                    "id": typing.cast(github_types.GitHubAccountIdType, 1),
                    "type": "User",
                    "avatar_url": "",
                },
            }
        ),
    )
    respx_mock.post("https://api.github.com/app/installations/0/access_tokens").respond(
        200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"}
    )

    respx_mock.get(
        f"/repos/{repo.owner.login}/{repo.name}/actions/jobs/{job.id}/logs",
    ).respond(200, stream=httpx.ByteStream(raw_log))

    tokens = await get_tokenized_cleaned_log(job)

    assert len(tokens) == expected_lenght

    assert openai_embedding.TIKTOKEN_ENCODING.decode(tokens) == expected_cleaned_log
