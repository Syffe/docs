import typing

import numpy as np
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github as gh_models
from mergify_engine.tests import utils as tests_utils


async def get_cosine_similarity_for_job(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: gh_models.WorkflowJob,
) -> typing.Sequence[gh_models.WorkflowJobLogNeighbours]:
    return (
        await session.scalars(
            sqlalchemy.select(gh_models.WorkflowJobLogNeighbours)
            .where(gh_models.WorkflowJobLogNeighbours.job_id == job.id)
            .order_by(gh_models.WorkflowJobLogNeighbours.neighbour_job_id)
        )
    ).all()


async def test_compute_log_embedding_cosine_similarity(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    # Create dataset ===================================================================
    owner = gh_models.GitHubAccount(id=1, login="owner", avatar_url="https://dummy.com")
    db.add(owner)
    repo1 = gh_models.GitHubRepository(id=1, owner=owner, name="repo1")
    db.add(repo1)
    repo2 = gh_models.GitHubRepository(id=2, owner=owner, name="repo2")
    db.add(repo2)

    # Jobs
    job_pep8_1_repo1 = tests_utils.add_workflow_job(
        db,
        {
            "id": 1,
            "name_without_matrix": "pep8",
            "repository": repo1,
            "log_embedding": [1] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_pep8_2_repo1 = tests_utils.add_workflow_job(
        db,
        {
            "id": 2,
            "name_without_matrix": "pep8",
            "repository": repo1,
            "log_embedding": [2] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_pep8_3_repo1 = tests_utils.add_workflow_job(
        db,
        {
            "id": 3,
            "name_without_matrix": "pep8",
            "repository": repo1,
            "log_embedding": [-1] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_pep8_1_repo2 = tests_utils.add_workflow_job(
        db,
        {
            "id": 4,
            "name_without_matrix": "pep8",
            "repository": repo2,
            "log_embedding": [1] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_pep8_2_repo2 = tests_utils.add_workflow_job(
        db,
        {
            "id": 5,
            "name_without_matrix": "pep8",
            "repository": repo2,
            "log_embedding": [2] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_docker_1_repo2 = tests_utils.add_workflow_job(
        db,
        {
            "id": 6,
            "name_without_matrix": "docker",
            "repository": repo2,
            "log_embedding": [1] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    job_docker_2_repo2 = tests_utils.add_workflow_job(
        db,
        {
            "id": 7,
            "name_without_matrix": "docker",
            "repository": repo2,
            "log_embedding": [2] * 1536,
            "embedded_log": "whatever",
            "log_status": gh_models.WorkflowJobLogStatus.EMBEDDED,
        },
    )
    # ==================================================================================

    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo1)
    assert len(results) == 0

    await gh_models.WorkflowJob.compute_logs_embedding_cosine_similarity(
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

    await gh_models.WorkflowJob.compute_logs_embedding_cosine_similarity(
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

    await gh_models.WorkflowJob.compute_logs_embedding_cosine_similarity(
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
    await gh_models.WorkflowJob.compute_logs_embedding_cosine_similarity(
        db, [job_pep8_1_repo2.id]
    )
    results = await get_cosine_similarity_for_job(db, job_pep8_1_repo2)
    assert len(results) == 1
    assert results[0].job_id == job_pep8_1_repo2.id
    assert results[0].neighbour_job_id == job_pep8_2_repo2.id
    assert results[0].cosine_similarity == -1


MAX_TOKENS_EMBEDDED_LOG = (
    openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
    - github_action.ERROR_TITLE_QUERY_TEMPLATE.get_tokens_size()
)


@pytest.mark.parametrize(
    "raw_log,expected_length,expected_cleaned_log,expected_embedded_log",
    [
        (["hello\n"], 1, "hello", "hello\n"),
        (
            ["hello\n"] * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * (openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN),
            # NOTE(sileht): 'hello\n' it's two token and we can't go over the bigger chat model
            "hello\n" * int(MAX_TOKENS_EMBEDDED_LOG / 2),
        ),
        (
            (["hello\n"] * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN)
            + [
                "before the end\n",
                "extra token at the end",
            ],
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            # NOTE(Kontrolix): When this part is cleaned, it leaves 4 tokens
            ("hello" * (openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 4))
            + "endextra token end",
            (
                # NOTE(sileht): 'hello\n' it's two token and we can't go over the bigger chat model
                "hello\n" * (int((MAX_TOKENS_EMBEDDED_LOG - 9) / 2))
                # 9 tokens
                + "before the end\nextra token at the end"
            ),
        ),
        (
            # more is removed by LogCleaner, this ensures we remove end of files
            # from emebedded_log and log_embedding
            ["hello\n", "more\n", "more\n", "more\n"],
            1,
            "hello",
            "hello\n",
        ),
    ],
    ids=[
        "one_token_string",
        "max_input_token_string",
        "too_long_input_token_string",
        "ending_lines_removed",
    ],
)
async def test_get_tokenized_cleaned_log(
    raw_log: list[str],
    expected_length: int,
    expected_cleaned_log: str,
    expected_embedded_log: str,
) -> None:
    tokens, truncated_log = await github_action.get_tokenized_cleaned_log(raw_log)

    assert len(tokens) == expected_length

    assert openai_api.TIKTOKEN_ENCODING.decode(tokens) == expected_cleaned_log
    assert len(truncated_log) == len(expected_embedded_log)
    assert truncated_log == expected_embedded_log
