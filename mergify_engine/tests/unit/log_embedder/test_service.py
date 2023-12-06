import datetime
import io
import json
import os
import re
import typing
from unittest import mock
import zipfile

import numpy as np
import numpy.typing as npt
import pytest
import respx
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import ci_issue
from mergify_engine.models import github as gh_models
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)


GHA_CI_LOGS_ZIP_DIRECTORY = "zfixtures/unit/log_embedder/gha-ci-logs-examples"
GHA_CI_LOGS_ZIP_BUFFER = io.BytesIO()

with zipfile.ZipFile(GHA_CI_LOGS_ZIP_BUFFER, "w") as zip_object:
    for folder_name, _sub_folders, file_names in os.walk(GHA_CI_LOGS_ZIP_DIRECTORY):
        for filename in file_names:
            file_path = os.path.join(folder_name, filename)
            zip_object.write(file_path, file_path[len(GHA_CI_LOGS_ZIP_DIRECTORY) :])

GHA_CI_LOGS_ZIP_BUFFER.seek(0)
GHA_CI_LOGS_ZIP = GHA_CI_LOGS_ZIP_BUFFER.read()


@pytest.mark.usefixtures("prepare_google_cloud_storage_setup")
async def test_embed_logs_on_controlled_data(
    respx_mock: respx.MockRouter,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
) -> None:
    owner = gh_models.GitHubAccount(
        id=1,
        login="owner_login",
        avatar_url="https://dummy.com",
    )
    db.add(owner)
    repo = gh_models.GitHubRepository(id=1, owner=owner, name="repo_name")
    db.add(repo)

    respx_mock.get(
        f"{settings.GITHUB_REST_API_URL}/users/{owner.login}/installation",
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
            },
        ),
    )
    respx_mock.post(
        f"{settings.GITHUB_REST_API_URL}/app/installations/0/access_tokens",
    ).respond(200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"})
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
                },
            ],
            "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    json_response = {
        "id": "chatcmpl-123",
        "object": "chat.completion",
        "created": 1677652288,
        "model": "gpt-3.5-turbo-0613",
        "choices": [
            {
                "index": 0,
                "delta": {
                    "role": "assistant",
                    "content": """
                        {
                            "failures": [
                                {
                                    "problem_type": "Toto title",
                                    "language": "Python",
                                    "filename": "toto.py",
                                    "lineno": null,
                                    "error": "Exception",
                                    "test_framework": "pytest",
                                    "stack_trace": ""
                                }
                            ]
                        }""",
                },
                "finish_reason": "stop",
            },
        ],
        "usage": {"prompt_tokens": 9, "completion_tokens": 12, "total_tokens": 21},
    }
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
    ).respond(
        200,
        content_type="text/event-stream",
        content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
    )

    # NOTE(Kontrolix): Reduce batch size to speed up test
    monkeypatch.setattr(github_action, "LOG_EMBEDDER_JOBS_BATCH_SIZE", 2)

    # Create 3 jobs (LOG_EMBEDDER_JOBS_BATCH_SIZE + 1)
    for i in range(3):
        job = gh_models.WorkflowJob(
            id=i,
            repository=repo,
            workflow_run_id=1,
            name_without_matrix="job_toto",
            started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
            completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
            conclusion=gh_models.WorkflowJobConclusion.FAILURE,
            labels=[],
            run_attempt=1,
            failed_step_name="toto",
            failed_step_number=1,
            head_sha="",
        )
        db.add(job)

        respx_mock.get(
            f"{settings.GITHUB_REST_API_URL}/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
        ).respond(
            200,
            stream=GHA_CI_LOGS_ZIP,  # type: ignore[arg-type]
        )

    await db.commit()

    pending_work = await github_action.embed_logs(redis_links)
    assert not pending_work
    jobs = (await db.scalars(sqlalchemy.select(gh_models.WorkflowJob))).all()
    assert len(jobs) == 3
    assert all(job.log_embedding is None for job in jobs)

    db.expunge_all()

    monkeypatch.setattr(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        [owner.login],
    )
    pending_work = await github_action.embed_logs(redis_links)
    assert pending_work
    pending_work = await github_action.embed_logs(redis_links)
    assert not pending_work

    jobs = (await db.scalars(sqlalchemy.select(gh_models.WorkflowJob))).all()
    assert len(jobs) == 3
    assert all(
        np.array_equal(
            # FIXME(sileht): This typing issue is annoying, sqlalchemy.Vector is not recognized as ndarray by mypy
            typing.cast(npt.NDArray[np.float32], job.log_embedding),
            OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"],
        )
        for job in jobs
    )

    # NOTE(Kontolix): Validate that if embedding has been computed for a job but neighbours
    # have not been computed, if we call the service, only neighbours are computed for that job.

    embedding_control_value = np.array([np.float32(-1)] * 1536)
    # Change embedding value to be sure that it is not compute again
    jobs[0].log_embedding = embedding_control_value
    await db.commit()

    pending_work = await github_action.embed_logs(redis_links)
    assert not pending_work

    db.expunge_all()

    a_job = await db.get(
        gh_models.WorkflowJob,
        jobs[0].id,
        options=[
            orm.joinedload(gh_models.WorkflowJob.ci_issue).selectinload(
                ci_issue.CiIssue.jobs,
            ),
            orm.joinedload(gh_models.WorkflowJob.log_metadata),
            orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt).selectinload(
                ci_issue.CiIssueGPT.jobs,
            ),
        ],
    )

    assert a_job is not None
    assert np.array_equal(
        typing.cast(npt.NDArray[np.float32], a_job.log_embedding),
        embedding_control_value,
    )
    assert a_job.log_status == gh_models.WorkflowJobLogStatus.DOWNLOADED
    assert (
        a_job.log_embedding_status == gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED
    )
    assert (
        a_job.log_metadata_extracting_status
        == gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED
    )
    assert a_job.ci_issue_id is not None
    assert a_job.ci_issue.name is None
    assert len(a_job.ci_issue.jobs) == 3

    assert len(a_job.log_metadata) == 1
    assert len(a_job.ci_issues_gpt) == 1
    assert a_job.ci_issues_gpt[0].name == "Toto title"
    assert len(a_job.ci_issues_gpt[0].jobs) == 3


@pytest.mark.populated_db_datasets("WorkflowJob")
@pytest.mark.usefixtures("prepare_google_cloud_storage_setup")
async def test_embed_logs_on_various_data(
    respx_mock: respx.MockRouter,
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
) -> None:
    respx_mock.get(
        re.compile(f"{settings.GITHUB_REST_API_URL}/users/.+/installation"),
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
            },
        ),
    )
    respx_mock.post(
        f"{settings.GITHUB_REST_API_URL}/app/installations/0/access_tokens",
    ).respond(200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"})

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
                },
            ],
            "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    json_response = {
        "id": "chatcmpl-123",
        "object": "chat.completion",
        "created": 1677652288,
        "model": "gpt-3.5-turbo-0613",
        "choices": [
            {
                "index": 0,
                "delta": {
                    "role": "assistant",
                    "content": """
                        {
                            "failures": [
                                {
                                    "problem_type": "Toto title",
                                    "language": "Python",
                                    "filename": "toto.py",
                                    "lineno": null,
                                    "error": "Exception",
                                    "test_framework": "pytest",
                                    "stack_trace": ""
                                }
                            ]
                        }""",
                },
                "finish_reason": "stop",
            },
        ],
        "usage": {"prompt_tokens": 9, "completion_tokens": 12, "total_tokens": 21},
    }
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
    ).respond(
        200,
        content_type="text/event-stream",
        content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
    )

    respx_mock.get(
        re.compile(
            f"{settings.GITHUB_REST_API_URL}/repos/.+/.+/actions/runs/.+/attempts/.+/logs",
        ),
    ).respond(
        200,
        stream=GHA_CI_LOGS_ZIP,  # type: ignore[arg-type]
    )

    # Update jobs name and failed step to match the zip archive mock
    await populated_db.execute(
        sqlalchemy.update(gh_models.WorkflowJob)
        .values(
            failed_step_name="toto",
            failed_step_number=1,
            name_without_matrix="job_toto",
        )
        .where(
            gh_models.WorkflowJob.failed_step_name.is_not(None),
            gh_models.WorkflowJob.failed_step_number.is_not(None),
        ),
    )
    await populated_db.commit()

    logins = set()
    for job in (
        await populated_db.execute(sqlalchemy.select(gh_models.WorkflowJob))
    ).scalars():
        logins.add(job.repository.owner.login)

    monkeypatch.setattr(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        list(logins),
    )

    count = (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count(gh_models.WorkflowJob.id)).where(
                gh_models.WorkflowJob.embedded_log.is_not(None),
            ),
        )
    ).all()[0][0]

    assert count == 5

    pending_work = True
    while pending_work:
        pending_work = await github_action.embed_logs(redis_links)

    count = (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count(gh_models.WorkflowJob.id)).where(
                gh_models.WorkflowJob.embedded_log.is_not(None),
            ),
        )
    ).all()[0][0]

    assert count == 7


@pytest.mark.ignored_logging_errors(
    "log-embedder: unexpected failure, retrying later",
    "log-embedder: too many unexpected failures, giving up",
)
@mock.patch.object(
    exceptions,
    "need_retry",
    return_value=datetime.timedelta(seconds=-60),
)
@pytest.mark.parametrize(
    "first_try, second_try, log_errors",
    (
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_extracted",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            [],
        ),
        (
            {
                "mock_log": "mock_log_gone",
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.GONE,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.GONE,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            [],
        ),
        (
            {
                "mock_log": "mock_log_error",
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.ERROR,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.ERROR,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            ["log-embedder: too many unexpected failures, giving up"],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_error",
                "mock_extracting": "mock_extracting_metadata_extracted",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            [],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_error",
                "mock_extracting": "mock_extracting_metadata_extracted",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.ERROR,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            ["log-embedder: too many unexpected failures, giving up"],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_misformated",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": "mock_extracting_metadata_extracted",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            ["log-embedder: unexpected failure, retrying later"],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_misformated",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR,
            },
            [
                "log-embedder: unexpected failure, retrying later",
                "log-embedder: too many unexpected failures, giving up",
            ],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_invalid_json",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": "mock_extracting_metadata_error",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR,
            },
            [
                "log-embedder: unexpected failure, retrying later",
                "log-embedder: too many unexpected failures, giving up",
            ],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_empty_failures",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            [],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_none_failures",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            [],
        ),
        (
            {
                "mock_log": "mock_log_downloaded",
                "mock_embedding": "mock_embedding_embedded",
                "mock_extracting": "mock_extracting_metadata_finish_reason_length",
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            {
                "mock_log": None,
                "mock_embedding": None,
                "mock_extracting": None,
                "log": gh_models.WorkflowJobLogStatus.DOWNLOADED,
                "embedding": gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                "metadata": gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
            },
            [],
        ),
    ),
)
@pytest.mark.usefixtures("prepare_google_cloud_storage_setup")
async def test_workflow_job_log_life_cycle(
    _: None,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    caplog: pytest.LogCaptureFixture,
    first_try: dict[str, typing.Any],
    second_try: dict[str, typing.Any],
    log_errors: list[str],
) -> None:
    owner = gh_models.GitHubAccount(id=1, login="owner", avatar_url="https://dummy.com")
    repo = gh_models.GitHubRepository(id=1, owner=owner, name="repo1")
    job = gh_models.WorkflowJob(
        id=1,
        repository=repo,
        workflow_run_id=1,
        name_without_matrix="job_toto",
        started_at=date.utcnow(),
        completed_at=date.utcnow(),
        conclusion=gh_models.WorkflowJobConclusion.FAILURE,
        labels=[],
        run_attempt=1,
        failed_step_name="toto",
        failed_step_number=1,
        head_sha="",
    )
    db.add(owner)
    db.add(repo)
    db.add(job)
    await db.commit()
    db.expunge_all()

    respx_mock.get(
        f"{settings.GITHUB_REST_API_URL}/users/{owner.login}/installation",
    ).respond(
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
            },
        ),
    )
    respx_mock.post(
        f"{settings.GITHUB_REST_API_URL}/app/installations/0/access_tokens",
    ).respond(200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"})

    def preprare_mocking(mocking_info: dict[str, typing.Any]) -> None:
        if mocking_info["mock_log"] == "mock_log_gone":
            respx_mock.get(
                f"{settings.GITHUB_REST_API_URL}/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
            ).respond(410)

        if mocking_info["mock_log"] == "mock_log_downloaded":
            respx_mock.get(
                f"{settings.GITHUB_REST_API_URL}/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
            ).respond(
                200,
                stream=GHA_CI_LOGS_ZIP,  # type: ignore[arg-type]
            )

        if mocking_info["mock_log"] == "mock_log_error":
            respx_mock.get(
                f"{settings.GITHUB_REST_API_URL}/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
            ).respond(500)

        if mocking_info["mock_embedding"] == "mock_embedding_embedded":
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
                        },
                    ],
                    "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
                    "usage": {"prompt_tokens": 2, "total_tokens": 2},
                },
            )

        if mocking_info["mock_embedding"] == "mock_embedding_error":
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/embeddings",
            ).respond(500)

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_extracted":
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": [
                                        {
                                            "problem_type": "Toto title",
                                            "language": "Python",
                                            "filename": "toto.py",
                                            "lineno": null,
                                            "error": "Exception",
                                            "test_framework": "pytest",
                                            "stack_trace": ""
                                        }
                                    ]
                                }""",
                        },
                        "finish_reason": "stop",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_misformated":
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": [
                                        {
                                            "ttttt": "Toto title",
                                            "aaaaa": "Python",
                                            "filename": "toto.py",
                                            "lineno": null,
                                            "ssss": "Exception",
                                            "test_framework": "pytest",
                                            "stack_trace": ""
                                        }
                                    ]
                                }""",
                        },
                        "finish_reason": "stop",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_invalid_json":
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": [
                                        {
                                            dfqdfezff
                                        }
                                    ]
                                }""",
                        },
                        "finish_reason": "stop",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_empty_failures":
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": [null]
                                }""",
                        },
                        "finish_reason": "stop",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_none_failures":
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": null
                                }""",
                        },
                        "finish_reason": "stop",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if (
            mocking_info["mock_extracting"]
            == "mock_extracting_metadata_finish_reason_length"
        ):
            json_response = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-3.5-turbo-0613",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": """
                                {
                                    "failures": {
                                """,
                        },
                        "finish_reason": "length",
                    },
                ],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                },
            }
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                200,
                content_type="text/event-stream",
                content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
            )

        if mocking_info["mock_extracting"] == "mock_extracting_metadata_error":
            respx_mock.post(
                f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
            ).respond(
                500,
            )

    monkeypatch.setattr(settings, "LOG_EMBEDDER_ENABLED_ORGS", [owner.login])
    monkeypatch.setattr(github_action, "LOG_EMBEDDER_MAX_ATTEMPTS", 2)

    job = await db.get_one(gh_models.WorkflowJob, 1)
    assert job.log_status == gh_models.WorkflowJobLogStatus.UNKNOWN
    assert job.log_embedding_status == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN
    assert (
        job.log_metadata_extracting_status
        == gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN
    )
    db.expunge_all()

    preprare_mocking(first_try)

    await github_action.embed_logs(redis_links)

    job = await db.get_one(gh_models.WorkflowJob, 1)
    assert job.log_status == first_try["log"]
    assert job.log_embedding_status == first_try["embedding"]
    assert job.log_metadata_extracting_status == first_try["metadata"]
    db.expunge_all()

    preprare_mocking(second_try)

    await github_action.embed_logs(redis_links)

    job = await db.get_one(gh_models.WorkflowJob, 1)
    assert job.log_status == second_try["log"]
    assert job.log_embedding_status == second_try["embedding"]
    assert job.log_metadata_extracting_status == second_try["metadata"]

    assert [
        r.message for r in caplog.get_records(when="call") if r.levelname == "ERROR"
    ] == log_errors

    assert job.log_metadata_extracting_status == second_try["metadata"]


@pytest.mark.ignored_logging_errors(
    "log-embedder: too many unexpected failures, giving up",
)
@pytest.mark.parametrize(
    "job_name, step, log_status, embedding_status, metadata_status",
    (
        (
            "job_toto",
            1,
            gh_models.WorkflowJobLogStatus.DOWNLOADED,
            gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
            gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
        ),
        (
            "front (format:check)",
            4,
            gh_models.WorkflowJobLogStatus.DOWNLOADED,
            gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
            gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
        ),
        (
            "job_toto",
            6,
            gh_models.WorkflowJobLogStatus.DOWNLOADED,
            gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
            gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
        ),
    ),
    ids=("simple", "special chars in name", "huge logs"),
)
@pytest.mark.usefixtures("prepare_google_cloud_storage_setup")
async def test_workflow_job_from_real_life(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
    step: int,
    redis_links: redis_utils.RedisLinks,
    log_status: gh_models.WorkflowJobLogStatus,
    embedding_status: gh_models.WorkflowJobLogEmbeddingStatus,
    metadata_status: gh_models.WorkflowJobLogMetadataExtractingStatus,
) -> None:
    owner = gh_models.GitHubAccount(id=1, login="owner", avatar_url="https://dummy.com")
    repo = gh_models.GitHubRepository(id=1, owner=owner, name="repo1")
    job = gh_models.WorkflowJob(
        id=1,
        repository=repo,
        workflow_run_id=1,
        name_without_matrix=job_name,
        started_at=date.utcnow(),
        completed_at=date.utcnow(),
        conclusion=gh_models.WorkflowJobConclusion.FAILURE,
        labels=[],
        run_attempt=1,
        failed_step_name="toto",
        failed_step_number=step,
        head_sha="",
    )

    db.add(owner)
    db.add(repo)
    db.add(job)
    await db.commit()
    db.expunge_all()

    respx_mock.get(
        f"{settings.GITHUB_REST_API_URL}/users/{owner.login}/installation",
    ).respond(
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
            },
        ),
    )
    respx_mock.post(
        f"{settings.GITHUB_REST_API_URL}/app/installations/0/access_tokens",
    ).respond(200, json={"token": "<app_token>", "expires_at": "2100-12-31T23:59:59Z"})

    respx_mock.get(
        f"{settings.GITHUB_REST_API_URL}/repos/{owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
    ).respond(
        200,
        stream=GHA_CI_LOGS_ZIP,  # type: ignore[arg-type]
    )

    if embedding_status.value != "unknown":
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
                    },
                ],
                "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
                "usage": {"prompt_tokens": 2, "total_tokens": 2},
            },
        )

    if metadata_status.value != "unknown":
        json_response = {
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-3.5-turbo-0613",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": """
                            {
                                "failures": [
                                    {
                                        "problem_type": "Toto title",
                                        "language": "Python",
                                        "filename": "toto.py",
                                        "lineno": null,
                                        "error": "Exception",
                                        "test_framework": "pytest",
                                        "stack_trace": ""
                                    }
                                ]
                            }""",
                    },
                    "finish_reason": "stop",
                },
            ],
            "usage": {"prompt_tokens": 9, "completion_tokens": 12, "total_tokens": 21},
        }
        respx_mock.post(
            f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
        ).respond(
            200,
            content_type="text/event-stream",
            content=f"data: {json.dumps(json_response)}\n\ndata: [DONE]\n\n".encode(),
        )

    monkeypatch.setattr(settings, "LOG_EMBEDDER_ENABLED_ORGS", [owner.login])
    monkeypatch.setattr(github_action, "LOG_EMBEDDER_MAX_ATTEMPTS", 1)

    jobs = list((await db.scalars(sqlalchemy.select(gh_models.WorkflowJob))).all())
    assert len(jobs) == 1
    assert jobs[0].log_status == gh_models.WorkflowJobLogStatus.UNKNOWN
    assert (
        jobs[0].log_embedding_status == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN
    )
    assert (
        jobs[0].log_metadata_extracting_status
        == gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN
    )

    db.expunge_all()

    await github_action.embed_logs(redis_links)

    jobs = list((await db.scalars(sqlalchemy.select(gh_models.WorkflowJob))).all())
    assert len(jobs) == 1
    assert jobs[0].log_status == log_status
    assert jobs[0].log_embedding_status == embedding_status
    assert jobs[0].log_metadata_extracting_status == metadata_status

    db.expunge_all()
