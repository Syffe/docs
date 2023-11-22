import codecs
from collections import abc
import contextlib
import dataclasses
import datetime
import io
import re
import typing
import zipfile

import daiquiri
from ddtrace import tracer
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import flaky_check
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import google_cloud_storage
from mergify_engine.clients import http
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue


LOG = daiquiri.getLogger(__name__)

# NOTE(Kontrolix): We remove all the windows forbidden path character as github does
WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP = re.compile(r"[\*\"/\\<>:|\?]")
LOG_EMBEDDER_JOBS_BATCH_SIZE = 100
LOG_EMBEDDER_MAX_ATTEMPTS = 30

EXTRACT_DATA_QUERY_TEMPLATE = openai_api.ChatCompletionQuery(
    role="user",
    content="""Analyze the program logs I will provide you and identify the root cause of the program's failure.
Your response must be an array of json object matching the following json structure.
If you find only one failure, add only one object to the array.
If you identify multiple source of failure, add multiple objects in the array.
If you don't find the requested information, don't speculate, fill the field with json `null` value.
JSON response structure:
{"failures: [{json object}]"}
JSON object structure:
{
"problem_type": "What is the root cause",
"language": "The programming language of the program that produces these logs",
"filename": "The filename where the error was raised.",
"lineno": "The line number where the error was raised",
"error": "The precise error type",
"test_framework": "The framework that was running when the error was raised"
"stack_trace: "The stack trace of the error",
}

Logs:
""",
    answer_size=500,
    seed=1,
    temperature=0,
    response_format="json_object",
)


@dataclasses.dataclass
class UnexpectedLogEmbedderError(Exception):
    message: str
    log_extras: dict[str, typing.Any] = dataclasses.field(default_factory=dict)


class ExtractedDataObject(typing.TypedDict):
    problem_type: str | None
    language: str | None
    filename: str | None
    lineno: str | None
    error: str | None
    test_framework: str | None
    stack_trace: str | None


async def embed_log(
    openai_client: openai_api.OpenAIClient,
    gcs_client: google_cloud_storage.GoogleCloudStorageClient | None,
    job: gh_models.WorkflowJob,
) -> None:
    try:
        if job.failed_step_number is None:
            log_lines = await download_failure_annotations(job)
        else:
            log_lines = await download_failed_step_log(job)
    except http.HTTPStatusError as e:
        if e.response.status_code in (410, 404):
            job.log_status = gh_models.WorkflowJobLogStatus.GONE
            return
        raise

    tokens, truncated_log = await get_tokenized_cleaned_log(log_lines)

    embedding = await openai_client.get_embedding(tokens)

    job.log_embedding = embedding
    job.embedded_log = truncated_log
    job.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

    if gcs_client is not None:
        await gcs_client.upload(
            settings.LOG_EMBEDDER_GCS_BUCKET,
            f"{job.repository.owner.id}/{job.repository.id}/{job.id}/logs.gz",
            codecs.encode("".join(log_lines).encode(), encoding="zlib"),
        )
        await gcs_client.upload(
            settings.LOG_EMBEDDER_GCS_BUCKET,
            f"{job.repository.owner.id}/{job.repository.id}/{job.id}/jobs.json",
            json.dumps(job.as_github_dict()).encode("utf-8"),
        )


def get_lines_from_zip(
    zip_file: zipfile.ZipFile,
    job: gh_models.WorkflowJob,
) -> list[str]:
    if job.failed_step_number is None:
        raise RuntimeError(
            "get_lines_from_zip() called on a job without failed_step_number",
        )

    cleaned_job_name = WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP.sub("", job.github_name)

    for i in zip_file.infolist():
        if not i.filename.startswith(f"{cleaned_job_name}/{job.failed_step_number}_"):
            continue

        with zip_file.open(i.filename) as log_file:
            return [line.decode() for line in log_file.readlines()]

    raise UnexpectedLogEmbedderError(
        "log-embedder: job log not found in zip file",
        log_extras={"files": [i.filename for i in zip_file.infolist()]},
    )


async def download_failed_step_log(job: gh_models.WorkflowJob) -> list[str]:
    repo = job.repository

    installation_json = await github.get_installation_from_login(repo.owner.login)

    with io.BytesIO() as zip_data:
        async with github.aget_client(installation_json) as client:
            resp = await client.get(
                f"/repos/{repo.owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs",
            )
            zip_data.write(resp.content)

        with zipfile.ZipFile(zip_data, "r") as zip_file:
            return get_lines_from_zip(zip_file, job)


async def download_failure_annotations(job: gh_models.WorkflowJob) -> list[str]:
    repo = job.repository

    installation_json = await github.get_installation_from_login(repo.owner.login)
    async with github.aget_client(installation_json) as client:
        resp = await client.get(
            f"/repos/{repo.owner.login}/{repo.name}/check-runs/{job.id}/annotations",
        )

        annotations = []
        for annotation in typing.cast(list[github_types.GitHubAnnotation], resp.json()):
            if annotation["annotation_level"] == "failure":
                annotations.append(annotation["message"])

        if not annotations:
            raise UnexpectedLogEmbedderError(
                "log-embedder: No failure annotation found",
                log_extras={
                    "workflow_run_id": job.workflow_run_id,
                    "job_id": job.id,
                    "gh_owner": repo.owner.login,
                    "gh_repo": repo.name,
                },
            )

        return annotations


async def get_tokenized_cleaned_log(
    log_lines: list[str],
) -> tuple[list[int], str]:
    cleaner = log_cleaner.LogCleaner()

    max_embedding_tokens = openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    max_chat_completion_tokens = (
        openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
        - EXTRACT_DATA_QUERY_TEMPLATE.get_tokens_size()
    )

    cleaned_tokens: list[int] = []
    truncated_log = ""
    truncated_log_ready = False
    truncated_log_tokens_length = 0

    cleaner.apply_log_tags("\n".join(log_lines))

    for line in reversed(log_lines):
        if not line:
            continue

        cleaned_line = cleaner.clean_line(line)
        # Start feeding truncated_log only on first non empty line
        if not cleaned_line and not truncated_log:
            continue

        if not truncated_log_ready:
            nb_tokens_in_line = len(openai_api.TIKTOKEN_ENCODING.encode(line))
            next_truncated_log_tokens_length = (
                truncated_log_tokens_length + nb_tokens_in_line
            )

            if next_truncated_log_tokens_length <= max_chat_completion_tokens:
                truncated_log = line + truncated_log
                truncated_log_tokens_length = next_truncated_log_tokens_length

            if truncated_log_tokens_length >= max_chat_completion_tokens:
                truncated_log_ready = True

        if not cleaned_line:
            continue

        tokenized_cleaned_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)

        total_tokens = len(tokenized_cleaned_line) + len(cleaned_tokens)

        if total_tokens <= max_embedding_tokens:
            cleaned_tokens = tokenized_cleaned_line + cleaned_tokens

        if total_tokens >= max_embedding_tokens:
            break

    return cleaned_tokens, truncated_log


async def extract_data_from_log(
    openai_client: openai_api.OpenAIClient,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: gh_models.WorkflowJob,
) -> None:
    if job.embedded_log is None:
        raise RuntimeError("extract_data_from_log called with job.embedded_log=None")

    query = openai_api.ChatCompletionQuery(
        role=EXTRACT_DATA_QUERY_TEMPLATE.role,
        content=f"{EXTRACT_DATA_QUERY_TEMPLATE.content}{job.embedded_log}",
        answer_size=EXTRACT_DATA_QUERY_TEMPLATE.answer_size,
        seed=EXTRACT_DATA_QUERY_TEMPLATE.seed,
        temperature=EXTRACT_DATA_QUERY_TEMPLATE.temperature,
        response_format=EXTRACT_DATA_QUERY_TEMPLATE.response_format,
    )

    chat_completion = await openai_client.get_chat_completion(query)
    chat_response = chat_completion["choices"][0].get("message", {}).get("content")

    if not chat_response:
        raise UnexpectedLogEmbedderError(
            "ChatGPT returned no extracted data for the job log",
            log_extras={"chat_completion": chat_completion},
        )

    extracted_data: list[ExtractedDataObject] = json.loads(chat_response)["failures"]

    for data in extracted_data:
        log_metadata = gh_models.WorkflowJobLogMetadata(workflow_job_id=job.id, **data)
        session.add(log_metadata)
        job.log_metadata.append(log_metadata)


@contextlib.contextmanager
def log_exception_and_maybe_retry(
    job: gh_models.WorkflowJob,
) -> abc.Generator[None, None, None]:
    try:
        yield
    except Exception as exc:
        log_extras = job.as_log_extras()
        if isinstance(exc, UnexpectedLogEmbedderError):
            log_extras.update(exc.log_extras)

        if exceptions.should_be_ignored(exc):
            job.log_status = gh_models.WorkflowJobLogStatus.ERROR
            LOG.warning(
                "log-embedder: failed with a fatal error",
                exc_info=True,
                **log_extras,
            )
            return

        if (
            isinstance(exc, http.RequestError)
            and exc.request.url == openai_api.OPENAI_API_BASE_URL
        ):
            # NOTE(sileht): This cost money so we don't want to retry too often
            base_retry_in = 10
        else:
            base_retry_in = 1

        retry_in = exceptions.need_retry(exc, base_retry_in=base_retry_in)
        if retry_in is None:
            # TODO(sileht): Maybe replace me by a exponential backoff + 1 hours limit
            retry_in = datetime.timedelta(minutes=10)

        job.log_embedding_retry_after = date.utcnow() + retry_in
        job.log_embedding_attempts += 1

        if job.log_embedding_attempts >= LOG_EMBEDDER_MAX_ATTEMPTS:
            job.log_status = gh_models.WorkflowJobLogStatus.ERROR
            LOG.error(
                "log-embedder: too many unexpected failures, giving up",
                exc_info=True,
                **log_extras,
            )
            return

        if isinstance(exc, http.RequestError | http.HTTPServerSideError):
            # We don't want to log anything related to network and HTTP server side error
            return

        LOG.error(
            "log-embedder: unexpected failure, retrying later",
            exc_info=True,
            **log_extras,
        )


@tracer.wrap("embed-logs")
async def embed_logs(redis_links: redis_utils.RedisLinks) -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    wjob = orm.aliased(gh_models.WorkflowJob, name="wjob")

    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(wjob)
            .options(orm.joinedload(wjob.ci_issue), orm.joinedload(wjob.log_metadata))
            .join(gh_models.GitHubRepository)
            .join(
                gh_models.GitHubAccount,
                sqlalchemy.and_(
                    gh_models.GitHubRepository.owner_id == gh_models.GitHubAccount.id,
                    gh_models.GitHubAccount.login.in_(
                        settings.LOG_EMBEDDER_ENABLED_ORGS,
                    ),
                ),
            )
            .where(
                wjob.conclusion == gh_models.WorkflowJobConclusion.FAILURE,
                sqlalchemy.or_(
                    wjob.log_embedding_retry_after.is_(None),
                    wjob.log_embedding_retry_after <= date.utcnow(),
                ),
                wjob.failed_step_number.is_not(None),
                wjob.log_status.notin_(
                    (
                        gh_models.WorkflowJobLogStatus.GONE,
                        gh_models.WorkflowJobLogStatus.ERROR,
                    ),
                ),
                sqlalchemy.or_(
                    wjob.log_embedding.is_(None),
                    wjob.ci_issue_id.is_(None),
                    wjob.ci_issue.has(CiIssue.name.is_(None)),
                    ~wjob.log_metadata.any(),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )

        jobs = (await session.scalars(stmt)).unique().all()

        LOG.info("log-embedder: %d jobs to embed", len(jobs), request=str(stmt))

        gcs_client = (
            None
            if settings.LOG_EMBEDDER_GCS_CREDENTIALS is None
            else google_cloud_storage.GoogleCloudStorageClient(
                settings.LOG_EMBEDDER_GCS_CREDENTIALS,
            )
        )
        async with openai_api.OpenAIClient() as openai_client:
            refresh_ready_job_ids = []
            for job in jobs:
                with log_exception_and_maybe_retry(job):
                    if job.log_embedding is None:
                        await embed_log(openai_client, gcs_client, job)

                    if job.embedded_log is not None:
                        await extract_data_from_log(openai_client, session, job)
                        if job.ci_issue is not None and job.ci_issue.name is None:
                            job.ci_issue.name = job.log_metadata[0].problem_type

                    if (
                        job.log_status == gh_models.WorkflowJobLogStatus.EMBEDDED
                        and job.ci_issue_id is None
                    ):
                        await CiIssue.link_job_to_ci_issue(session, job)
                        if job.ci_issue is not None and job.ci_issue.name is None:
                            job.ci_issue.name = job.log_metadata[0].problem_type

                        refresh_ready_job_ids.append(job.id)

                await session.commit()

        await session.commit()

        await flaky_check.send_pull_refresh_for_jobs(redis_links, refresh_ready_job_ids)

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
