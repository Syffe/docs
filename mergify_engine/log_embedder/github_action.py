import dataclasses
import datetime
import io
import json
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
from mergify_engine import json as mergify_json
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import google_cloud_storage
from mergify_engine.clients import http
from mergify_engine.log_embedder import log as logm
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import ci_issue
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)

# NOTE(Kontrolix): We remove all the windows forbidden path character as github does
WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP = re.compile(r"[\*\"/\\<>:|\?]")
LOG_EMBEDDER_JOBS_BATCH_SIZE = 100
LOG_EMBEDDER_MAX_ATTEMPTS = 30

JSON_STRUCTURE = {
    "problem_type": "What is the root cause",
    "language": "The programming language of the program that produces these logs",
    "filename": "The filename where the error was raised.",
    "lineno": "The line number where the error was raised",
    "error": "The precise error type",
    "test_framework": "The framework that was running when the error was raised",
    "stack_trace": "The stack trace of the error",
}

EXTRACT_DATA_QUERY_TEMPLATE = openai_api.ChatCompletionQuery(
    role="user",
    content=f"""Analyze the program logs I will provide you and identify the root cause of the program's failure.
Your response must be an array of json object matching the following json structure.
If you find only one failure, add only one object to the array.
If you identify multiple source of failure, add multiple objects in the array.
If you don't find the requested information, don't speculate, fill the field with json `null` value.
JSON response structure: {{"failures: [{{json object}}]"}}
JSON object structure: {json.dumps(JSON_STRUCTURE)}
Logs:
""",
    answer_size=500,
    seed=1,
    temperature=0,
    response_format="json_object",
)

MAX_CHAT_COMPLETION_TOKENS = (
    openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
    - EXTRACT_DATA_QUERY_TEMPLATE.get_tokens_size()
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


async def fetch_and_store_log(
    gcs_client: google_cloud_storage.GoogleCloudStorageClient,
    job: gh_models.WorkflowJob,
) -> logm.Log:
    """Fetch and store original log from GitHub

    Returns a list of log lines."""
    installation = await github.get_installation_from_login(
        job.repository.owner.login,
    )

    async with github.aget_client(installation) as client:
        if job.failed_step_number is None:
            log = logm.Log.from_lines(await job.download_failure_annotations(client))
        else:
            zipped_logs_content = await job.download_failed_logs(client)
            log = logm.Log.from_bytes(
                get_step_log_from_zipped_content(
                    zipped_logs_content,
                    job.github_name,
                    job.failed_step_number,
                ),
            )

    await gcs_client.upload(
        settings.LOG_EMBEDDER_GCS_BUCKET,
        f"{job.repository.owner.id}/{job.repository.id}/{job.id}/logs.gz",
        log.encode(),
    )
    await gcs_client.upload(
        settings.LOG_EMBEDDER_GCS_BUCKET,
        f"{job.repository.owner.id}/{job.repository.id}/{job.id}/jobs.json",
        mergify_json.dumps(job.as_github_dict()).encode("utf-8"),
    )

    job.log_status = gh_models.WorkflowJobLogStatus.DOWNLOADED

    return log


async def get_log(
    gcs_client: google_cloud_storage.GoogleCloudStorageClient,
    job: gh_models.WorkflowJob,
) -> logm.Log:
    if job.log_status == gh_models.WorkflowJobLogStatus.DOWNLOADED:
        log = await gcs_client.download(
            settings.LOG_EMBEDDER_GCS_BUCKET,
            f"{job.repository.owner.id}/{job.repository.id}/{job.id}/logs.gz",
        )

        if log is None:
            LOG.error(
                "Downloaded log is missing from bucket, re-downloading",
                **job.as_log_extras(),
            )
            return await fetch_and_store_log(gcs_client, job)

        return logm.Log.decode(log.download_as_bytes())

    return await fetch_and_store_log(gcs_client, job)


async def embed_log(
    openai_client: openai_api.OpenAIClient,
    job: gh_models.WorkflowJob,
    log: logm.Log,
) -> None:
    tokens, truncated_log = await get_tokenized_cleaned_log(log)

    embedding = await openai_client.get_embedding(tokens)

    job.log_embedding = embedding
    job.embedded_log = truncated_log
    job.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED


def get_step_log_from_zipped_content(
    zipped_log_content: bytes,
    github_name: str,
    failed_step_number: int,
) -> bytes:
    cleaned_job_name = WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP.sub("", github_name)

    with io.BytesIO() as zip_data:
        zip_data.write(zipped_log_content)

        with zipfile.ZipFile(zip_data, "r") as zip_file:
            for i in zip_file.infolist():
                if not i.filename.startswith(
                    f"{cleaned_job_name}/{failed_step_number}_",
                ):
                    continue

                with zip_file.open(i.filename) as log_file:
                    return log_file.read()

    # FIXME(sileht): We should mark the job as ERROR instead of retrying
    raise UnexpectedLogEmbedderError(
        "log-embedder: job log not found in zip file",
        log_extras={"files": [i.filename for i in zip_file.infolist()]},
    )


async def get_tokenized_cleaned_log(
    log: logm.Log,
) -> tuple[list[int], str]:
    cleaner = log_cleaner.LogCleaner()

    cleaned_tokens: list[int] = []
    truncated_log_lines: list[str] = []
    truncated_log_ready = False
    truncated_log_tokens_length = 0

    for line in reversed(log.lines):
        if not line:
            continue

        cleaned_line = cleaner.clean_line(line, log.tags)
        # Start feeding truncated_log only on first non empty line
        if not cleaned_line and not truncated_log_lines:
            continue

        if not truncated_log_ready:
            nb_tokens_in_line = len(openai_api.TIKTOKEN_ENCODING.encode(line))
            next_truncated_log_tokens_length = (
                truncated_log_tokens_length + nb_tokens_in_line
            )

            if next_truncated_log_tokens_length <= MAX_CHAT_COMPLETION_TOKENS:
                truncated_log_lines.insert(0, line)
                truncated_log_tokens_length = next_truncated_log_tokens_length

            if truncated_log_tokens_length >= MAX_CHAT_COMPLETION_TOKENS:
                truncated_log_ready = True

        if not cleaned_line:
            continue

        tokenized_cleaned_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)

        total_tokens = len(tokenized_cleaned_line) + len(cleaned_tokens)

        if total_tokens <= openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN:
            cleaned_tokens = tokenized_cleaned_line + cleaned_tokens

        if total_tokens >= openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN:
            break

    return cleaned_tokens, "\n".join(truncated_log_lines)


async def extract_data_from_log(
    openai_client: openai_api.OpenAIClient,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: gh_models.WorkflowJob,
    log_lines: list[str],
) -> None:
    cleaned_log = get_cleaned_log(log_lines)

    query = openai_api.ChatCompletionQuery(
        role=EXTRACT_DATA_QUERY_TEMPLATE.role,
        content=f"{EXTRACT_DATA_QUERY_TEMPLATE.content}{cleaned_log}",
        answer_size=EXTRACT_DATA_QUERY_TEMPLATE.answer_size,
        seed=EXTRACT_DATA_QUERY_TEMPLATE.seed,
        temperature=EXTRACT_DATA_QUERY_TEMPLATE.temperature,
        response_format=EXTRACT_DATA_QUERY_TEMPLATE.response_format,
    )

    chat_completion = await openai_client.get_chat_completion(query)
    choice = chat_completion["choices"][0]
    chat_response = choice.get("message", {}).get("content")

    if not chat_response:
        # FIXME(sileht): We should mark the job as ERROR instead of retrying
        raise UnexpectedLogEmbedderError(
            "ChatGPT returned no extracted data for the job log",
            log_extras={"chat_completion": chat_completion},
        )

    try:
        extracted_data: list[ExtractedDataObject] = json.loads(chat_response)[
            "failures"
        ]
    except json.JSONDecodeError:
        if choice["finish_reason"] == openai_api.ChatCompletionFinishReason.length:
            # FIXME(Kontrolix): It means that GPT responde with a too long response,
            # for now I have no better solution than push the error under the carpet.
            # But we will have to improve the prompt or the cleaner or the model we use ...
            # FIXME(sileht): We should mark the job as ERROR, not DOWNLOADED in such case
            extracted_data = []
        else:
            raise

    # FIXME(Kontrolix): It means that GPT found no error, for now I have no better
    # solution than push the error under the carpet. But we will have to improve
    # the prompt or the cleaner or the model we use ...
    # FIXME(sileht): We should mark the job as ERROR, not DOWNLOADED in such case
    if extracted_data is None or extracted_data == [None]:
        extracted_data = []

    for data in extracted_data:
        log_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job.id,
            **data,
        )
        session.add(log_metadata)
        job.log_metadata.append(log_metadata)

    job.log_metadata_extracting_status = (
        gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED
    )


def get_cleaned_log(log_lines: list[str]) -> str:
    cleaner = log_cleaner.LogCleaner()

    cleaned_lines: list[str] = []

    total_tokens = 0
    for line in reversed(log_lines):
        if not line:
            continue

        cleaned_line = cleaner.gpt_clean_line(line)

        if not cleaned_line:
            continue

        tokenized_cleaned_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)

        total_tokens += len(tokenized_cleaned_line)

        if total_tokens <= MAX_CHAT_COMPLETION_TOKENS:
            cleaned_lines.insert(0, cleaned_line)

        # NOTE(Kontrolix): Add 1 for the `\n` that will links lines
        total_tokens += 1

        if total_tokens >= MAX_CHAT_COMPLETION_TOKENS:
            break

    return "\n".join(cleaned_lines)


@dataclasses.dataclass
class Retry:
    at: datetime.datetime


def log_exception_and_maybe_retry(
    exc: Exception,
    attempts: int,
    log_extras: dict[str, typing.Any],
) -> Retry | None:
    if isinstance(exc, UnexpectedLogEmbedderError):
        log_extras.update(exc.log_extras)

    if exceptions.should_be_ignored(exc):
        LOG.warning(
            "log-embedder: failed with a fatal error",
            exc_info=True,
            **log_extras,
        )
        return None

    if attempts >= LOG_EMBEDDER_MAX_ATTEMPTS:
        LOG.error(
            "log-embedder: too many unexpected failures, giving up",
            exc_info=True,
            **log_extras,
        )
        return None

    if (
        isinstance(exc, http.RequestError)
        and exc.request.url == openai_api.OPENAI_API_BASE_URL
    ):
        # NOTE(sileht): This cost money so we don't want to retry too often
        base_retry_min = 10
    else:
        base_retry_min = 1

    retry_in = exceptions.need_retry_in(exc, base_retry_min=base_retry_min)
    if retry_in is None:
        # TODO(sileht): Maybe replace me by a exponential backoff + 1 hours limit
        retry_in = datetime.timedelta(minutes=10)

    retry_at = date.utcnow() + retry_in

    # We don't want to log anything related to network and HTTP server side error
    if not isinstance(exc, http.RequestError | http.HTTPServerSideError):
        LOG.error(
            "log-embedder: unexpected failure, retrying later",
            exc_info=True,
            **log_extras,
        )

    return Retry(at=retry_at)


async def embed_logs_with_log_embedding(
    redis_links: redis_utils.RedisLinks,
    gcs_client: google_cloud_storage.GoogleCloudStorageClient,
) -> bool:
    wjob = orm.aliased(gh_models.WorkflowJob, name="wjob")

    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(wjob)
            .options(orm.joinedload(wjob.ci_issue))
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
                wjob.log_embedding_status
                != gh_models.WorkflowJobLogEmbeddingStatus.ERROR,
                wjob.log_status.notin_(
                    (
                        gh_models.WorkflowJobLogStatus.GONE,
                        gh_models.WorkflowJobLogStatus.ERROR,
                    ),
                ),
                sqlalchemy.or_(
                    wjob.log_embedding.is_(None),
                    wjob.ci_issue_id.is_(None),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )

        jobs = (await session.scalars(stmt)).all()

        LOG.info("log-embedder: %d jobs to embed", len(jobs))

        async with openai_api.OpenAIClient() as openai_client:
            refresh_ready_job_ids = []
            for job in jobs:
                try:
                    log = await get_log(gcs_client, job)
                except gh_models.WorkflowJob.UnableToRetrieveLog:
                    job.log_status = gh_models.WorkflowJobLogStatus.GONE
                    await session.commit()
                    continue
                except Exception as e:
                    retry = log_exception_and_maybe_retry(
                        e,
                        job.log_downloading_attempts + 1,
                        job.as_log_extras(),
                    )
                    if retry is None:
                        job.log_status = gh_models.WorkflowJobLogStatus.ERROR
                    else:
                        job.log_downloading_attempts += 1
                        job.log_downloading_retry_after = retry.at
                    await session.commit()
                    continue

                if (
                    job.log_embedding_status
                    == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN
                ):
                    try:
                        await embed_log(openai_client, job, log)
                    except Exception as e:
                        retry = log_exception_and_maybe_retry(
                            e,
                            job.log_embedding_attempts + 1,
                            job.as_log_extras(),
                        )
                        if retry is None:
                            job.log_embedding_status = (
                                gh_models.WorkflowJobLogEmbeddingStatus.ERROR
                            )
                        else:
                            job.log_embedding_attempts += 1
                            job.log_embedding_retry_after = retry.at
                        await session.commit()
                        continue

                if job.ci_issue_id is None:
                    await ci_issue.CiIssue.link_job_to_ci_issue(session, job)
                    refresh_ready_job_ids.append(job.id)

                await session.commit()

        await flaky_check.send_pull_refresh_for_jobs(redis_links, refresh_ready_job_ids)

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


async def embed_logs_with_extracted_metadata(
    redis_links: redis_utils.RedisLinks,
    gcs_client: google_cloud_storage.GoogleCloudStorageClient,
) -> bool:
    wjob = orm.aliased(gh_models.WorkflowJob, name="wjob")

    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(wjob)
            .options(
                orm.joinedload(wjob.ci_issues_gpt),
                orm.joinedload(wjob.log_metadata),
            )
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
                    wjob.log_metadata_extracting_retry_after.is_(None),
                    wjob.log_metadata_extracting_retry_after <= date.utcnow(),
                ),
                wjob.failed_step_number.is_not(None),
                wjob.log_metadata_extracting_status
                != gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR,
                wjob.log_status.notin_(
                    (
                        gh_models.WorkflowJobLogStatus.GONE,
                        gh_models.WorkflowJobLogStatus.ERROR,
                    ),
                ),
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        ~wjob.log_metadata.any(),
                        wjob.log_metadata_extracting_status
                        != gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
                    ),
                    sqlalchemy.and_(
                        ~wjob.ci_issues_gpt.any(),
                        wjob.log_metadata_extracting_status
                        == gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
                    ),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )

        jobs = (await session.scalars(stmt)).unique().all()

        LOG.info(
            "log-embedder: %d jobs to extract metadata",
            len(jobs),
        )

        async with openai_api.OpenAIClient() as openai_client:
            for job in jobs:
                try:
                    log = await get_log(gcs_client, job)
                except gh_models.WorkflowJob.UnableToRetrieveLog:
                    job.log_status = gh_models.WorkflowJobLogStatus.GONE
                    await session.commit()
                    continue
                except Exception as e:
                    retry = log_exception_and_maybe_retry(
                        e,
                        job.log_downloading_attempts + 1,
                        job.as_log_extras(),
                    )
                    if retry is None:
                        job.log_status = gh_models.WorkflowJobLogStatus.ERROR
                    else:
                        job.log_downloading_attempts += 1
                        job.log_downloading_retry_after = retry.at
                    await session.commit()
                    continue

                if job.log_metadata_extracting_status == (
                    gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN
                ):
                    try:
                        await extract_data_from_log(
                            openai_client,
                            session,
                            job,
                            log.lines,
                        )
                    except Exception as e:
                        retry = log_exception_and_maybe_retry(
                            e,
                            job.log_metadata_extracting_attempts + 1,
                            job.as_log_extras(),
                        )
                        if retry is None:
                            job.log_metadata_extracting_status = (
                                gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR
                            )
                        else:
                            job.log_metadata_extracting_attempts += 1
                            job.log_metadata_extracting_retry_after = retry.at
                        await session.commit()
                        continue

                # FIXME(sileht): it does not make sense to check log_metadata is empty when
                # the status is DOWNLOADED... but now we have the DB with not log_metadata
                # but with status set as DOWNLOADED...
                if job.log_metadata and not job.ci_issues_gpt:
                    await ci_issue.CiIssueGPT.link_job_to_ci_issues(session, job)

                await session.commit()

        # FIXME(Kontrolix): For now, flacky_check is link to ci_issue when it will be link
        # to ci_issue_gpt adapte this method. I keep it here to not forget.
        # await flaky_check.send_pull_refresh_for_jobs(redis_links, refresh_ready_job_ids)

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


@tracer.wrap("embed-logs")
async def embed_logs(redis_links: redis_utils.RedisLinks) -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    gcs_client = google_cloud_storage.GoogleCloudStorageClient(
        settings.LOG_EMBEDDER_GCS_CREDENTIALS,
    )

    return any(
        (
            await embed_logs_with_extracted_metadata(
                redis_links,
                gcs_client,
            ),
            await embed_logs_with_log_embedding(
                redis_links,
                gcs_client,
            ),
        ),
    )
