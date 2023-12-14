import dataclasses
import datetime
import io
import json
import re
import typing
import zipfile

import daiquiri
from ddtrace import tracer
import httpx
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
LOG_EMBEDDER_MAX_ATTEMPTS = 5

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


CHAT_COMPLETION_MODEL = openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]


MAX_CHAT_COMPLETION_TOKENS = (
    CHAT_COMPLETION_MODEL["max_tokens"] - EXTRACT_DATA_QUERY_TEMPLATE.get_tokens_size()
)


@dataclasses.dataclass
class UnexpectedLogEmbedderError(Exception):
    message: str
    log_extras: dict[str, typing.Any] = dataclasses.field(default_factory=dict)


class UnableToExtractLogMetadata(Exception):
    pass


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
    job.log_extract = log.extract(
        openai_api.BYTES_PER_TOKEN_APPROX * CHAT_COMPLETION_MODEL["max_tokens"],
    )

    return log


async def get_log(
    gcs_client: google_cloud_storage.GoogleCloudStorageClient,
    job: gh_models.WorkflowJob,
) -> logm.Log:
    if job.log_status == gh_models.WorkflowJobLogStatus.DOWNLOADED:
        if job.log_extract is None:
            LOG.error(
                "Downloaded log is missing from database, re-downloading",
                **job.as_log_extras(),
            )
            return await fetch_and_store_log(gcs_client, job)

        return logm.Log.from_content(job.log_extract)

    return await fetch_and_store_log(gcs_client, job)


async def embed_log(
    openai_client: openai_api.OpenAIClient,
    job: gh_models.WorkflowJob,
    log: logm.Log,
) -> None:
    tokens = await get_tokenized_cleaned_log(log)

    embedding = await openai_client.get_embedding(tokens)

    job.log_embedding = embedding
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
) -> list[int]:
    cleaner = log_cleaner.LogCleaner()

    cleaned_tokens: list[int] = []

    for line in reversed(log.lines):
        if not line:
            continue

        cleaned_line = cleaner.clean_line(line, log.tags)

        if not cleaned_line:
            continue

        tokenized_cleaned_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)

        total_tokens = len(tokenized_cleaned_line) + len(cleaned_tokens)

        if total_tokens <= openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN:
            cleaned_tokens = tokenized_cleaned_line + cleaned_tokens

        if total_tokens >= openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN:
            break

    return cleaned_tokens


async def extract_data_from_log(
    openai_client: openai_api.OpenAIClient,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: gh_models.WorkflowJob,
    log: logm.Log,
) -> None:
    cleaned_log = get_cleaned_log(log)
    query = openai_api.ChatCompletionQuery(
        role=EXTRACT_DATA_QUERY_TEMPLATE.role,
        content=f"{EXTRACT_DATA_QUERY_TEMPLATE.content}{cleaned_log}",
        answer_size=EXTRACT_DATA_QUERY_TEMPLATE.answer_size,
        seed=EXTRACT_DATA_QUERY_TEMPLATE.seed,
        temperature=EXTRACT_DATA_QUERY_TEMPLATE.temperature,
        response_format=EXTRACT_DATA_QUERY_TEMPLATE.response_format,
    )

    try:
        chat_completion = await openai_client.get_chat_completion(query)
    except http.HTTPClientSideError as err:
        # NOTE(Kontrolix): Do not retry when OpenAI said that there is an error in
        # the prompt, the error will still be there next time.
        if (
            err.response.status_code == 400
            and "Detected an error in the prompt" in err.response.content.decode()
        ):
            raise UnableToExtractLogMetadata

        raise

    choice = chat_completion["choices"][0]
    if choice["finish_reason"] != "stop":
        # FIXME(Kontrolix): It means that GPT reaches a limit.
        # for now I have no better solution than push the error under the carpet.
        # But we will have to improve the prompt or the cleaner or the model we use ...
        raise UnableToExtractLogMetadata

    chat_response = choice["message"]["content"]
    if not chat_response:
        # FIXME(sileht): We should mark the job as ERROR instead of retrying
        LOG.warning(
            "ChatGPT returned no extracted data for the job log",
            chat_completion=chat_completion,
            **job.as_log_extras(),
        )
        raise UnableToExtractLogMetadata

    extracted_data: list[ExtractedDataObject] = json.loads(chat_response)["failures"]

    # FIXME(Kontrolix): It means that GPT found no error, for now I have no better
    # solution than push the error under the carpet. But we will have to improve
    # the prompt or the cleaner or the model we use ...
    if not extracted_data or extracted_data == [None]:
        LOG.warning(
            "ChatGPT returned no extracted data for the job log",
            chat_completion=chat_completion,
            **job.as_log_extras(),
        )
        raise UnableToExtractLogMetadata

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


def get_cleaned_log(log: logm.Log) -> str:
    cleaned_lines: list[str] = []

    total_tokens = 0
    for cleaned_line in log.iter_gpt_cleaned_log_lines_reverse():
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
    job: gh_models.WorkflowJob,
) -> bool:
    log_extras = job.as_log_extras()
    if isinstance(exc, UnexpectedLogEmbedderError):
        log_extras.update(exc.log_extras)

    if exceptions.should_be_ignored(exc):
        LOG.warning(
            "log-embedder: failed with a fatal error",
            exc_info=True,
            **log_extras,
        )
        return False

    if job.log_processing_attempts + 1 >= LOG_EMBEDDER_MAX_ATTEMPTS:
        LOG.error(
            "log-embedder: too many unexpected failures, giving up",
            exc_info=True,
            **log_extras,
        )
        return False

    if (
        isinstance(exc, httpx.HTTPError)
        and exc.request is not None
        and str(exc.request.url).startswith(openai_api.OPENAI_API_BASE_URL)
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

    job.log_processing_attempts += 1
    job.log_processing_retry_after = retry_at
    return True


async def try_commit_or_rollback(
    session: sqlalchemy.ext.asyncio.AsyncSessionTransaction
    | sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    try:
        await session.commit()
    except sqlalchemy.exc.DatabaseError:
        await session.rollback()
        raise


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
                    wjob.log_processing_retry_after.is_(None),
                    wjob.log_processing_retry_after <= date.utcnow(),
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
                    wjob.log_embedding_status
                    == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
                    wjob.log_status == gh_models.WorkflowJobLogStatus.UNKNOWN,
                    wjob.ci_issue_id.is_(None),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )

        jobs = (await session.scalars(stmt)).all()
        # We detach jobs from the session to avoid these instances to be updated
        session.expunge_all()

        LOG.info("log-embedder: %d jobs to embed", len(jobs))

        async with openai_api.OpenAIClient() as openai_client:
            refresh_ready_job_ids = []
            for job in jobs:
                job_for_update = await session.merge(job, load=False)
                try:
                    log = await get_log(gcs_client, job_for_update)
                    await try_commit_or_rollback(session)
                except gh_models.WorkflowJob.UnableToRetrieveLog:
                    session.expunge(job_for_update)
                    job_for_error = await session.merge(job, load=False)
                    job_for_error.log_status = gh_models.WorkflowJobLogStatus.GONE
                    await try_commit_or_rollback(session)
                    continue
                except Exception as e:
                    session.expunge(job_for_update)
                    job_for_error = await session.merge(job, load=False)
                    retry = log_exception_and_maybe_retry(e, job_for_error)
                    if not retry:
                        job_for_error.log_status = gh_models.WorkflowJobLogStatus.ERROR
                    await try_commit_or_rollback(session)
                    continue

                if (
                    job_for_update.log_embedding_status
                    == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN
                ):
                    try:
                        await embed_log(openai_client, job_for_update, log)
                        await try_commit_or_rollback(session)
                    except Exception as e:
                        session.expunge(job_for_update)
                        job_for_error = await session.merge(job, load=False)
                        retry = log_exception_and_maybe_retry(e, job_for_error)
                        if not retry:
                            job_for_error.log_embedding_status = (
                                gh_models.WorkflowJobLogEmbeddingStatus.ERROR
                            )
                        await try_commit_or_rollback(session)
                        continue

                if job_for_update.ci_issue_id is None:
                    await ci_issue.CiIssue.link_job_to_ci_issue(session, job_for_update)
                    await try_commit_or_rollback(session)
                    refresh_ready_job_ids.append(job_for_update.id)

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
                wjob.failed_step_number.is_not(None),
                sqlalchemy.or_(
                    wjob.log_processing_retry_after.is_(None),
                    wjob.log_processing_retry_after <= date.utcnow(),
                ),
                wjob.log_status.notin_(
                    (
                        gh_models.WorkflowJobLogStatus.GONE,
                        gh_models.WorkflowJobLogStatus.ERROR,
                    ),
                ),
                wjob.log_metadata_extracting_status
                != gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR,
                sqlalchemy.or_(
                    wjob.log_status == gh_models.WorkflowJobLogStatus.UNKNOWN,
                    wjob.log_metadata_extracting_status
                    == gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
                    # FIXME(sileht): should it be status like other?
                    ~wjob.ci_issues_gpt.any(),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )

        jobs = (await session.scalars(stmt)).unique().all()
        # We detach jobs from the session to avoid these instances to be updated
        session.expunge_all()

        LOG.info(
            "log-embedder: %d jobs to extract metadata",
            len(jobs),
        )

        async with openai_api.OpenAIClient() as openai_client:
            for job in jobs:
                job_for_update = await session.merge(job, load=False)
                try:
                    log = await get_log(gcs_client, job_for_update)
                    await try_commit_or_rollback(session)
                except gh_models.WorkflowJob.UnableToRetrieveLog:
                    session.expunge(job_for_update)
                    job_for_error = await session.merge(job, load=False)
                    job_for_error.log_status = gh_models.WorkflowJobLogStatus.GONE
                    await try_commit_or_rollback(session)
                    continue
                except Exception as e:
                    session.expunge(job_for_update)
                    job_for_error = await session.merge(job, load=False)
                    retry = log_exception_and_maybe_retry(e, job_for_error)
                    if not retry:
                        job_for_error.log_status = gh_models.WorkflowJobLogStatus.ERROR
                    await try_commit_or_rollback(session)
                    continue

                if job_for_update.log_metadata_extracting_status == (
                    gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN
                ):
                    try:
                        await extract_data_from_log(
                            openai_client,
                            session,
                            job_for_update,
                            log,
                        )
                        await try_commit_or_rollback(session)
                    except UnableToExtractLogMetadata:
                        session.expunge(job_for_update)
                        job_for_error = await session.merge(job, load=False)
                        job_for_error.log_metadata_extracting_status = (
                            gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR
                        )
                        await try_commit_or_rollback(session)
                        continue
                    except Exception as e:
                        session.expunge(job_for_update)
                        job_for_error = await session.merge(job, load=False)
                        retry = log_exception_and_maybe_retry(e, job_for_error)
                        if not retry:
                            job_for_error.log_metadata_extracting_status = (
                                gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR
                            )
                        await try_commit_or_rollback(session)
                        continue

                if not job_for_update.ci_issues_gpt:
                    await ci_issue.CiIssueGPT.link_job_to_ci_issues(
                        session,
                        job_for_update,
                    )
                    await try_commit_or_rollback(session)

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
