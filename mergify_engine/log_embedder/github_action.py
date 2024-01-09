import copy
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
    "problem_type": "The type of problem that caused the failure",
    "test_name": "Name of the test that failed if any, null otherwise",
    "language": "The programming language of the program that produced the error",
    "filename": "The filename where the error was raised, null if no file is detected",
    "lineno": "The line number where the error was raised as an integer, null otherwise",
    "error": "The precise error type, null otherwise",
    "test_framework": "The framework that was running when the error was raised if any, null otherwise",
    "stack_trace": "The stack trace of the error if any, null otherwise",
}

_EXTRACT_DATA_QUERY_TEMPLATE = openai_api.ChatCompletion(
    model=settings.LOG_EMBEDDER_METADATA_EXTRACT_MODEL,
    messages=[
        openai_api.ChatCompletionMessage(
            role="system",
            content=f"""You are a CI software engineer. Analyze the program logs provided as input and identify the root causes of the job failure.
Your response must be an array of JSON objects matching the following requirements:
- If you find only one failure, add only one object to the array.
- If you identify multiple source of failure, add multiple objects in the array.
- If you don't find the requested information, don't speculate, return an empty array.

JSON response structure: {{"failures": [{{json object}}]}}
JSON object structure: {json.dumps(JSON_STRUCTURE)}""",
        ),
    ],
    seed=1,
    temperature=0,
    response_format={"type": "json_object"},
)

# https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
# Token limit is shared between prompt and completion so we need to take that
# into account and reserve a few tokens for the answer.
MINIMUM_ANSWER_RESERVED_TOKEN = 500

MAX_LOGS_TOKENS = (
    openai_api.OPENAI_CHAT_COMPLETION_MODEL_MAX_TOKENS
    - openai_api.get_chat_completion_token_size(_EXTRACT_DATA_QUERY_TEMPLATE)
    - MINIMUM_ANSWER_RESERVED_TOKEN
)


def get_completion_query(log_content: str) -> openai_api.ChatCompletion:
    query = copy.deepcopy(_EXTRACT_DATA_QUERY_TEMPLATE)
    query["messages"].append(
        openai_api.ChatCompletionMessage(role="user", content=log_content),
    )
    return query


@dataclasses.dataclass
class LogNotFoundInZipFileError(Exception):
    message: str
    log_extras: dict[str, typing.Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class UnableToExtractLogMetadataError(Exception):
    chat_completion_result: openai_api.ChatCompletionResponse | None


class ExtractedDataObject(typing.TypedDict):
    problem_type: str | None
    test_name: str | None
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
        openai_api.BYTES_PER_TOKEN_APPROX
        * openai_api.OPENAI_CHAT_COMPLETION_MODEL_MAX_TOKENS,
    )

    return log


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
    searching_pattern = f"{cleaned_job_name}/{failed_step_number}_"
    with io.BytesIO() as zip_data:
        zip_data.write(zipped_log_content)

        with zipfile.ZipFile(zip_data, "r") as zip_file:
            for i in zip_file.infolist():
                if not i.filename.startswith(searching_pattern):
                    continue

                with zip_file.open(i.filename) as log_file:
                    return log_file.read()

    raise LogNotFoundInZipFileError(
        "log-embedder: job log not found in zip file",
        log_extras={
            "files": [i.filename for i in zip_file.infolist()],
            "searching_pattern": searching_pattern,
        },
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


async def create_job_log_metadata(
    openai_client: openai_api.OpenAIClient,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: gh_models.WorkflowJob,
    log: logm.Log,
) -> None:
    try:
        extracted_data = await extract_data_from_log(openai_client, log)
    except UnableToExtractLogMetadataError as err:
        LOG.warning(
            "Unable to extract data for the job log",
            chat_completion=err.chat_completion_result,
            **job.as_log_extras(),
        )
        raise

    for data in extracted_data:
        # NOTE(Kontrolix): Convert all data returned by GPT in str as we expect them to be
        # sometimes `lineno` is returned as int. It would be preferable to have `lineno`
        # column as an int in db but sometime GPT return str for `lineno`
        # like `line 51` or `41 and 45`
        stringified_data = {
            key: None if value is None else str(value) for key, value in data.items()
        }

        log_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job.id,
            **stringified_data,
        )
        session.add(log_metadata)
        job.log_metadata.append(log_metadata)

    job.log_metadata_extracting_status = (
        gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED
    )


async def extract_data_from_log(
    openai_client: openai_api.OpenAIClient,
    log: logm.Log,
) -> list[ExtractedDataObject]:
    cleaned_log = get_cleaned_log(log)
    query = get_completion_query(cleaned_log)

    try:
        chat_completion = await openai_client.get_chat_completion(query)
    except http.HTTPClientSideError as err:
        # NOTE(Kontrolix): Do not retry when OpenAI said that there is an error in
        # the prompt, the error will still be there next time.
        if (
            err.response.status_code == 400
            and "Detected an error in the prompt" in err.response.content.decode()
        ):
            raise UnableToExtractLogMetadataError(None)

        raise

    choice = chat_completion["choices"][0]
    if choice["finish_reason"] != "stop":
        # FIXME(Kontrolix): It means that GPT reaches a limit.
        # for now I have no better solution than push the error under the carpet.
        # But we will have to improve the prompt or the cleaner or the model we use ...
        raise UnableToExtractLogMetadataError(chat_completion)

    chat_response = choice["message"]["content"]
    if not chat_response:
        raise UnableToExtractLogMetadataError(chat_completion)

    extracted_data: list[ExtractedDataObject] = json.loads(chat_response)["failures"]

    # FIXME(Kontrolix): It means that GPT found no error, for now I have no better
    # solution than push the error under the carpet. But we will have to improve
    # the prompt or the cleaner or the model we use ...
    if not extracted_data or extracted_data == [None]:
        raise UnableToExtractLogMetadataError(chat_completion)

    return extracted_data


def get_cleaned_log(log: logm.Log) -> str:
    cleaned_lines: list[str] = []

    total_tokens = 0
    for cleaned_line in log.iter_gpt_cleaned_log_lines_reverse():
        if not cleaned_line:
            continue

        tokenized_cleaned_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)

        total_tokens += len(tokenized_cleaned_line)

        if total_tokens <= MAX_LOGS_TOKENS:
            cleaned_lines.insert(0, cleaned_line)

        # NOTE(Kontrolix): Add 1 for the `\n` that will links lines
        total_tokens += 1

        if total_tokens >= MAX_LOGS_TOKENS:
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
    if isinstance(exc, LogNotFoundInZipFileError):
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

    # We don't want to log anything related to:
    #     - network and HTTP server side error
    #     - LogNotFoundInZipFileError
    if not isinstance(
        exc,
        http.RequestError | http.HTTPServerSideError | LogNotFoundInZipFileError,
    ):
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


def get_job_processing_base_query() -> sqlalchemy.Select[tuple[gh_models.WorkflowJob]]:
    return (
        sqlalchemy.select(gh_models.WorkflowJob)
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
            gh_models.WorkflowJob.conclusion == gh_models.WorkflowJobConclusion.FAILURE,
            sqlalchemy.or_(
                gh_models.WorkflowJob.log_processing_retry_after.is_(None),
                gh_models.WorkflowJob.log_processing_retry_after <= date.utcnow(),
            ),
        )
        .order_by(gh_models.WorkflowJob.completed_at.asc())
        .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
    )


async def get_logs_embedding() -> bool:
    async with database.create_session() as session:
        stmt = (
            get_job_processing_base_query()
            .options(
                orm.joinedload(gh_models.WorkflowJob.ci_issue),
            )
            .where(
                gh_models.WorkflowJob.log_status
                == gh_models.WorkflowJobLogStatus.DOWNLOADED,
                gh_models.WorkflowJob.log_embedding_status
                == gh_models.WorkflowJobLogEmbeddingStatus.UNKNOWN,
            )
        )

        jobs = (await session.scalars(stmt)).all()
        # We detach jobs from the session to prevent these instances from being updated.
        # Therefore we can have a reservoir of jobs at their initial state to be able
        # to handle DatabaseError
        session.expunge_all()

        LOG.info("log-embedder: %d jobs to embed", len(jobs))

        async with openai_api.OpenAIClient() as openai_client:
            for job in jobs:
                job_for_update = await session.merge(job, load=False)

                if job_for_update.log_extract is None:
                    LOG.error(
                        "log-embedder: Job log_status is DOWNLOADED but log-extract is `None`",
                        exc_info=True,
                        **job.as_log_extras(),
                    )
                    job_for_update.log_embedding_status = (
                        gh_models.WorkflowJobLogEmbeddingStatus.ERROR
                    )
                    await try_commit_or_rollback(session)
                    continue

                try:
                    await embed_log(
                        openai_client,
                        job_for_update,
                        logm.Log.from_content(job_for_update.log_extract),
                    )
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

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


async def link_jobs_to_ci_issue(redis_links: redis_utils.RedisLinks) -> bool:
    async with database.create_session() as session:
        stmt = (
            get_job_processing_base_query()
            .options(
                orm.joinedload(gh_models.WorkflowJob.ci_issue),
            )
            .where(
                gh_models.WorkflowJob.log_embedding_status
                == gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                # FIXME(sileht/Kontrolix): should it be status like other?
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            )
        )

        jobs = (await session.scalars(stmt)).all()

        LOG.info("log-embedder: %d jobs to link to ci_issue", len(jobs))

        refresh_ready_job_ids = []
        for job in jobs:
            await ci_issue.CiIssue.link_job_to_ci_issue(session, job)
            await try_commit_or_rollback(session)
            refresh_ready_job_ids.append(job.id)

        await flaky_check.send_pull_refresh_for_jobs(redis_links, refresh_ready_job_ids)

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


async def extract_metadata_from_logs() -> bool:
    async with database.create_session() as session:
        stmt = (
            get_job_processing_base_query()
            .options(
                orm.joinedload(gh_models.WorkflowJob.log_metadata),
            )
            .where(
                gh_models.WorkflowJob.log_status
                == gh_models.WorkflowJobLogStatus.DOWNLOADED,
                gh_models.WorkflowJob.log_metadata_extracting_status
                == gh_models.WorkflowJobLogMetadataExtractingStatus.UNKNOWN,
            )
        )

        jobs = (await session.scalars(stmt)).unique().all()
        # We detach jobs from the session to prevent these instances from being updated.
        # Therefore we can have a reservoir of jobs at their initial state to be able
        # to handle DatabaseError
        session.expunge_all()

        LOG.info(
            "log-embedder: %d jobs to extract metadata",
            len(jobs),
        )

        async with openai_api.OpenAIClient() as openai_client:
            for job in jobs:
                job_for_update = await session.merge(job, load=False)

                if job_for_update.log_extract is None:
                    LOG.error(
                        "log-embedder: Job log_status is DOWNLOADED but log-extract is `None`",
                        exc_info=True,
                        **job.as_log_extras(),
                    )
                    job_for_update.log_metadata_extracting_status = (
                        gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR
                    )
                    await try_commit_or_rollback(session)
                    continue

                try:
                    try:
                        await create_job_log_metadata(
                            openai_client,
                            session,
                            job_for_update,
                            logm.Log.from_content(job_for_update.log_extract),
                        )
                    except UnableToExtractLogMetadataError:
                        session.expunge(job_for_update)
                        job_for_error = await session.merge(job, load=False)
                        job_for_error.log_metadata_extracting_status = (
                            gh_models.WorkflowJobLogMetadataExtractingStatus.ERROR
                        )
                        await try_commit_or_rollback(session)
                        continue
                    await try_commit_or_rollback(session)
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

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


async def link_jobs_to_ci_issue_gpt() -> bool:
    async with database.create_session() as session:
        stmt = (
            get_job_processing_base_query()
            .options(
                orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
                orm.joinedload(gh_models.WorkflowJob.log_metadata),
            )
            .where(
                gh_models.WorkflowJob.log_metadata_extracting_status
                == gh_models.WorkflowJobLogMetadataExtractingStatus.EXTRACTED,
                # FIXME(sileht): should it be status like other?
                ~gh_models.WorkflowJob.ci_issues_gpt.any(),
            )
        )

        jobs = (await session.scalars(stmt)).unique().all()

        LOG.info(
            "log-embedder: %d jobs to link to ci_issue_gpt",
            len(jobs),
        )

        for job in jobs:
            await ci_issue.CiIssueGPT.link_job_to_ci_issues(
                session,
                job,
            )
            await try_commit_or_rollback(session)

        # FIXME(Kontrolix): For now, flacky_check is link to ci_issue when it will be link
        # to ci_issue_gpt adapte this method. I keep it here to not forget.
        # await flaky_check.send_pull_refresh_for_jobs(redis_links, refresh_ready_job_ids)

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


async def download_logs_for_failed_jobs() -> bool:
    async with database.create_session() as session:
        stmt = get_job_processing_base_query().where(
            gh_models.WorkflowJob.log_status == gh_models.WorkflowJobLogStatus.UNKNOWN,
        )

        jobs = (await session.scalars(stmt)).unique().all()
        # We detach jobs from the session to prevent these instances from being updated.
        # Therefore we can have a reservoir of jobs at their initial state to be able
        # to handle DatabaseError
        session.expunge_all()

        LOG.info(
            "log-embedder: %d jobs to downloads logs",
            len(jobs),
        )

        if not jobs:
            return False

        gcs_client = google_cloud_storage.GoogleCloudStorageClient(
            settings.LOG_EMBEDDER_GCS_CREDENTIALS,
        )

        for job in jobs:
            job_for_update = await session.merge(job, load=False)
            try:
                try:
                    await fetch_and_store_log(gcs_client, job_for_update)
                except gh_models.WorkflowJob.UnableToRetrieveLogError:
                    session.expunge(job_for_update)
                    job_for_error = await session.merge(job, load=False)
                    job_for_error.log_status = gh_models.WorkflowJobLogStatus.GONE
                await try_commit_or_rollback(session)
            except Exception as e:
                session.expunge(job_for_update)
                job_for_error = await session.merge(job, load=False)
                retry = log_exception_and_maybe_retry(e, job_for_error)
                if not retry:
                    job_for_error.log_status = gh_models.WorkflowJobLogStatus.ERROR
                await try_commit_or_rollback(session)
                continue

        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0


@tracer.wrap("process-failed-jobs")
async def process_failed_jobs(redis_links: redis_utils.RedisLinks) -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    return any(
        (
            await download_logs_for_failed_jobs(),
            await extract_metadata_from_logs(),
            await link_jobs_to_ci_issue_gpt(),
            await get_logs_embedding(),
            await link_jobs_to_ci_issue(redis_links),
        ),
    )
