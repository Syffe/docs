import dataclasses
import io
import re
import zipfile

import daiquiri
from ddtrace import tracer
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)

# NOTE(Kontrolix): We remove all the windows forbidden path character as github does
WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP = re.compile(r"[\*\"/\\<>:|\?]")
LOG_EMBEDDER_JOBS_BATCH_SIZE = 100
LOG_EMBEDDER_MAX_ATTEMPTS = 15


ERROR_TITLE_QUERY_TEMPLATE = openai_api.ChatCompletionQuery(
    "user",
    """Analyze the following logs, spot the error and give me a
meaningful title to qualify this error. Your answer must contain only the title. The logs:
    """,
    100,
)


@dataclasses.dataclass
class NoLogFileFoundInZip(Exception):
    message: str
    files: list[str] = dataclasses.field(default_factory=list)


async def embed_log(
    openai_client: openai_api.OpenAIClient, job: github_actions.WorkflowJob
) -> None:
    try:
        log_lines = await download_failed_step_log(job)

    except NoLogFileFoundInZip as e:
        LOG.error(e.message, files=e.files, **job.as_log_extras())
        return

    except http.HTTPStatusError as e:
        if e.response.status_code == 410:
            job.log_status = github_actions.WorkflowJobLogStatus.GONE
            return

        if exceptions.should_be_ignored(e):
            job.log_status = github_actions.WorkflowJobLogStatus.ERROR
            LOG.warning(
                "log-embedder: downloading job log failed with a fatal error",
                exc_info=True,
                **job.as_log_extras(),
            )

            return

        retry_in = exceptions.need_retry(e)
        if retry_in is not None:
            job.log_embedding_retry_after = date.utcnow() + retry_in
            job.log_embedding_attempts += 1
            if job.log_embedding_attempts >= LOG_EMBEDDER_MAX_ATTEMPTS:
                job.log_status = github_actions.WorkflowJobLogStatus.ERROR
                LOG.warning(
                    "log-embedder: downloading job log failed too many times",
                    exc_info=True,
                    **job.as_log_extras(),
                )
            else:
                LOG.warning(
                    "log-embedder: downloading job log failed, retrying later",
                    exc_info=True,
                    **job.as_log_extras(),
                )

            return

        raise

    tokens, truncated_log = await get_tokenized_cleaned_log(log_lines)

    embedding = await openai_client.get_embedding(tokens)

    job.log_embedding = embedding
    job.embedded_log = truncated_log
    job.log_status = github_actions.WorkflowJobLogStatus.EMBEDDED


def get_lines_from_zip(
    zip_file: zipfile.ZipFile, job: github_actions.WorkflowJob
) -> list[str]:
    if job.failed_step_number is None:
        raise RuntimeError(
            "get_lines_from_zip() called on a job without failed_step_number"
        )

    cleaned_job_name = WORKFLOW_JOB_NAME_INVALID_CHARS_REGEXP.sub("", job.name)

    for i in zip_file.infolist():
        if not i.filename.startswith(f"{cleaned_job_name}/{job.failed_step_number}_"):
            continue

        with zip_file.open(i.filename) as log_file:
            return [line.decode() for line in log_file.readlines()]

    raise NoLogFileFoundInZip(
        "log-embedder: job log not found in zip file",
        files=[i.filename for i in zip_file.infolist()],
    )


async def download_failed_step_log(job: github_actions.WorkflowJob) -> list[str]:
    repo = job.repository

    installation_json = await github.get_installation_from_login(repo.owner.login)

    with io.BytesIO() as zip_data:
        async with github.aget_client(installation_json) as client:
            resp = await client.get(
                f"/repos/{repo.owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs"
            )
            zip_data.write(resp.content)

        with zipfile.ZipFile(zip_data, "r") as zip_file:
            return get_lines_from_zip(zip_file, job)


async def get_tokenized_cleaned_log(
    log_lines: list[str],
) -> tuple[list[int], str]:
    cleaner = log_cleaner.LogCleaner()

    max_embedding_tokens = openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    max_chat_completion_tokens = (
        openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
        - ERROR_TITLE_QUERY_TEMPLATE.get_tokens_size()
    )

    cleaned_tokens: list[int] = []
    truncated_log = ""
    truncated_log_ready = False
    truncated_log_tokens_length = 0

    line_iterator = reversed(log_lines)

    # start by removing useless ending lines
    for line in line_iterator:
        if not line:
            continue

    for line in reversed(log_lines):
        if not line:
            continue

        cleaned_line = cleaner.clean_line(line)
        # Start feeding truncated_log only on first non empty line
        if not cleaned_line and not truncated_log:
            continue

        if not truncated_log_ready:
            nb_tokens_in_line = len(openai_api.TIKTOKEN_ENCODING.encode(line))
            truncated_log_tokens_length += nb_tokens_in_line

            if truncated_log_tokens_length <= max_chat_completion_tokens:
                truncated_log = line + truncated_log

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


async def set_embedded_log_error_title(
    openai_client: openai_api.OpenAIClient, job: github_actions.WorkflowJob
) -> None:
    if job.embedded_log is None:
        raise RuntimeError(
            "set_embedded_log_error_title canlled with job.embedded_log=None"
        )

    query = openai_api.ChatCompletionQuery(
        role=ERROR_TITLE_QUERY_TEMPLATE.role,
        content=f"{ERROR_TITLE_QUERY_TEMPLATE.content}{job.embedded_log}",
        answer_size=ERROR_TITLE_QUERY_TEMPLATE.answer_size,
    )

    if (
        query.get_tokens_size()
        > openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
    ):
        # NOTE(sileht): the job.embedded_log has been created with an old
        # version of get_tokenized_cleaned_log() that doesn't truncate the log correctly
        # So just reset the state of this job
        job.log_status = github_actions.WorkflowJobLogStatus.UNKNOWN
        job.embedded_log = None
        job.log_embedding = None
        return

    chat_completion = await openai_client.get_chat_completion(query)
    title = chat_completion.get("choices", [{}])[0].get("message", {}).get("content")  # type: ignore[typeddict-item]

    if not title:
        LOG.error(
            "log-embedder: ChatGPT returned no title for the job log",
            job_id=job.id,
            chat_completion=chat_completion,
        )
        return

    job.embedded_log_error_title = title


@tracer.wrap("embed-logs")
async def embed_logs() -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    wjob = orm.aliased(github_actions.WorkflowJob, name="wjob")

    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(wjob)
            .join(github_repository.GitHubRepository)
            .join(
                github_account.GitHubAccount,
                sqlalchemy.and_(
                    github_repository.GitHubRepository.owner_id
                    == github_account.GitHubAccount.id,
                    github_account.GitHubAccount.login.in_(
                        settings.LOG_EMBEDDER_ENABLED_ORGS
                    ),
                ),
            )
            .where(
                wjob.conclusion == github_actions.WorkflowJobConclusion.FAILURE,
                sqlalchemy.or_(
                    wjob.log_embedding_retry_after.is_(None),
                    wjob.log_embedding_retry_after <= date.utcnow(),
                ),
                wjob.failed_step_number.is_not(None),
                wjob.log_status.notin_(
                    (
                        github_actions.WorkflowJobLogStatus.GONE,
                        github_actions.WorkflowJobLogStatus.ERROR,
                    )
                ),
                sqlalchemy.or_(
                    wjob.log_embedding.is_(None),
                    wjob.neighbours_computed_at.is_(None),
                    wjob.embedded_log_error_title.is_(None),
                ),
            )
            .order_by(wjob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )
        jobs = (await session.scalars(stmt)).all()

        LOG.info("log-embedder: %d jobs to embed", len(jobs), request=str(stmt))

        async with openai_api.OpenAIClient() as openai_client:
            job_ids_to_compute_cosine_similarity = []
            for job in jobs:
                try:
                    if job.log_embedding is None:
                        await embed_log(openai_client, job)

                    if (
                        job.embedded_log_error_title is None
                        and job.embedded_log is not None
                    ):
                        await set_embedded_log_error_title(openai_client, job)

                except Exception:
                    LOG.error(
                        "log-embedder: unexpected failure",
                        **job.as_log_extras(),
                        exc_info=True,
                    )

                if job.log_embedding is not None and job.neighbours_computed_at is None:
                    job_ids_to_compute_cosine_similarity.append(job.id)
                await session.commit()

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, job_ids_to_compute_cosine_similarity
        )
        await session.commit()
        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
