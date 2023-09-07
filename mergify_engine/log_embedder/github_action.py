import io
import re
import zipfile

import daiquiri
from ddtrace import tracer
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
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


async def embed_log(
    openai_client: openai_api.OpenAIClient, job: github_actions.WorkflowJob
) -> None:
    try:
        log_lines = await download_failed_step_log(job)
    except http.HTTPStatusError as e:
        if e.response.status_code == 410:
            job.log_status = github_actions.WorkflowJobLogStatus.GONE
            return
        raise

    tokens, first_line, last_line = await get_tokenized_cleaned_log(log_lines)
    embedding = await openai_client.get_embedding(tokens)
    job.log_embedding = embedding
    job.embedded_log = "".join(log_lines[first_line:last_line])
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

    LOG.info(
        "log-embedder: job log not found in zip file",
        job_id=job.id,
        job_data=job.as_dict(),
        files=[i.filename for i in zip_file.infolist()],
    )
    raise RuntimeError(
        "log-embedder: job log not found in zip file",
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
) -> tuple[list[int], int, int]:
    cleaner = log_cleaner.LogCleaner()

    tokens: list[int] = []
    first_line = 0
    last_line = 0
    max_tokens = openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN

    for i, line in enumerate(reversed(log_lines)):
        cleaned_line = cleaner.clean_line(line)
        if cleaned_line:
            tokenized_line = openai_api.TIKTOKEN_ENCODING.encode(cleaned_line)
            total_tokens = len(tokenized_line) + len(tokens)
            if total_tokens <= max_tokens:
                tokens = tokenized_line + tokens
                if last_line == 0:
                    last_line = len(log_lines) - i
                first_line = len(log_lines) - 1 - i

            if total_tokens >= max_tokens:
                break

    return tokens, first_line, last_line


async def set_embedded_log_error_title(
    openai_client: openai_api.OpenAIClient, job: github_actions.WorkflowJob
) -> None:
    chat_completion = await openai_client.get_chat_completion(
        [
            openai_api.ChatCompletionMessage(
                role=openai_api.ChatCompletionRole.user,
                content=f"""Analyze the following logs, spot the error and give me a
                meaningful title to qualify this error. Your answer must contain
                only the title. The logs: {job.embedded_log}""",
            )
        ]
    )
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
                if job.log_embedding is None:
                    try:
                        await embed_log(openai_client, job)
                    except openai_api.OpenAiException:
                        LOG.error(
                            "log-embedder: a job raises an unexpected error on log embedding",
                            job_id=job.id,
                            exc_info=True,
                        )

                if (
                    job.embedded_log_error_title is None
                    and job.embedded_log is not None
                ):
                    await set_embedded_log_error_title(openai_client, job)

                if job.log_embedding is not None and job.neighbours_computed_at is None:
                    job_ids_to_compute_cosine_similarity.append(job.id)
                await session.commit()

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, job_ids_to_compute_cosine_similarity
        )
        await session.commit()
        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
