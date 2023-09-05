import io
import re
import zipfile

import daiquiri
from ddtrace import tracer
import sqlalchemy

from mergify_engine import database
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_api
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)

LOG_EMBEDDER_JOBS_BATCH_SIZE = 100

# NOTE(Kontrolix): We remove all the windows forbidden path character as github does
CLEAN_FAILED_STEP_REGEXP = re.compile(r"[\*\"/\\<>:|\?]")


async def embed_log(
    openai_client: openai_api.OpenAIClient, job: github_actions.WorkflowJob
) -> None:
    log_lines = await download_failed_step_log(job)
    tokens, first_line, last_line = await get_tokenized_cleaned_log(log_lines)
    embedding = await openai_client.get_embedding(tokens)
    job.log_embedding = embedding
    job.embedded_log = "".join(log_lines[first_line:last_line])


async def download_failed_step_log(
    job: github_actions.WorkflowJob,
) -> list[str]:
    if job.failed_step_number is None or job.failed_step_name is None:
        LOG.info(
            "log-embedder: Tried to download log with not enough data",
            job_id=job.id,
            job_data=job.as_dict(),
        )
        raise RuntimeError(
            "We should not have arrived here, let's find why it happened."
        )

    repo = job.repository

    installation_json = await github.get_installation_from_login(repo.owner.login)

    with io.BytesIO() as zip_data:
        async with github.aget_client(installation_json) as client:
            resp = await client.get(
                f"/repos/{repo.owner.login}/{repo.name}/actions/runs/{job.workflow_run_id}/attempts/{job.run_attempt}/logs"
            )
            zip_data.write(resp.content)

        with zipfile.ZipFile(zip_data, "r") as zip_file:
            with zip_file.open(
                f"{job.name}/{job.failed_step_number}_{CLEAN_FAILED_STEP_REGEXP.sub('', job.failed_step_name)}.txt"
            ) as log_file:
                return [line.decode() for line in log_file.readlines()]


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


@tracer.wrap("embed-logs")
async def embed_logs() -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(github_actions.WorkflowJob)
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
                github_actions.WorkflowJob.conclusion
                == github_actions.WorkflowJobConclusion.FAILURE,
                sqlalchemy.or_(
                    github_actions.WorkflowJob.log_embedding.is_(None),
                    github_actions.WorkflowJob.neighbours_computed_at.is_(None),
                ),
            )
            .order_by(github_actions.WorkflowJob.completed_at.asc())
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

                if job.neighbours_computed_at is None:
                    job_ids_to_compute_cosine_similarity.append(job.id)
                await session.commit()

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, job_ids_to_compute_cosine_similarity
        )
        await session.commit()
        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
