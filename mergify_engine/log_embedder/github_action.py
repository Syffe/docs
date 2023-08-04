from collections import abc
from collections import deque

import daiquiri
from ddtrace import tracer
import sqlalchemy

from mergify_engine import database
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_embedding
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)

LOG_EMBEDDER_JOBS_BATCH_SIZE = 100


async def embed_log(job: github_actions.WorkflowJob) -> None:
    tokens = await get_tokenized_cleaned_log(job)
    embedding = await openai_embedding.get_embedding(tokens)
    job.log_embedding = embedding


async def log_download(
    job: github_actions.WorkflowJob,
) -> abc.AsyncGenerator[str, None]:
    repo = job.repository

    installation_json = await github.get_installation_from_login(repo.owner.login)
    async with github.aget_client(installation_json) as client:
        async with client.stream(
            "GET", f"/repos/{repo.owner.login}/{repo.name}/actions/jobs/{job.id}/logs"
        ) as resp:
            async for line in resp.aiter_lines():
                yield line


async def get_tokenized_cleaned_log(job: github_actions.WorkflowJob) -> list[int]:
    cleaner = log_cleaner.LogCleaner()

    tokens: deque[int] = deque(
        maxlen=openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    )
    async for log_line in log_download(job):
        cleaned_log_line = cleaner.clean_line(log_line)
        if cleaned_log_line:
            line_tokenized = openai_embedding.TIKTOKEN_ENCODING.encode(cleaned_log_line)
            tokens.extend(line_tokenized)

    return list(tokens)


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

        job_ids = []
        for job in jobs:
            if job.log_embedding is None:
                await embed_log(job)
            job_ids.append(job.id)
            await session.commit()
        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, job_ids
        )
        await session.commit()
        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
