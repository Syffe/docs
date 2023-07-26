from collections import abc

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


LOG_EMBEDDER_JOBS_BATCH_SIZE = 100


async def embed_log(job: github_actions.WorkflowJob) -> None:
    cleaner = log_cleaner.LogCleaner()
    cleaned_log = []
    async for log_line in log_download(job):
        cleaned_log_line = cleaner.clean_line(log_line)

        if cleaned_log_line:
            cleaned_log.append(cleaned_log_line)

    embedding = await openai_embedding.get_embedding("\n".join(cleaned_log))

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


@tracer.wrap("log_embedder.run_log_embedder_pipelines", span_type="worker")
async def embed_logs() -> bool:
    if not settings.LOG_EMBEDDER_ENABLED_ORGS:
        return False

    async with database.create_session() as session:
        stmt = (
            (
                sqlalchemy.select(github_actions.WorkflowJob)
                .join(github_repository.GitHubRepository)
                .join(
                    github_repository.GitHubRepository.owner.and_(
                        github_account.GitHubAccount.login.in_(
                            settings.LOG_EMBEDDER_ENABLED_ORGS
                        )
                    )
                )
                .where(github_actions.WorkflowJob.log_embedding.is_(None))
            )
            .order_by(github_actions.WorkflowJob.completed_at.asc())
            .limit(LOG_EMBEDDER_JOBS_BATCH_SIZE)
        )
        jobs = (await session.scalars(stmt)).all()
        for job in jobs:
            await embed_log(job)
            await session.commit()
        return LOG_EMBEDDER_JOBS_BATCH_SIZE - len(jobs) == 0
