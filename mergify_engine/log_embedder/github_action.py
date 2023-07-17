from collections import abc

from mergify_engine.clients import github
from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import openai_embedding
from mergify_engine.models import github_actions


async def log_embedding(job: github_actions.WorkflowJob) -> None:
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
