import datetime

import daiquiri
import ddtrace

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import job_registries
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github


LOG = daiquiri.getLogger(__name__)


async def dump(
    redis_links: redis_utils.RedisLinks,
    owner: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
    at: datetime.date,
    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth | None = None,
) -> None:
    LOG.info("dump CI data", gh_owner=owner, gh_repo=repository, at=at)

    if auth is None:
        auth = github.GithubAppInstallationAuth(
            await github.get_installation_from_login(owner)
        )

    client = github.AsyncGithubInstallationClient(auth=auth)
    http_pull_registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, pull_registries.HTTPPullRequestRegistry(client)
    )
    http_job_registry = job_registries.HTTPJobRegistry(client, http_pull_registry)

    pg_job_registry = job_registries.PostgresJobRegistry()
    pg_pull_registry = pull_registries.PostgresPullRequestRegistry()

    with ddtrace.tracer.trace(
        "ci.dump", span_type="worker", resource=f"{owner}/{repository}"
    ) as span:
        span.set_tags({"gh_owner": owner, "gh_repo": repository})

        async for job in http_job_registry.search(owner, repository, at):
            await pg_job_registry.insert(job)

            for pull in job.pulls:
                await pg_pull_registry.insert(pull)
                await pg_pull_registry.register_job_run(pull.id, job)
