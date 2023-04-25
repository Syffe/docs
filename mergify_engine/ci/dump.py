import datetime
import typing

import daiquiri
import ddtrace
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import job_registries
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)

DUMP_FREQUENCY = datetime.timedelta(days=1)


class NoDataToDump(Exception):
    pass


async def dump_next_repository(redis_links: redis_utils.RedisLinks) -> None:
    async with database.create_session() as session:
        try:
            next_sub = await get_next_repository(session)
        except NoDataToDump:
            return

    gh_client = await _create_gh_client_from_login(next_sub.owner)
    repository = await _get_repository_name_from_id(gh_client, next_sub.repository_id)
    await dump(
        redis_links, gh_client, next_sub.owner, repository, next_sub.last_dump_at.date()
    )

    async with database.create_session() as session:
        await update_repository_dump_date(
            session, next_sub.repository_id, next_sub.last_dump_at
        )


class NextRepositoryResult(typing.NamedTuple):
    owner_id: github_types.GitHubAccountIdType
    owner: github_types.GitHubLogin
    repository_id: github_types.GitHubRepositoryIdType
    last_dump_at: datetime.datetime


async def get_next_repository(
    session: sqlalchemy.ext.asyncio.AsyncSession,
) -> NextRepositoryResult:
    sql = (
        sqlalchemy.select(github_repository.GitHubRepository)
        .where(
            (github_repository.GitHubRepository.last_dump_at < datetime.date.today())
            | (github_repository.GitHubRepository.last_dump_at.is_(None))
        )
        .order_by(github_repository.GitHubRepository.last_dump_at.asc().nulls_first())
        .limit(1)
    )

    result = await session.execute(sql)
    row = result.first()
    if not row:
        raise NoDataToDump

    owner_id = row.GitHubRepository.owner.id
    owner = row.GitHubRepository.owner.login
    repository_id = row.GitHubRepository.id
    yesterday = (
        datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        - DUMP_FREQUENCY
    )
    at = row.GitHubRepository.last_dump_at or yesterday

    return NextRepositoryResult(owner_id, owner, repository_id, at)


async def _create_gh_client_from_login(
    owner: github_types.GitHubLogin,
) -> github.AsyncGithubInstallationClient:
    auth = github.GithubAppInstallationAuth(
        await github.get_installation_from_login(owner)
    )
    return github.AsyncGithubInstallationClient(auth=auth)


async def _get_repository_name_from_id(
    gh_client: github.AsyncGithubInstallationClient,
    repository_id: github_types.GitHubRepositoryIdType,
) -> github_types.GitHubRepositoryName:
    repo_data: github_types.GitHubRepository = await gh_client.item(
        f"/repositories/{repository_id}"
    )
    return repo_data["name"]


async def update_repository_dump_date(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    repository_id: github_types.GitHubRepositoryIdType,
    dump_at: datetime.date,
) -> None:
    sql = (
        sqlalchemy.update(github_repository.GitHubRepository)
        .values(last_dump_at=dump_at + DUMP_FREQUENCY)
        .where(github_repository.GitHubRepository.id == repository_id)
    )
    await session.execute(sql)
    await session.commit()


async def dump(
    redis_links: redis_utils.RedisLinks,
    gh_client: github.AsyncGithubInstallationClient,
    owner: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
    at: datetime.date,
) -> None:
    LOG.info("dump CI data", gh_owner=owner, gh_repo=repository, at=at)

    http_pull_registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, pull_registries.HTTPPullRequestRegistry(gh_client)
    )
    http_job_registry = job_registries.HTTPJobRegistry(gh_client, http_pull_registry)

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
