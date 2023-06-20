import datetime
import typing

import daiquiri
import ddtrace
import msgpack
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.ci import job_registries
from mergify_engine.ci import models as ci_models
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)


class MissingWorkflowRunEvent(Exception):
    pass


class MissingWorkflowJobEvent(Exception):
    pass


class WorkflowJobAlreadyExists(Exception):
    pass


async def download_next_repositories(redis_links: redis_utils.RedisLinks) -> None:
    async with database.create_session() as session:
        next_repos = await get_next_repositories(session)

    for next_repo in next_repos:
        gh_client = await _create_gh_client_from_login(next_repo.owner)
        repository = await _get_repository_name_from_id(
            gh_client, next_repo.repository_id
        )
        await download(
            redis_links,
            gh_client,
            next_repo.owner,
            repository,
            date.DateTimeRange(
                next_repo.last_download_at,
                next_repo.last_download_at + settings.CI_DOWNLOAD_FREQUENCY,
            ),
        )

        async with database.create_session() as session:
            await update_repository_download_date(
                session, next_repo.repository_id, next_repo.last_download_at
            )


class NextRepositoryResult(typing.NamedTuple):
    owner_id: github_types.GitHubAccountIdType
    owner: github_types.GitHubLogin
    repository_id: github_types.GitHubRepositoryIdType
    last_download_at: datetime.datetime


async def get_next_repositories(
    session: sqlalchemy.ext.asyncio.AsyncSession,
) -> list[NextRepositoryResult]:
    sql = (
        sqlalchemy.select(github_repository.GitHubRepository)
        .where(
            (
                github_repository.GitHubRepository.last_download_at
                <= date.utcnow() - settings.CI_DOWNLOAD_FREQUENCY
            )
            | (github_repository.GitHubRepository.last_download_at.is_(None))
        )
        .order_by(
            github_repository.GitHubRepository.last_download_at.asc().nulls_first()
        )
        .limit(settings.CI_DOWNLOAD_BATCH_SIZE)
    )
    result = await session.scalars(sql)

    repos = []
    for row in result:
        default_last_download_at = date.utcnow() - settings.CI_DOWNLOAD_FREQUENCY
        at = row.last_download_at or default_last_download_at
        # NOTE(charly): we set the timezone because last_download_at is stored as a
        # TIMESTAMP WITHOUT TIME ZONE. We always work with UTC, the timezone doesn't
        # come from the user.
        atz = at.replace(tzinfo=datetime.UTC)

        repos.append(NextRepositoryResult(row.owner_id, row.owner.login, row.id, atz))
    return repos


async def _create_gh_client_from_login(
    owner: github_types.GitHubLogin,
) -> github.AsyncGitHubInstallationClient:
    auth = github.GitHubAppInstallationAuth(
        await github.get_installation_from_login(owner)
    )
    return github.AsyncGitHubInstallationClient(auth=auth)


async def _get_repository_name_from_id(
    gh_client: github.AsyncGitHubInstallationClient,
    repository_id: github_types.GitHubRepositoryIdType,
) -> github_types.GitHubRepositoryName:
    repo_data: github_types.GitHubRepository = await gh_client.item(
        f"/repositories/{repository_id}"
    )
    return repo_data["name"]


async def update_repository_download_date(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    repository_id: github_types.GitHubRepositoryIdType,
    download_at: datetime.date,
) -> None:
    sql = (
        sqlalchemy.update(github_repository.GitHubRepository)
        .values(last_download_at=download_at + settings.CI_DOWNLOAD_FREQUENCY)
        .where(github_repository.GitHubRepository.id == repository_id)
    )
    await session.execute(sql)
    await session.commit()


async def download(
    redis_links: redis_utils.RedisLinks,
    gh_client: github.AsyncGitHubInstallationClient,
    owner: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
    date_range: date.DateTimeRange,
) -> None:
    pg_job_registry = job_registries.PostgresJobRegistry()
    pg_pull_registry = pull_registries.PostgresPullRequestRegistry()
    http_pull_registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, pull_registries.HTTPPullRequestRegistry(client=gh_client)
    )
    http_job_registry = job_registries.HTTPJobRegistry(
        client=gh_client,
        pull_registry=http_pull_registry,
        destination_registry=pg_job_registry,
    )

    with ddtrace.tracer.trace(
        "ci.download", span_type="worker", resource=f"{owner}/{repository}"
    ) as span:
        span.set_tags({"gh_owner": owner, "gh_repo": repository})
        LOG.info(
            "download CI data",
            gh_owner=owner,
            gh_repo=repository,
            date_range=date_range,
        )

        async for job in http_job_registry.search(owner, repository, date_range):
            await _insert_job(pg_job_registry, pg_pull_registry, job)


async def process_event_stream(redis_links: redis_utils.RedisLinks) -> None:
    pg_job_registry = job_registries.PostgresJobRegistry()
    pg_pull_registry = pull_registries.PostgresPullRequestRegistry()

    with ddtrace.tracer.trace("ci.event_processing", span_type="worker"):
        min_stream_event_id = "-"

        while stream_events := await redis_links.stream.xrange(
            "workflow_job",
            min=min_stream_event_id,
            count=settings.CI_EVENT_PROCESSING_BATCH_SIZE,
        ):
            LOG.info(
                "process CI events",
                stream_event_ids=[id_.decode() for id_, _ in stream_events],
            )

            for stream_event_id, stream_event in stream_events:
                try:
                    await _process_stream_event(
                        redis_links,
                        pg_job_registry,
                        pg_pull_registry,
                        stream_event_id,
                        stream_event,
                    )
                except Exception:
                    LOG.exception(
                        "unprocessable CI event",
                        stream_event=stream_event,
                        stream_event_id=stream_event_id,
                    )

            min_stream_event_id = f"({stream_event_id.decode()}"


async def _process_stream_event(
    redis_links: redis_utils.RedisLinks,
    pg_job_registry: job_registries.PostgresJobRegistry,
    pg_pull_registry: pull_registries.PostgresPullRequestRegistry,
    stream_event_id: bytes,
    stream_event: dict[bytes, bytes],
) -> None:
    run_key = stream_event[b"workflow_run_key"].decode()

    try:
        run_event = await _get_run_event(redis_links, run_key)
    except MissingWorkflowRunEvent:
        # NOTE(charly): we haven't received the completed workflow run event, we
        # process the next event in the stream, this one will be processed later
        return

    job_id = stream_event[b"workflow_job_id"].decode()
    try:
        job_event = await _get_job_event(redis_links, run_key, job_id, pg_job_registry)
    except WorkflowJobAlreadyExists:
        # NOTE(charly): we already processed the workflow job event, GitHub send
        # it twice.
        pass
    else:
        job = await _create_job(redis_links, run_event, job_event)
        await _insert_job(pg_job_registry, pg_pull_registry, job)

    delete_pipeline = await redis_links.stream.pipeline()
    await delete_pipeline.hdel(run_key, f"workflow_job/{job_id}")
    await delete_pipeline.xdel("workflow_job", stream_event_id)
    await delete_pipeline.execute()


async def _get_run_event(
    redis_links: redis_utils.RedisLinks, run_key: str
) -> github_types.GitHubEventWorkflowRun:
    raw_run_event = await redis_links.stream.hget(run_key, "workflow_run")

    if raw_run_event is None:
        raise MissingWorkflowRunEvent

    return typing.cast(
        github_types.GitHubEventWorkflowRun,
        msgpack.unpackb(raw_run_event)["data"],
    )


async def _get_job_event(
    redis_links: redis_utils.RedisLinks,
    run_key: str,
    job_id: str,
    pg_job_registry: job_registries.PostgresJobRegistry,
) -> github_types.GitHubEventWorkflowJob:
    job_key = f"workflow_job/{job_id}"
    raw_job_event = await redis_links.stream.hget(run_key, job_key)

    if raw_job_event is None:
        if await pg_job_registry.exists(int(job_id)):
            raise WorkflowJobAlreadyExists

        raise MissingWorkflowJobEvent

    return typing.cast(
        github_types.GitHubEventWorkflowJob,
        msgpack.unpackb(raw_job_event)["data"],
    )


async def _create_job(
    redis_links: redis_utils.RedisLinks,
    run_event: github_types.GitHubEventWorkflowRun,
    job_event: github_types.GitHubEventWorkflowJob,
) -> ci_models.JobRun:
    try:
        owner = run_event["organization"]["login"]
    except KeyError:
        owner = run_event["repository"]["owner"]["login"]

    http_pull_registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, pull_registries.HTTPPullRequestRegistry(login=owner)
    )

    return await ci_models.JobRun.create_job(
        http_pull_registry, job_event["workflow_job"], run_event["workflow_run"]
    )


async def _insert_job(
    pg_job_registry: job_registries.PostgresJobRegistry,
    pg_pull_registry: pull_registries.PostgresPullRequestRegistry,
    job: ci_models.JobRun,
) -> None:
    await pg_job_registry.insert(job)

    for pull in job.pulls:
        await pg_pull_registry.insert(pull)
        await pg_pull_registry.register_job_run(pull.id, job)
