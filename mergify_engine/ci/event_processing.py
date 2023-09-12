from collections import abc
import typing

import daiquiri
from ddtrace import tracer
import msgpack
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.ci import pull_registries
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


LOG = daiquiri.getLogger(__name__)


async def process_event_streams(redis_links: redis_utils.RedisLinks) -> None:
    await _process_workflow_run_stream(redis_links)
    await _process_workflow_job_stream(redis_links)


async def _process_workflow_run_stream(redis_links: redis_utils.RedisLinks) -> None:
    async for stream_event_id, stream_event in _iter_stream(
        redis_links, "gha_workflow_run", settings.CI_EVENT_PROCESSING_BATCH_SIZE
    ):
        try:
            await _process_workflow_run_event(
                redis_links, stream_event_id, stream_event
            )
        except Exception:
            LOG.exception(
                "unprocessable workflow_run event",
                stream_event=stream_event,
                stream_event_id=stream_event_id,
            )


async def _iter_stream(
    redis_links: redis_utils.RedisLinks, key: str, batch_size: int
) -> abc.AsyncGenerator[tuple[bytes, dict[bytes, bytes]], None]:
    min_stream_event_id = "-"

    while stream_events := await redis_links.stream.xrange(
        key, min=min_stream_event_id, count=batch_size
    ):
        for stream_event_id, stream_event in stream_events:
            yield stream_event_id, stream_event

        min_stream_event_id = f"({stream_event_id.decode()}"


@tracer.wrap("ci.workflow_run_processing")
async def _process_workflow_run_event(
    redis_links: redis_utils.RedisLinks,
    stream_event_id: bytes,
    stream_event: dict[bytes, bytes],
) -> None:
    workflow_run_event = typing.cast(
        github_types.GitHubEventWorkflowRun, msgpack.unpackb(stream_event[b"data"])
    )
    workflow_run = workflow_run_event["workflow_run"]

    async for attempt in database.tenacity_retry_on_pk_integrity_error(
        (github_repository.GitHubRepository, github_account.GitHubAccount)
    ):
        with attempt:
            async with database.create_session() as session:
                await github_actions.WorkflowRun.insert(
                    session, workflow_run, workflow_run_event["repository"]
                )

                if workflow_run["event"] in ("pull_request", "pull_request_target"):
                    await _insert_pull_request(redis_links, session, workflow_run_event)

                await session.commit()

    await redis_links.stream.xdel("gha_workflow_run", stream_event_id)  # type: ignore [no-untyped-call]


async def _insert_pull_request(
    redis_links: redis_utils.RedisLinks,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    workflow_run_event: github_types.GitHubEventWorkflowRun,
) -> None:
    try:
        login = workflow_run_event["organization"]["login"]
    except KeyError:
        login = workflow_run_event["repository"]["owner"]["login"]

    http_pull_registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, pull_registries.HTTPPullRequestRegistry(login=login)
    )

    workflow_run = workflow_run_event["workflow_run"]
    pulls = await http_pull_registry.get_from_commit(
        workflow_run["repository"]["owner"]["login"],
        workflow_run["repository"]["name"],
        workflow_run["head_sha"],
    )

    for pull in pulls:
        await github_actions.PullRequest.insert(session, pull)
        await github_actions.PullRequestWorkflowRunAssociation.insert(
            session, pull.id, workflow_run["id"]
        )


async def _process_workflow_job_stream(redis_links: redis_utils.RedisLinks) -> None:
    async for stream_event_id, stream_event in _iter_stream(
        redis_links, "gha_workflow_job", settings.CI_EVENT_PROCESSING_BATCH_SIZE
    ):
        try:
            await _process_workflow_job_event(
                redis_links, stream_event_id, stream_event
            )
        except Exception:
            LOG.exception(
                "unprocessable workflow_job event",
                stream_event=stream_event,
                stream_event_id=stream_event_id,
            )


@tracer.wrap("ci.workflow_job_processing")
async def _process_workflow_job_event(
    redis_links: redis_utils.RedisLinks,
    stream_event_id: bytes,
    stream_event: dict[bytes, bytes],
) -> None:
    event_data = msgpack.unpackb(stream_event[b"data"])
    workflow_job = typing.cast(
        github_types.GitHubWorkflowJob,
        event_data["workflow_job"],
    )
    repository = typing.cast(
        github_types.GitHubRepository,
        event_data["repository"],
    )

    # NOTE(Kontrolix): This test is here to filter some broken jobs
    if workflow_job.get("runner_id") is not None:
        async for attempt in database.tenacity_retry_on_pk_integrity_error(
            (github_repository.GitHubRepository, github_account.GitHubAccount)
        ):
            with attempt:
                async with database.create_session() as session:
                    await github_actions.WorkflowJob.insert(
                        session, workflow_job, repository
                    )
                    await session.commit()

    await redis_links.stream.xdel("gha_workflow_job", stream_event_id)  # type: ignore [no-untyped-call]
