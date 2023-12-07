import logging
import typing

import daiquiri
from ddtrace import tracer
import msgpack

from mergify_engine import database
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


async def process_event_streams(redis_links: redis_utils.RedisLinks) -> None:
    await _process_workflow_run_stream(redis_links)
    await _process_workflow_job_stream(redis_links)


async def _process_workflow_run_stream(redis_links: redis_utils.RedisLinks) -> None:
    async for stream_event_id, stream_event in redis_utils.iter_stream(
        redis_links.stream,
        "gha_workflow_run",
        settings.CI_EVENT_PROCESSING_BATCH_SIZE,
    ):
        try:
            await _process_workflow_run_event(
                redis_links,
                stream_event_id,
                stream_event,
            )
        except Exception as e:
            if exceptions.should_be_ignored(e):
                return

            log_level = (
                logging.ERROR if exceptions.need_retry_in(e) is None else logging.INFO
            )
            LOG.log(
                log_level,
                "unprocessable workflow_run event",
                stream_event=stream_event,
                stream_event_id=stream_event_id,
                exc_info=True,
            )


@tracer.wrap("ci.workflow_run_processing")
async def _process_workflow_run_event(
    redis_links: redis_utils.RedisLinks,
    stream_event_id: bytes,
    stream_event: dict[bytes, bytes],
) -> None:
    workflow_run_event = typing.cast(
        github_types.GitHubEventWorkflowRun,
        msgpack.unpackb(stream_event[b"data"]),
    )
    workflow_run = workflow_run_event["workflow_run"]

    async for attempt in database.tenacity_retry_on_pk_integrity_error(
        (gh_models.GitHubRepository, gh_models.GitHubAccount),
    ):
        with attempt:
            async with database.create_session() as session:
                await gh_models.WorkflowRun.insert(
                    session,
                    workflow_run,
                    workflow_run_event["repository"],
                )

                await session.commit()

    await redis_links.stream.xdel("gha_workflow_run", stream_event_id)


async def _process_workflow_job_stream(redis_links: redis_utils.RedisLinks) -> None:
    async for stream_event_id, stream_event in redis_utils.iter_stream(
        redis_links.stream,
        "gha_workflow_job",
        settings.CI_EVENT_PROCESSING_BATCH_SIZE,
    ):
        try:
            await _process_workflow_job_event(
                redis_links,
                stream_event_id,
                stream_event,
            )
        except Exception as e:
            if exceptions.should_be_ignored(e):
                return

            log_level = (
                logging.ERROR if exceptions.need_retry_in(e) is None else logging.INFO
            )
            LOG.log(
                log_level,
                "unprocessable workflow_job event",
                stream_event=stream_event,
                stream_event_id=stream_event_id,
                exc_info=True,
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
            (gh_models.GitHubRepository, gh_models.GitHubAccount),
        ):
            with attempt:
                async with database.create_session() as session:
                    await gh_models.WorkflowJob.insert(
                        session,
                        workflow_job,
                        repository,
                    )
                    await session.commit()

    await redis_links.stream.xdel("gha_workflow_job", stream_event_id)
