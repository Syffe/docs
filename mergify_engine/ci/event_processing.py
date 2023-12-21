import typing

import daiquiri
from ddtrace import tracer
import msgpack

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


GHA_WORKFLOW_RUN_REDIS_KEY = "gha_workflow_run"
GHA_WORKFLOW_JOB_REDIS_KEY = "gha_workflow_job"


async def process_workflow_run_stream(redis_links: redis_utils.RedisLinks) -> bool:
    return await redis_utils.process_stream(
        "gha_workflow_run",
        redis_links.stream,
        redis_key=GHA_WORKFLOW_RUN_REDIS_KEY,
        batch_size=settings.CI_EVENT_PROCESSING_BATCH_SIZE,
        event_processor=_process_workflow_run_event,
    )


@tracer.wrap("ci.workflow_run_processing")
async def _process_workflow_run_event(
    event_id: bytes,
    event: dict[bytes, bytes],
) -> None:
    workflow_run_event = typing.cast(
        github_types.GitHubEventWorkflowRun,
        msgpack.unpackb(event[b"data"]),
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


async def process_workflow_job_stream(redis_links: redis_utils.RedisLinks) -> bool:
    return await redis_utils.process_stream(
        "gha_workflow_job",
        redis_links.stream,
        redis_key=GHA_WORKFLOW_JOB_REDIS_KEY,
        batch_size=settings.CI_EVENT_PROCESSING_BATCH_SIZE,
        event_processor=_process_workflow_job_event,
    )


@tracer.wrap("ci.workflow_job_processing")
async def _process_workflow_job_event(
    event_id: bytes,
    event: dict[bytes, bytes],
) -> None:
    event_data = msgpack.unpackb(event[b"data"])
    workflow_job = typing.cast(
        github_types.GitHubWorkflowJob,
        event_data["workflow_job"],
    )
    repository = typing.cast(
        github_types.GitHubRepository,
        event_data["repository"],
    )

    # NOTE(Kontrolix): This test is here to filter some broken jobs
    if workflow_job.get("runner_id") is None:
        return

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


async def delete_outdated_workflow_jobs() -> None:
    async with database.create_session() as session:
        await gh_models.WorkflowJob.delete_outdated(
            session,
            retention_time=database.CLIENT_DATA_RETENTION_TIME,
        )
        await session.commit()
