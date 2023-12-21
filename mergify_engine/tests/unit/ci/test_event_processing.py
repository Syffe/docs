import datetime
import typing

import msgpack
import pytest
import sqlalchemy
from sqlalchemy import func
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import redis_utils
from mergify_engine.ci import event_processing
from mergify_engine.models import github as gh_models
from mergify_engine.tests.tardis import time_travel


MAIN_TIMESTAMP = "2023-05-11T12:40:28Z"
LATER_TIMESTAMP = "2024-08-22T12:00:00+00:00"


@pytest.mark.parametrize(
    "event_filename",
    [
        "workflow_run.completed.json",
        "workflow_run.no_org.json",
        "workflow_run.completed.no-actor.json",
    ],
)
async def test_process_event_stream_workflow_run(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, typing.Any],
    event_filename: str,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_run",
        "data": msgpack.packb(sample_ci_events_to_process[event_filename]),
    }
    await redis_links.stream.xadd(
        "gha_workflow_run",
        fields=stream_event,  # type: ignore[arg-type]
    )
    await redis_links.stream.xadd(
        "gha_workflow_run",
        fields=stream_event,  # type: ignore[arg-type]
    )

    await event_processing.process_workflow_run_stream(redis_links)

    workflow_runs = list(await db.scalars(sqlalchemy.select(gh_models.WorkflowRun)))
    assert len(workflow_runs) == 1
    actual_workflow_run = workflow_runs[0]
    assert actual_workflow_run.event == gh_models.WorkflowRunTriggerEvent.PULL_REQUEST

    stream_events = await redis_links.stream.xrange("workflow_run")
    assert len(stream_events) == 0


@pytest.mark.parametrize(
    (
        "event_file_name",
        "conclusion",
        "failed_step_number",
        "failed_step_name",
        "nb_steps",
    ),
    (
        (
            "workflow_job.completed_failure.json",
            gh_models.WorkflowJobConclusion.FAILURE,
            3,
            "Run echo hello",
            5,
        ),
        (
            "workflow_job.completed.json",
            gh_models.WorkflowJobConclusion.SUCCESS,
            None,
            None,
            5,
        ),
        (
            "workflow_job.completed_failure_no_failed_steps.json",
            gh_models.WorkflowJobConclusion.FAILURE,
            None,
            None,
            5,
        ),
    ),
)
async def test_process_event_stream_workflow_job(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, typing.Any],
    event_file_name: str,
    conclusion: gh_models.WorkflowJobConclusion,
    failed_step_number: int,
    failed_step_name: str,
    nb_steps: int,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(sample_ci_events_to_process[event_file_name]),
    }
    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=stream_event,  # type: ignore[arg-type]
    )
    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=stream_event,  # type: ignore[arg-type]
    )

    await event_processing.process_workflow_job_stream(redis_links)

    sql = sqlalchemy.select(gh_models.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 1
    current_workflow_job = workflow_jobs[0]
    assert current_workflow_job.conclusion == conclusion
    assert current_workflow_job.labels == ["ubuntu-20.04"]

    assert current_workflow_job.steps is not None
    assert len(current_workflow_job.steps) == nb_steps

    assert current_workflow_job.failed_step_number == failed_step_number
    assert current_workflow_job.failed_step_name == failed_step_name

    assert current_workflow_job.head_sha == "967926ca14c083f858d26dd4a5e669febe9c3a2f"

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 0


async def test_process_event_stream_broken_workflow_job(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, typing.Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(
            sample_ci_events_to_process["workflow_job.broken_job.json"],
        ),
    }
    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=stream_event,  # type: ignore[arg-type]
    )

    sql = sqlalchemy.select(gh_models.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 0
    stream_events = await redis_links.stream.xrange("gha_workflow_job")
    assert len(stream_events) == 1

    await event_processing.process_workflow_job_stream(redis_links)

    sql = sqlalchemy.select(gh_models.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 0
    stream_events = await redis_links.stream.xrange("gha_workflow_job")
    assert len(stream_events) == 0


@time_travel(datetime.datetime.fromisoformat(LATER_TIMESTAMP))
async def test_delete_workflow_job(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, typing.Any],
) -> None:
    monkeypatch.setattr(
        database,
        "CLIENT_DATA_RETENTION_TIME",
        datetime.timedelta(hours=1),
    )

    outdated_job_slim_event = sample_ci_events_to_process[
        "workflow_job.completed_failure.json"
    ]
    outdated_job_slim_event["workflow_job"]["completed_at"] = MAIN_TIMESTAMP

    outdated_job_stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(outdated_job_slim_event),
    }

    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=outdated_job_stream_event,  # type: ignore[arg-type]
    )
    await event_processing.process_workflow_job_stream(redis_links)

    outdated_log_metadata = gh_models.WorkflowJobLogMetadata(
        workflow_job_id=outdated_job_slim_event["workflow_job"]["id"],
    )
    db.add(outdated_log_metadata)

    # we create a non-outdated job to check it will not be deleted
    job_slim_event = outdated_job_slim_event
    job_slim_event["workflow_job"]["id"] = 13403749846
    job_slim_event["workflow_job"]["completed_at"] = LATER_TIMESTAMP
    job_stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(job_slim_event),
    }

    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=job_stream_event,  # type: ignore[arg-type]
    )
    await event_processing.process_workflow_job_stream(redis_links)

    log_metadata = gh_models.WorkflowJobLogMetadata(
        workflow_job_id=job_slim_event["workflow_job"]["id"],
    )
    db.add(log_metadata)
    await db.commit()

    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(gh_models.WorkflowJob),
    )
    assert result.scalar() == 2

    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(gh_models.WorkflowJobLogMetadata),
    )
    assert result.scalar() == 2

    await event_processing.delete_outdated_workflow_jobs()

    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(gh_models.WorkflowJob),
    )
    assert result.scalar() == 1

    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(gh_models.WorkflowJobLogMetadata),
    )
    assert result.scalar() == 1

    job = await db.scalar(sqlalchemy.select(gh_models.WorkflowJob))
    assert job is not None
    assert job.id == 13403749846
