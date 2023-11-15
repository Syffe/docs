import typing

import msgpack
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import event_processing
from mergify_engine.models import github as gh_models


@pytest.fixture
def sample_ci_events_to_process(
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> dict[str, github_events.CIEventToProcess]:
    ci_events = {}

    for filename, (event_type, event) in sample_events.items():
        if event_type in ("workflow_run", "workflow_job"):
            ci_events[filename] = github_events.CIEventToProcess(event_type, "", event)

    return ci_events


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
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    event_filename: str,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_run",
        "data": msgpack.packb(sample_ci_events_to_process[event_filename].slim_event),
    }
    await redis_links.stream.xadd(
        "gha_workflow_run",
        fields=stream_event,  # type: ignore[arg-type]
    )
    await redis_links.stream.xadd(
        "gha_workflow_run",
        fields=stream_event,  # type: ignore[arg-type]
    )

    await event_processing.process_event_streams(redis_links)

    workflow_runs = list(await db.scalars(sqlalchemy.select(gh_models.WorkflowRun)))
    assert len(workflow_runs) == 1
    actual_workflow_run = workflow_runs[0]
    assert actual_workflow_run.event == gh_models.WorkflowRunTriggerEvent.PULL_REQUEST

    stream_events = await redis_links.stream.xrange("workflow_run")
    assert len(stream_events) == 0


@pytest.mark.parametrize(
    "event_file_name, conclusion, failed_step_number, failed_step_name, nb_steps",
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
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    event_file_name: str,
    conclusion: gh_models.WorkflowJobConclusion,
    failed_step_number: int,
    failed_step_name: str,
    nb_steps: int,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(sample_ci_events_to_process[event_file_name].slim_event),
    }
    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=stream_event,  # type: ignore[arg-type]
    )
    await redis_links.stream.xadd(
        "gha_workflow_job",
        fields=stream_event,  # type: ignore[arg-type]
    )

    await event_processing.process_event_streams(redis_links)

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
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    caplog: pytest.LogCaptureFixture,
) -> None:
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(
            sample_ci_events_to_process["workflow_job.broken_job.json"].slim_event,
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

    await event_processing.process_event_streams(redis_links)

    sql = sqlalchemy.select(gh_models.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 0
    stream_events = await redis_links.stream.xrange("gha_workflow_job")
    assert len(stream_events) == 0
