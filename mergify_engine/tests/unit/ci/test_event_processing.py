import typing
from unittest import mock

import msgpack
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import event_processing
from mergify_engine.ci import pull_registries
from mergify_engine.models import github_actions


@pytest.fixture
def sample_ci_events_to_process(
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]]
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
    logger_checker: None,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_run",
        "data": msgpack.packb(sample_ci_events_to_process[event_filename].slim_event),
    }
    await redis_links.stream.xadd("gha_workflow_run", stream_event)
    await redis_links.stream.xadd("gha_workflow_run", stream_event)

    with mock.patch.object(
        pull_registries.RedisPullRequestRegistry,
        "get_from_commit",
        return_value=[
            pull_registries.PullRequest(id=1, number=1, title="hello", state="open")
        ],
    ):
        await event_processing.process_event_streams(redis_links)

    workflow_runs = list(
        await db.scalars(sqlalchemy.select(github_actions.WorkflowRun))
    )
    assert len(workflow_runs) == 1
    actual_workflow_run = workflow_runs[0]
    assert (
        actual_workflow_run.event == github_actions.WorkflowRunTriggerEvent.PULL_REQUEST
    )

    pulls = list(await db.scalars(sqlalchemy.select(github_actions.PullRequest)))
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 1
    assert actual_pull.title == "hello"
    assert actual_pull.state == "open"

    associations = list(
        await db.scalars(
            sqlalchemy.select(github_actions.PullRequestWorkflowRunAssociation)
        )
    )
    assert len(associations) == 1
    actual_association = associations[0]
    assert actual_association.pull_request_id == 1

    stream_events = await redis_links.stream.xrange("workflow_run")
    assert len(stream_events) == 0


@pytest.mark.parametrize(
    "event_file_name, conclusion, failed_step_number, failed_step_name",
    (
        (
            "workflow_job.completed_failure.json",
            github_actions.WorkflowJobConclusion.FAILURE,
            3,
            "Run echo hello",
        ),
        (
            "workflow_job.completed.json",
            github_actions.WorkflowJobConclusion.SUCCESS,
            None,
            None,
        ),
    ),
)
async def test_process_event_stream_workflow_job(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    logger_checker: None,
    event_file_name: str,
    conclusion: github_actions.WorkflowJobConclusion,
    failed_step_number: int,
    failed_step_name: str,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(sample_ci_events_to_process[event_file_name].slim_event),
    }
    await redis_links.stream.xadd("gha_workflow_job", stream_event)
    await redis_links.stream.xadd("gha_workflow_job", stream_event)

    await event_processing.process_event_streams(redis_links)

    sql = sqlalchemy.select(github_actions.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 1
    actual_workflow_job = workflow_jobs[0]
    assert actual_workflow_job.conclusion == conclusion
    assert actual_workflow_job.labels == ["ubuntu-20.04"]

    assert actual_workflow_job.steps is not None
    assert len(actual_workflow_job.steps) == 5

    assert actual_workflow_job.failed_step_number == failed_step_number
    assert actual_workflow_job.failed_step_name == failed_step_name

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 0
