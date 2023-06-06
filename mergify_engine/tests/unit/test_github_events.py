import json
import os
import typing
from unittest import mock

import msgpack
import pytest

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.clients import github


GITHUB_SAMPLE_EVENTS = {}
_EVENT_DIR = os.path.join(os.path.dirname(__file__), "events")
for filename in os.listdir(_EVENT_DIR):
    event_type = filename.split(".")[0]
    with open(os.path.join(_EVENT_DIR, filename)) as event:
        GITHUB_SAMPLE_EVENTS[filename] = (event_type, json.load(event))


@pytest.mark.parametrize("event_type, event", list(GITHUB_SAMPLE_EVENTS.values()))
@mock.patch("mergify_engine.worker_pusher.push")
async def test_filter_and_dispatch(
    worker_push: mock.Mock,
    event_type: github_types.GitHubEventType,
    event: github_types.GitHubEvent,
    redis_links: redis_utils.RedisLinks,
) -> None:
    event_id = "my_event_id"
    hook_id = "my_hook_id"
    try:
        await github_events.filter_and_dispatch(
            mock.Mock(),
            redis_links,
            event_type,
            event_id,
            hook_id,
            event,
        )
    except github_events.IgnoredEvent as e:
        assert e.event_type == event_type
        assert e.event_id == event_id
        assert isinstance(e.reason, str)


async def test_event_classifier(
    redis_links: redis_utils.RedisLinks,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    mergify_bot = await github.GitHubAppInfo.get_bot(redis_links.cache)

    expected_event_classes = {
        "workflow_run.completed.json": github_events.CIEventToProcess,
        "workflow_job.completed.json": github_events.CIEventToProcess,
        "workflow_run.in_progress.json": github_events.EventToIgnore,
        "workflow_job.in_progress.json": github_events.EventToIgnore,
    }

    for filename, expected_event_class in expected_event_classes.items():
        event_type, event = sample_events[filename]
        classified_event = await github_events.event_classifier(
            redis_links, event_type, "whatever", "whatever", event, mergify_bot
        )
        assert isinstance(classified_event, expected_event_class)


async def test_push_ci_event(
    redis_links: redis_utils.RedisLinks,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    _, event = sample_events["workflow_run.completed.json"]
    run_id = event["workflow_run"]["id"]
    await worker_pusher.push_ci_event(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubRepositoryIdType(456),
        "workflow_run",
        "whatever",
        "whatever",
        event,
    )

    _, event = sample_events["workflow_job.completed.json"]
    job_id = event["workflow_job"]["id"]
    await worker_pusher.push_ci_event(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubRepositoryIdType(456),
        "workflow_job",
        "whatever",
        "whatever",
        event,
    )

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 1
    _, event = stream_events[0]
    assert event[b"owner_id"] == b"123"
    assert event[b"repo_id"] == b"456"
    assert event[b"workflow_run_id"] == str(run_id).encode()
    assert event[b"workflow_job_id"] == str(job_id).encode()
    assert event[b"workflow_run_key"] == f"workflow_run/123/456/{run_id}".encode()

    events = await redis_links.stream.hgetall(f"workflow_run/123/456/{run_id}")
    assert len(events) == 2

    assert b"workflow_run" in events
    run = msgpack.unpackb(events[b"workflow_run"])
    assert run["event_type"] == "workflow_run"

    assert f"workflow_job/{job_id}".encode() in events
    run = msgpack.unpackb(events[f"workflow_job/{job_id}".encode()])
    assert run["event_type"] == "workflow_job"

    # Push the same event twice, shouldn't raise an error
    _, event = sample_events["workflow_job.completed.json"]
    job_id = event["workflow_job"]["id"]
    await worker_pusher.push_ci_event(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubRepositoryIdType(456),
        "workflow_job",
        "whatever",
        "whatever",
        event,
    )
