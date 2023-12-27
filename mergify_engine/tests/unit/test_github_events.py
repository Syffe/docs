import json
import os
import typing
from unittest import mock

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


@pytest.mark.parametrize(("event_type", "event"), list(GITHUB_SAMPLE_EVENTS.values()))
@mock.patch("mergify_engine.worker_pusher.push")
async def test_filter_and_dispatch(
    _worker_push: mock.Mock,
    event_type: github_types.GitHubEventType,
    event: github_types.GitHubEvent,
    redis_links: redis_utils.RedisLinks,
) -> None:
    event_id = "my_event_id"
    try:
        await github_events.filter_and_dispatch(
            mock.Mock(),
            redis_links,
            event_type,
            event_id,
            event,
        )
    except github_events.IgnoredEvent as e:
        # Not all events are supposed to raise an error, so the
        # PT017 is useless here.
        assert isinstance(e.reason, str)  # noqa: PT017


async def test_event_classifier(
    redis_links: redis_utils.RedisLinks,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    mergify_bot = await github.GitHubAppInfo.get_bot(redis_links.cache)

    expected_event_classes = {
        "workflow_run.completed.json": None,
        "workflow_job.completed.json": github_events.EventRoute.CI_MONITORING,
        "workflow_run.in_progress.json": None,
        "workflow_job.in_progress.json": None,
    }

    for filename, expected_event_routes in expected_event_classes.items():
        event_type, event = sample_events[filename]
        try:
            classified_event = await github_events.event_classifier(
                redis_links,
                event_type,
                "whatever",
                event,
                mergify_bot,
            )
        except github_events.IgnoredEvent:
            if expected_event_routes is not None:
                raise

        if expected_event_routes is not None:
            assert classified_event.routes == expected_event_routes


async def test_push_ci_event_workflow_run(
    redis_links: redis_utils.RedisLinks,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    _, event = sample_events["workflow_run.completed.json"]
    await worker_pusher.push_ci_event(
        redis_links.stream,
        "workflow_run",
        "whatever",
        event,
    )

    stream_events = await redis_links.stream.xrange("gha_workflow_run")
    assert len(stream_events) == 1

    _, event = stream_events[0]
    assert event[b"event_type"] == b"workflow_run"
    assert b"data" in event
    assert b"timestamp" in event
    assert b"delivery_id" in event


async def test_push_ci_event_workflow_job(
    redis_links: redis_utils.RedisLinks,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    _, event = sample_events["workflow_job.completed.json"]
    await worker_pusher.push_ci_event(
        redis_links.stream,
        "workflow_job",
        "whatever",
        event,
    )

    stream_events = await redis_links.stream.xrange("gha_workflow_job")
    assert len(stream_events) == 1

    _, event = stream_events[0]
    assert event[b"event_type"] == b"workflow_job"
    assert b"data" in event
    assert b"timestamp" in event
    assert b"delivery_id" in event
