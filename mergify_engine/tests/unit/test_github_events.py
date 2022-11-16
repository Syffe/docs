import json
import os
from unittest import mock

import pytest

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils


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
    fake_github_app_info: None,
) -> None:
    event_id = "my_event_id"
    try:
        await github_events.filter_and_dispatch(
            redis_links,
            event_type,
            event_id,
            event,
        )
    except github_events.IgnoredEvent as e:
        assert e.event_type == event_type
        assert e.event_id == event_id
        assert isinstance(e.reason, str)
