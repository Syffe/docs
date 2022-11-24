import json
import os
from unittest import mock

import httpx
import pytest

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import utils


with open(os.path.join(os.path.dirname(__file__), "events", "push.json")) as f:
    push_event = json.load(f)

with open(os.path.join(os.path.dirname(__file__), "events", "pull_request.json")) as f:
    pull_request_event = json.load(f)


@pytest.mark.parametrize(
    "event,event_type,status_code,reason",
    (
        (
            {
                "sender": {
                    "login": "JD",
                },
                "event_type": "foobar",
            },
            "foobar",
            200,
            b"Event ignored: unexpected event_type",
        ),
        (
            push_event,
            "push",
            200,
            b"Event ignored: push on refs/tags/simple-tag",
        ),
        (
            pull_request_event,
            "pull_request",
            202,
            b"Event queued",
        ),
    ),
)
@mock.patch(
    "mergify_engine.config.WEBHOOK_SECRET_PRE_ROTATION",
    new_callable=mock.PropertyMock(return_value="secret!!"),
)
async def test_push_event(
    _: mock.PropertyMock,
    event: github_types.GitHubEvent,
    event_type: str,
    status_code: int,
    reason: bytes,
    web_client: httpx.AsyncClient,
    fake_github_app_info: None,
) -> None:
    charset = "utf-8"
    data = json.dumps(event).encode(charset)
    headers = {
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data, config.WEBHOOK_SECRET)}",
        "X-GitHub-Event": event_type,
        "Content-Type": f"application/json; charset={charset}",
        "X-GitHub-Delivery": "f00bar",
    }
    reply = await web_client.post("/event", content=data, headers=headers)
    assert reply.content == reason
    assert reply.status_code == status_code

    # Same with WEBHOOK_SECRET_PRE_ROTATION for key rotation
    assert config.WEBHOOK_SECRET_PRE_ROTATION is not None
    charset = "utf-8"
    data = json.dumps(event).encode(charset)
    headers = {
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data, config.WEBHOOK_SECRET_PRE_ROTATION)}",
        "X-GitHub-Event": event_type,
        "Content-Type": f"application/json; charset={charset}",
        "X-GitHub-Delivery": "f00bar",
    }
    reply = await web_client.post("/event", content=data, headers=headers)
    assert reply.content == reason
    assert reply.status_code == status_code
