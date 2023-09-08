import asyncio
import os
from unittest import mock
import uuid

import httpx
import pytest
import respx

from mergify_engine import event_forwarder
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.worker import manager


@mock.patch(
    "mergify_engine.github_events.filter_and_dispatch",
    new_callable=mock.AsyncMock,
)
@mock.patch(
    "mergify_engine.settings.GITHUB_WEBHOOK_FORWARD_URL",
    new_callable=mock.PropertyMock(return_value="https://forward.example.com"),
)
@mock.patch(
    "mergify_engine.settings.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES",
    new_callable=mock.PropertyMock(return_value=["push"]),
)
async def test_app_event_forward(
    _: mock.Mock,
    __: mock.PropertyMock,
    ___: mock.PropertyMock,
    web_client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    request: pytest.FixtureRequest,
    event_loop: asyncio.BaseEventLoop,
    redis_links: redis_utils.RedisLinks,
) -> None:
    with open(os.path.join(os.path.dirname(__file__), "events", "push.json")) as f:
        data = f.read()

    headers = {
        "X-GitHub-Delivery": str(uuid.uuid4()),
        "X-GitHub-Event": "push",
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data.encode(), settings.GITHUB_WEBHOOK_SECRET.get_secret_value())}",
        "User-Agent": "GitHub-Hookshot/044aadd",
        "Content-Type": "application/json",
    }
    respx_mock.post(
        "https://forward.example.com/", headers=headers, content=data
    ).respond(200, content="")

    w = manager.ServiceManager(
        enabled_services={"event-forwarder"},
        event_forwarder_idle_time=0,
    )
    await w.start()
    request.addfinalizer(lambda: event_loop.run_until_complete(w._shutdown()))
    await web_client.post("/event", content=data, headers=headers)
    while await redis_links.stream.xlen(event_forwarder.EVENT_FORWARDER_REDIS_KEY) != 0:
        await asyncio.sleep(0.001)
    assert respx_mock.calls.call_count == 1
