import os
from unittest import mock
import uuid

import httpx
from pytest_httpserver import httpserver

from mergify_engine import config
from mergify_engine import utils


@mock.patch(
    "mergify_engine.github_events.filter_and_dispatch",
    new_callable=mock.AsyncMock,
)
@mock.patch(
    "mergify_engine.config.WEBHOOK_FORWARD_EVENT_TYPES",
    new_callable=mock.PropertyMock(return_value=["push"]),
)
async def test_app_event_forward(
    _: mock.Mock,
    __: mock.PropertyMock,
    web_client: httpx.AsyncClient,
    httpserver: httpserver.HTTPServer,
) -> None:

    with open(os.path.join(os.path.dirname(__file__), "events", "push.json")) as f:
        data = f.read()

    headers = {
        "X-GitHub-Delivery": str(uuid.uuid4()),
        "X-GitHub-Event": "push",
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data.encode(), config.WEBHOOK_SECRET)}",
        "User-Agent": "GitHub-Hookshot/044aadd",
        "Content-Type": "application/json",
    }
    httpserver.expect_request(
        "/", method="POST", data=data, headers=headers
    ).respond_with_data("")

    with mock.patch(
        "mergify_engine.config.WEBHOOK_APP_FORWARD_URL",
        httpserver.url_for("/"),
    ):
        await web_client.post("/event", content=data, headers=headers)

    httpserver.check_assertions()  # type: ignore[no-untyped-call]


@mock.patch(
    "mergify_engine.config.WEBHOOK_FORWARD_EVENT_TYPES",
    new_callable=mock.PropertyMock(return_value=["purchased"]),
)
async def test_market_event_forward(
    _: mock.PropertyMock,
    httpserver: httpserver.HTTPServer,
    web_client: httpx.AsyncClient,
) -> None:

    with open(
        os.path.join(os.path.dirname(__file__), "events", "marketplace.json")
    ) as f:
        data = f.read()

    headers = {
        "X-GitHub-Delivery": str(uuid.uuid4()),
        "X-GitHub-Event": "purchased",
        "X-Hub-Signature": f"sha1={utils.compute_hmac(data.encode(), config.WEBHOOK_SECRET)}",
        "User-Agent": "GitHub-Hookshot/044aadd",
        "Content-Type": "application/json",
    }
    httpserver.expect_request(
        "/", method="POST", data=data, headers=headers
    ).respond_with_data("")

    with mock.patch(
        "mergify_engine.config.WEBHOOK_MARKETPLACE_FORWARD_URL",
        httpserver.url_for("/"),
    ):
        await web_client.post("/marketplace", content=data, headers=headers)

    httpserver.check_assertions()  # type: ignore[no-untyped-call]
