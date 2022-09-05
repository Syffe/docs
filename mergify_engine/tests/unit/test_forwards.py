import os
from unittest import mock
import uuid

from pytest_httpserver import httpserver
from starlette import testclient

from mergify_engine import config
from mergify_engine import utils
from mergify_engine.web import root


@mock.patch(
    "mergify_engine.github_events.filter_and_dispatch",
    new_callable=mock.AsyncMock,
)
@mock.patch(
    "mergify_engine.config.WEBHOOK_FORWARD_EVENT_TYPES",
    new_callable=mock.PropertyMock(return_value=["push"]),
)
def test_app_event_forward(
    _: mock.Mock, __: mock.PropertyMock, httpserver: httpserver.HTTPServer
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
        with testclient.TestClient(root.app) as client:
            client.post("/event", data=data, headers=headers)

    httpserver.check_assertions()  # type: ignore[no-untyped-call]


@mock.patch(
    "mergify_engine.config.WEBHOOK_FORWARD_EVENT_TYPES",
    new_callable=mock.PropertyMock(return_value=["purchased"]),
)
def test_market_event_forward(
    _: mock.PropertyMock, httpserver: httpserver.HTTPServer
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
        with testclient.TestClient(root.app) as client:
            client.post("/marketplace", data=data, headers=headers)

    httpserver.check_assertions()  # type: ignore[no-untyped-call]
