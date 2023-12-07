from collections import abc
import importlib
import json
import logging
import typing
from unittest import mock

from ddtrace import tracer
import pytest

from mergify_engine import logs


@pytest.fixture()
def _enable_tracer() -> abc.Generator[None, None, None]:
    enabled = tracer.enabled
    with mock.patch.object(tracer._writer, "flush_queue"), mock.patch.object(
        tracer._writer,
        "write",
    ):
        tracer.enabled = True
        try:
            yield
        finally:
            tracer.enabled = enabled


def test_logging(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    _enable_tracer: typing.Literal[None],
) -> None:
    monkeypatch.setenv("HEROKU_RELEASE_VERSION", "v1234")

    importlib.reload(logs)

    logs.WORKER_ID.set("shared-30")

    with tracer.trace(
        "testing",
        span_type="test",
        resource="test_logging",
        service="whatever",
    ) as span:
        span.set_tag("gh_owner", "foobar")
        record = logging.LogRecord(
            "name",
            logging.ERROR,
            "file.py",
            123,
            "this is impossible",
            (),
            None,
        )
        formatter = logs.HerokuDatadogFormatter()
        formatted = formatter.format(record)
        assert json.loads(formatted) == {
            "message": "this is impossible",
            "timestamp": mock.ANY,
            "status": "error",
            "logger": {"name": "name"},
            "dd.trace_id": mock.ANY,
            "dd.span_id": mock.ANY,
            "dd.root_span.resource": "test_logging",
            "dd.root_span.tags.gh_owner": "foobar",
            # FIXME(sileht): It should be "whatever" but it's buggy
            # here we get a random service depending on the order tests are
            # running, so use ANY for now.
            "dd.service": mock.ANY,
            "dd.version": mock.ANY,
            "dd.env": "",
            "HEROKU_RELEASE_VERSION": "v1234",
            "worker_id": "shared-30",
        }
