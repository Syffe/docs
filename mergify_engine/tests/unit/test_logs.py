import logging
import typing

import pytest

from mergify_engine import logs
from mergify_engine import settings
from mergify_engine.config import types


@pytest.mark.parametrize(
    "url,url_expected",
    (
        ("redis://foobar/db?args", "redis://foobar/db?args"),
        ("redis://foo@bar/db?args", "redis://*****@bar/db?args"),
        ("redis://foobar@/db?args", "redis://*****@/db?args"),
        ("redis://foo:bar@/db?args", "redis://*****@/db?args"),
        ("redis://foo:bar@example/db?args", "redis://*****@example/db?args"),
        ("redis://foo:bar@example:80/db?args", "redis://*****@example:80/db?args"),
        ("redis://foo:bar@example:0/db?args", "redis://*****@example:0/db?args"),
        ("redis://foo:bar@:2/db?args", "redis://*****@:2/db?args"),
        ("redis://foo@:0/db?args", "redis://*****@:0/db?args"),
        ("redis://:bar@:2/db?args", "redis://*****@:2/db?args"),
    ),
)
def test_strip_url_credentials(url: str, url_expected: str) -> None:
    assert logs.strip_url_credentials(url) == url_expected


@pytest.mark.parametrize(
    "config_value,expected_hostname,expected_port",
    (
        (False, None, None),
        (True, "127.0.0.1", 10518),
        ("udp://foobar.example.com:1234", "foobar.example.com", 1234),
    ),
)
def test_datadog_logger(
    monkeypatch: pytest.MonkeyPatch,
    config_value: str | bool,
    expected_hostname: str | None,
    expected_port: int | None,
    logging_reset: None,
) -> None:
    if isinstance(config_value, bool):
        monkeypatch.setattr(settings, "LOG_DATADOG", config_value)
    else:
        monkeypatch.setattr(settings, "LOG_DATADOG", types.UdpUrl(config_value))

    monkeypatch.setattr(settings, "LOG_STDOUT", False)
    logs.setup_logging(dump_config=False)

    root_logger = logging.getLogger()
    if expected_hostname is None:
        assert root_logger.handlers == []
    else:
        assert len(root_logger.handlers) == 1
        handler = typing.cast(logging.handlers.SocketHandler, root_logger.handlers[0])
        assert handler.host == expected_hostname
        assert handler.port == expected_port
