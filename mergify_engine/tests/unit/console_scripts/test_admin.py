import re
from unittest import mock

import pytest
import respx

from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.console_scripts import account_suspend
from mergify_engine.console_scripts import admin_cli
from mergify_engine.console_scripts import connectivity_check
from mergify_engine.tests import utils


@mock.patch.object(account_suspend, "suspended")
def test_admin_suspend(suspended: mock.Mock, _setup_database: None) -> None:
    result = utils.test_console_scripts(admin_cli.admin_cli, ["suspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` suspended\n"
    assert suspended.mock_calls == [mock.call("PUT", "foobar")]


@mock.patch.object(account_suspend, "suspended")
def test_admin_unsuspend(suspended: mock.Mock, _setup_database: None) -> None:
    result = utils.test_console_scripts(admin_cli.admin_cli, ["unsuspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` unsuspended\n"
    assert suspended.mock_calls == [mock.call("DELETE", "foobar")]


def test_check_connectivity(
    _setup_database: None,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        settings,
        "ENV_CACHE_URL",
        types.RedisDSN.parse("redis://not-exists.localhost:12345"),
    )
    monkeypatch.setattr(
        settings,
        "GITHUB_URL",
        types.NormalizedUrl("https://github.example.com"),
    )
    monkeypatch.setattr(connectivity_check, "TIMEOUT", 0.2)
    respx_mock.get("https://github.example.com/api/v3/app").respond(418)

    result = utils.test_console_scripts(admin_cli.admin_cli, ["connectivity-check"])
    assert result.exit_code == 0, result.output
    assert re.match(
        r"""Redis: failed to connect
> Error .*not-exists\.localhost:12345.*
Postgres: connected
GitHub server: failed to connect
> 418 Client Error: I'm a teapot for url `https://github\.example\.com/api/v3/app`
> Details: <empty-response>
""",
        result.output,
    )
