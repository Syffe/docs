from unittest import mock

from mergify_engine.console_scripts import account_suspend
from mergify_engine.console_scripts import admin_cli
from mergify_engine.tests import utils


@mock.patch.object(account_suspend, "suspended")
def test_admin_suspend(suspended: mock.Mock, setup_database: None) -> None:
    result = utils.test_console_scripts(admin_cli.admin_cli, ["suspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` suspended\n"
    assert suspended.mock_calls == [mock.call("PUT", "foobar")]


@mock.patch.object(account_suspend, "suspended")
def test_admin_unsuspend(suspended: mock.Mock, setup_database: None) -> None:
    result = utils.test_console_scripts(admin_cli.admin_cli, ["unsuspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` unsuspended\n"
    assert suspended.mock_calls == [mock.call("DELETE", "foobar")]
