from unittest import mock

from mergify_engine import console_scripts
from mergify_engine.tests import utils


@mock.patch.object(console_scripts.account_suspend, "suspended")
def test_admin_suspend(suspended: mock.Mock, setup_database: None) -> None:
    result = utils.test_console_scripts(console_scripts.admin, ["suspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` suspended\n"
    assert suspended.mock_calls == [mock.call("PUT", "foobar")]


@mock.patch.object(console_scripts.account_suspend, "suspended")
def test_admin_unsuspend(suspended: mock.Mock, setup_database: None) -> None:
    result = utils.test_console_scripts(console_scripts.admin, ["unsuspend", "foobar"])
    assert result.exit_code == 0
    assert result.output == "Account `foobar` unsuspended\n"
    assert suspended.mock_calls == [mock.call("DELETE", "foobar")]
