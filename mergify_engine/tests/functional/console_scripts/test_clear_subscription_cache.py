import pytest

from mergify_engine.console_scripts import admin_cli
from mergify_engine.tests import utils
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder()
def test_clear_subscription_cache(
    recorder: func_conftest.RecorderFixture,
    _setup_database: None,
) -> None:
    result = utils.test_console_scripts(
        admin_cli.admin_cli,
        ["clear-subscription-cache", str(recorder.config["organization_id"])],
    )
    assert result.exit_code == 0, result.output
    assert (
        result.output
        == f"Subscription cache cleared for `{recorder.config['organization_id']}`\n"
    )
