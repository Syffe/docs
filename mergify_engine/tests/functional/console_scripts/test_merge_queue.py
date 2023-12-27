import pytest

from mergify_engine.console_scripts import admin_cli
from mergify_engine.tests import utils
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder()
def test_merge_queue_reset(
    shadow_office: func_conftest.SubscriptionFixture,  # noqa: ARG001
    recorder: func_conftest.RecorderFixture,
    _setup_database: None,
) -> None:
    repo = f"https://github.com/{recorder.config['organization_name']}/{recorder.config['repository_name']}"
    result = utils.test_console_scripts(
        admin_cli.admin_cli,
        ["merge-queue-reset", repo],
    )
    assert result.exit_code == 0, result.output
    assert (
        result.output
        == f"{recorder.config['organization_name']}/{recorder.config['repository_name']} merge queue reseted\n"
    )
