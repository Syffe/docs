import pytest

from mergify_engine import console_scripts
from mergify_engine.tests import utils
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
def test_merge_queue_reset(
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
    setup_database: None,
) -> None:
    repo = f"https://github.com/{recorder.config['organization_name']}/{recorder.config['repository_name']}"
    result = utils.test_console_scripts(
        console_scripts.admin, ["merge-queue-reset", repo]
    )
    assert result.exit_code == 0, result.output
    assert (
        result.output
        == "mergifyio-testing/functional-testing-repo-sileht merge queue reseted\n"
    )
