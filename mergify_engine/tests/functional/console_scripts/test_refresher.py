import pytest

from mergify_engine import console_scripts
from mergify_engine.tests import utils
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
def test_refresher(
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
    setup_database: None,
) -> None:
    repo = f"https://github.com/{recorder.config['organization_name']}/{recorder.config['repository_name']}/branch/main"
    result = utils.test_console_scripts(
        console_scripts.admin, ["refresh", "--action=admin", repo]
    )
    assert result.exit_code == 0, result.output
    assert (
        result.output
        == "refresh of branch `https://github.com/mergifyio-testing/functional-testing-repo-sileht/branch/main` has been requested\n"
    )
