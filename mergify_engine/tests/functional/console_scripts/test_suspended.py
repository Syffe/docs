import pytest

from mergify_engine import settings
from mergify_engine.console_scripts import account_suspend


@pytest.mark.recorder()
async def test_suspended() -> None:
    await account_suspend.suspended("PUT", settings.TESTING_ORGANIZATION_NAME)
    await account_suspend.suspended("DELETE", settings.TESTING_ORGANIZATION_NAME)
