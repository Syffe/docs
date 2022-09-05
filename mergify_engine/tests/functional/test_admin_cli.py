import pytest

from mergify_engine import admin
from mergify_engine import config


@pytest.mark.recorder
async def test_suspended() -> None:
    await admin.suspended("PUT", config.TESTING_ORGANIZATION_NAME)
    await admin.suspended("DELETE", config.TESTING_ORGANIZATION_NAME)
