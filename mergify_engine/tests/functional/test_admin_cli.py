import pytest

from mergify_engine import admin
from mergify_engine import settings


@pytest.mark.recorder
async def test_suspended() -> None:
    await admin.suspended("PUT", settings.TESTING_ORGANIZATION_NAME)
    await admin.suspended("DELETE", settings.TESTING_ORGANIZATION_NAME)
