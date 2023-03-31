import httpx
import pytest

from mergify_engine import settings
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
async def test_api_application(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
) -> None:
    r = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {
        "id": 123,
        "name": "testing application",
        "account_scope": {
            "id": settings.TESTING_ORGANIZATION_ID,
            "login": settings.TESTING_ORGANIZATION_NAME,
        },
    }
