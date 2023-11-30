import httpx
import pytest

from mergify_engine import settings
from mergify_engine.tests import utils
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
async def test_api_application(
    web_client: httpx.AsyncClient,
    shadow_office: func_conftest.SubscriptionFixture,
) -> None:
    r = await web_client.get(
        "/v1/application",
        headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {
        "id": utils.ANY_UUID4,
        "name": "on-premise-app-from-env",
        "account_scope": {
            "id": settings.TESTING_ORGANIZATION_ID,
            "login": settings.TESTING_ORGANIZATION_NAME,
        },
    }
