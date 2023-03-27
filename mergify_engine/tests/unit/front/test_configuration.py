import pytest

from mergify_engine import config
from mergify_engine import settings
from mergify_engine.tests import conftest


async def test_site_configuration(
    web_client: conftest.CustomTestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        settings, "DASHBOARD_UI_DATADOG_CLIENT_TOKEN", "a-not-so-secret-token"
    )
    monkeypatch.setattr(
        settings,
        "DASHBOARD_UI_FEATURES",
        ["applications", "intercom", "subscriptions", "statuspage"],
    )

    resp = await web_client.get("/front/configuration")
    assert resp.status_code == 200
    assert resp.json() == {
        "dd_client_token": "a-not-so-secret-token",
        "github_application_name": "mergify-test",
        "github_server_url": config.GITHUB_URL,
        "ui_features": ["applications", "intercom", "statuspage", "subscriptions"],
    }
