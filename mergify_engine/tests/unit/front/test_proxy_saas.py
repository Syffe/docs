import pytest
import respx
import sqlalchemy

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models import github_user
from mergify_engine.tests import conftest


async def test_saas_proxy_saas_mode_true(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", True)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    unwanted_headers = respx.patterns.M(headers={"dnt": "1"})
    assert unwanted_headers is not None
    respx_mock.route(
        respx.patterns.M(
            method="get",
            url="http://localhost:5000/engine/saas/github-account/42/stripe-create",
            params="plan=Essential",
            headers={
                "Authorization": f"Bearer {config.ENGINE_TO_DASHBOARD_API_KEY}",
                "Mergify-On-Behalf-Of": str(user.id),
            },
        )
        & ~unwanted_headers
    ).respond(200, json={"url": "https://portal.stripe.com/foobar"})

    url = "/front/proxy/saas/github-account/42/stripe-create?plan=Essential"

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401

    await web_client.log_as(user.id)

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.json() == {"url": "https://portal.stripe.com/foobar"}

    await web_client.logout()
    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401


async def test_saas_proxy_saas_mode_false(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", False)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    url = "/front/proxy/saas/github-account/42/stripe-create?plan=Essential"

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401

    await web_client.log_as(user.id)

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 510

    await web_client.logout()
    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401


async def test_saas_subscription_with_saas_mode_true(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", True)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    unwanted_headers = respx.patterns.M(headers={"dnt": "1"})
    assert unwanted_headers is not None
    respx_mock.route(
        respx.patterns.M(
            method="get",
            url="http://localhost:5000/engine/saas/github-account/42/subscription-details",
            headers={
                "Authorization": f"Bearer {config.ENGINE_TO_DASHBOARD_API_KEY}",
                "Mergify-On-Behalf-Of": str(user.id),
            },
        )
        & ~unwanted_headers
    ).respond(200, json={"plan": {"name": "Essential"}})

    url = "/front/proxy/saas/github-account/42/subscription-details"

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401

    await web_client.log_as(user.id)

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.json() == {"plan": {"name": "Essential"}}

    await web_client.logout()
    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401


async def test_saas_subscription_with_saas_mode_false(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", False)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    url = "/front/proxy/saas/github-account/42/subscription-details"

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401

    await web_client.log_as(user.id)

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.json() == {
        "billable_seats": [],
        "billable_seats_count": 0,
        "billing_manager": False,
        "plan": {
            "discontinued": False,
            "features": [
                "private_repository",
                "public_repository",
                "priority_queues",
                "custom_checks",
                "random_request_reviews",
                "merge_bot_account",
                "queue_action",
                "depends_on",
                "show_sponsor",
                "dedicated_worker",
                "advanced_monitoring",
                "queue_freeze",
                "eventlogs_short",
                "eventlogs_long",
                "merge_queue_stats",
            ],
            "name": "OnPremise Premium",
        },
        "role": "member",
        "subscription": None,
    }

    await web_client.logout()
    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401


async def test_saas_intercom_with_saas_mode_true(
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", True)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    unwanted_headers = respx.patterns.M(headers={"dnt": "1"})
    assert unwanted_headers is not None
    respx_mock.route(
        respx.patterns.M(
            method="get",
            url="http://localhost:5000/engine/saas/intercom",
            headers={
                "Authorization": f"Bearer {config.ENGINE_TO_DASHBOARD_API_KEY}",
                "Mergify-On-Behalf-Of": str(user.id),
            },
        )
        & ~unwanted_headers
    ).respond(200, json={"yo": "ya"})

    url = "/front/proxy/saas/intercom"

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401

    await web_client.log_as(user.id)

    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.json() == {"yo": "ya"}

    await web_client.logout()
    resp = await web_client.get(url, headers={"dnt": "1"})
    assert resp.status_code == 401


@pytest.mark.parametrize(
    "url, proxied_url, proxy_location, expected_location",
    (
        (
            "/front/proxy/saas/github-account/42/stripe-customer-portal",
            f"{config.SUBSCRIPTION_BASE_URL}/engine/saas/github-account/42/stripe-customer-portal",
            "https://portal.stripe.com/",
            "https://portal.stripe.com/",
        ),
        (
            "/front/proxy/saas/github-account/42/something-else",
            f"{config.SUBSCRIPTION_BASE_URL}/engine/saas/github-account/42/something-else",
            f"{config.SUBSCRIPTION_BASE_URL}/engine/saas/foo/bar",
            f"{settings.DASHBOARD_UI_FRONT_BASE_URL}/front/proxy/saas/foo/bar",
        ),
    ),
)
async def test_saas_proxy_redirect(
    url: str,
    proxied_url: str,
    proxy_location: str,
    expected_location: str,
    monkeypatch: pytest.MonkeyPatch,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
    front_login_mock: None,
) -> None:
    monkeypatch.setattr(config, "SAAS_MODE", True)

    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("user-login"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    respx_mock.get(
        proxied_url,
        headers={
            "Authorization": f"Bearer {config.ENGINE_TO_DASHBOARD_API_KEY}",
            "Mergify-On-Behalf-Of": str(user.id),
        },
    ).respond(307, headers={"Location": proxy_location})

    await web_client.log_as(user.id)
    resp = await web_client.get(url, follow_redirects=False)
    assert resp.status_code == 307, (resp.text, url)
    assert resp.headers["Location"] == expected_location
