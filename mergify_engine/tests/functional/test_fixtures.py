import httpx
import pytest

from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.tests.functional import conftest as func_conftest


async def test_fixture_web_client(
    web_client: httpx.AsyncClient,
) -> None:
    r = await web_client.get("/foobar")
    assert r.status_code == 404


@pytest.mark.recorder()
async def test_fixture_recorder() -> None:
    async with github.AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
        r = await client.get("/app")
        assert r.status_code == 200
        assert r.json()["owner"]["id"] == settings.TESTING_ORGANIZATION_ID
        assert r.json()["owner"]["login"] == settings.TESTING_ORGANIZATION_NAME


async def test_fixture_shadow_office(
    shadow_office: func_conftest.SubscriptionFixture,
) -> None:
    assert shadow_office.subscription.features == frozenset(
        {
            subscription.Features.PUBLIC_REPOSITORY,
        },
    )
