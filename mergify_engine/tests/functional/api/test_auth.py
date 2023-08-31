import typing
from unittest import mock

import fastapi
import httpx
import pydantic
import pytest

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine.tests.functional import conftest as func_conftest
from mergify_engine.web.api import security


@pydantic.dataclasses.dataclass
class ResponseTest:
    user_login: github_types.GitHubLogin


@pytest.fixture(autouse=True)
def create_testing_router(web_server: fastapi.FastAPI) -> None:
    router = fastapi.APIRouter()

    @router.get(
        "/testing-endpoint/{owner}/{repository}",
        response_model=ResponseTest,
    )
    async def test_explicit_deps(
        owner: typing.Annotated[
            github_types.GitHubLogin,
            fastapi.Path(description="The owner of the repository"),
        ],
        repository: typing.Annotated[
            github_types.GitHubRepositoryName,
            fastapi.Path(description="The name of the repository"),
        ],
        repository_ctxt: security.Repository,
    ) -> ResponseTest:
        org = await repository_ctxt.installation.client.item(f"/users/{owner}")
        return ResponseTest(org["login"])

    api_app = next(
        r
        for r in web_server.router.routes
        if r.path == "/v1"  # type:ignore[attr-defined]
    ).app
    api_app.include_router(router)


async def test_api_auth_unknown_path(
    web_client: httpx.AsyncClient,
) -> None:
    # Default
    r = await web_client.get("/v1/foobar")
    assert r.status_code == 404
    assert r.json() == {"detail": "Not Found"}


@pytest.mark.recorder
async def test_api_auth_scoped(
    web_client: httpx.AsyncClient,
    shadow_office: func_conftest.SubscriptionFixture,
    recorder: func_conftest.RecorderFixture,
) -> None:
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == recorder.config["organization_name"]

    # check case insensitive
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name'].upper()}/{recorder.config['repository_name'].upper()}",
        headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == recorder.config["organization_name"]


@pytest.mark.recorder
async def test_api_auth_invalid_token(
    web_client: httpx.AsyncClient,
    shadow_office: func_conftest.SubscriptionFixture,
    recorder: func_conftest.RecorderFixture,
) -> None:
    # invalid header
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": "whatever"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}

    # invalid token too short
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": "bearer whatever"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}

    # invalid token good size
    invalid_token = "6" * 64
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": f"bearer {invalid_token}"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}

    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": f"bearer {invalid_token}"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}


@pytest.mark.recorder
async def test_api_auth_no_token(
    web_client: httpx.AsyncClient,
    shadow_office: func_conftest.SubscriptionFixture,
    recorder: func_conftest.RecorderFixture,
) -> None:
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}"
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}


@pytest.mark.recorder
async def test_api_repository_auth_cached(
    web_client: httpx.AsyncClient,
    shadow_office: func_conftest.SubscriptionFixture,
    recorder: func_conftest.RecorderFixture,
    redis_links: redis_utils.RedisLinks,
) -> None:
    # Make sure that a person having access to a repository makes it so that
    # the repository is correctly stored as a dict in redis.
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
    )
    assert r.status_code == 200

    cache_redis_key = security.get_redis_key_for_repo_access_check(
        recorder.config["organization_name"]
    )
    raw_redis_value = await redis_links.cache.hget(
        cache_redis_key,
        f"{recorder.config['organization_name']}/{recorder.config['repository_name']}",
    )
    assert raw_redis_value is not None
    redis_value = json.loads(raw_redis_value)
    assert redis_value["status_code"] == 200

    # Make sure that a person not having access to a repository makes it `False`
    # when stored in redis
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/testbar",
        headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
    )
    assert r.status_code == 404

    raw_redis_value = await redis_links.cache.hget(
        cache_redis_key,
        f"{recorder.config['organization_name']}/testbar",
    )
    assert raw_redis_value is not None
    redis_value = json.loads(raw_redis_value)
    assert redis_value["status_code"] == 404

    # Re-request to use cache
    with mock.patch(
        "mergify_engine.clients.github.AsyncGitHubInstallationClient.item",
        side_effect=RuntimeError("Should not have used the GitHub endpoint"),
    ):
        r = await web_client.get(
            f"/v1/testing-endpoint/{recorder.config['organization_name']}/testbar",
            headers={"Authorization": f"bearer {shadow_office.api_key_admin}"},
        )
        assert r.status_code == 404

    # Make sure that a "installation_repositories" event cleans up the associated
    # repositories of the event.
    # NOTE: This is impossible to test with a real event, so we need to manually send one
    await github_events.event_classifier(
        redis_links,
        "installation_repositories",
        "123eventid",
        # This is a slim event with just the data required to make sure the
        # authentication cache clearing mechanism work.
        github_types.GitHubEventInstallationRepositories(  # type: ignore[typeddict-item]
            {
                "installation": {
                    "account": {
                        "login": recorder.config["organization_name"],
                    }
                },
                "repositories_added": [
                    {
                        "full_name": f"{recorder.config['organization_name']}/testbar",
                    }
                ],
                "repositories_removed": [
                    {
                        "full_name": f"{recorder.config['organization_name']}/{recorder.config['repository_name']}",
                    }
                ],
            }
        ),
        # We don't need the bot infos
        github_types.GitHubAccount({}),  # type: ignore[typeddict-item]
    )

    raw_redis_value = await redis_links.cache.hget(
        cache_redis_key,
        f"{recorder.config['organization_name']}/testbar",
    )
    assert raw_redis_value is None
    raw_redis_value = await redis_links.cache.hget(
        cache_redis_key,
        f"{recorder.config['organization_name']}/{recorder.config['repository_name']}",
    )
    assert raw_redis_value is None
