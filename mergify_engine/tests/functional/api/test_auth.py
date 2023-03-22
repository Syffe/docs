import typing

import fastapi
import httpx
import pydantic
import pytest

from mergify_engine import github_types
from mergify_engine.tests.functional import conftest as func_conftest
from mergify_engine.web.api import security


@pydantic.dataclasses.dataclass
class ResponseTest:
    user_login: github_types.GitHubLogin


@pytest.fixture(autouse=True)
def create_testing_router(web_server: fastapi.FastAPI) -> None:
    router = fastapi.APIRouter()

    @router.get(
        "/testing-endpoint/{owner}/{repository}",  # noqa: FS003
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

    api_app = [
        r
        for r in web_server.router.routes
        if r.path == "/v1"  # type:ignore[attr-defined]
    ][0].app
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
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
) -> None:
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == recorder.config["organization_name"]

    # check case insensitive
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name'].upper()}/{recorder.config['repository_name'].upper()}",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == recorder.config["organization_name"]


@pytest.mark.recorder
async def test_api_auth_invalid_token(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
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
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
) -> None:
    r = await web_client.get(
        f"/v1/testing-endpoint/{recorder.config['organization_name']}/{recorder.config['repository_name']}"
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}
