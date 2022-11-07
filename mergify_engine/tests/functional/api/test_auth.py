import fastapi
import httpx
import pydantic
import pytest

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine.clients import github
from mergify_engine.tests.functional import conftest as func_conftest
from mergify_engine.web import root as web_root
from mergify_engine.web.api import security


@pydantic.dataclasses.dataclass
class ResponseTest:
    user_login: github_types.GitHubLogin


@pytest.fixture(scope="module", autouse=True)
def create_testing_router() -> None:
    router = fastapi.APIRouter()

    @router.get("/testing-endpoint-with-explicit-dep", response_model=ResponseTest)
    async def test_explicit_deps(
        installation: github_types.GitHubInstallation = fastapi.Depends(  # noqa: B008
            security.get_installation
        ),
    ) -> ResponseTest:
        async with github.aget_client(installation) as client:
            org = await client.item(f"/user/{installation['account']['id']}")
            return ResponseTest(org["login"])

    @router.get(
        "/testing-endpoint-with-owner/{owner}",  # noqa: FS003
        response_model=ResponseTest,
    )
    async def test_owner(
        installation: github_types.GitHubInstallation = fastapi.Depends(  # noqa: B008
            security.get_installation
        ),
    ) -> ResponseTest:
        async with github.aget_client(installation) as client:
            org = await client.item(f"/user/{installation['account']['id']}")
            return ResponseTest(org["login"])

    api_app = [
        r
        for r in web_root.app.router.routes
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
async def test_api_auth(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
) -> None:
    r = await web_client.get(
        "/v1/testing-endpoint-with-explicit-dep",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == config.TESTING_ORGANIZATION_NAME


@pytest.mark.recorder
async def test_api_auth_scoped(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
) -> None:

    r = await web_client.get(
        f"/v1/testing-endpoint-with-owner/{config.TESTING_ORGANIZATION_NAME}",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == config.TESTING_ORGANIZATION_NAME

    # check case insensitive
    r = await web_client.get(
        f"/v1/testing-endpoint-with-owner/{config.TESTING_ORGANIZATION_NAME.upper()}",
        headers={"Authorization": f"bearer {dashboard.api_key_admin}"},
    )
    assert r.status_code == 200, r.json()
    assert r.json()["user_login"] == config.TESTING_ORGANIZATION_NAME


@pytest.mark.recorder
async def test_api_auth_invalid_token(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
) -> None:
    # invalid header
    r = await web_client.get(
        "/v1/testing-endpoint-with-explicit-dep",
        headers={"Authorization": "whatever"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Not authenticated"}

    # invalid token too short
    r = await web_client.get(
        "/v1/testing-endpoint-with-explicit-dep",
        headers={"Authorization": "bearer whatever"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}

    # invalid token good size
    invalid_token = "6" * 64
    r = await web_client.get(
        "/v1/foobar/", headers={"Authorization": f"bearer {invalid_token}"}
    )
    assert r.status_code == 404
    assert r.json() == {"detail": "Not Found"}
    r = await web_client.get(
        "/v1/testing-endpoint-with-explicit-dep",
        headers={"Authorization": f"bearer {invalid_token}"},
    )
    assert r.status_code == 403
    assert r.json() == {"detail": "Forbidden"}


async def test_api_auth_no_token(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
) -> None:
    r = await web_client.get("/v1/testing-endpoint-with-explicit-dep")
    assert r.status_code == 403
    assert r.json() == {"detail": "Not authenticated"}
