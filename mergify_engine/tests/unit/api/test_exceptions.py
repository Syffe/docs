import datetime
import typing

import fastapi
import httpx
import pytest
import starlette

from mergify_engine import exceptions
from mergify_engine import pagination
from mergify_engine.tests.functional.api import test_auth
from mergify_engine.web import root as web_root


@pytest.fixture(autouse=True)
def endpoint_with_testing_router(web_server: fastapi.FastAPI) -> None:
    router = fastapi.APIRouter()

    @router.get(
        "/testing-endpoint-exception-rate-limited",
        response_model=test_auth.ResponseTest,
    )
    async def test_exception_rate_limited() -> None:
        raise exceptions.RateLimited(
            datetime.timedelta(seconds=622, microseconds=280475),
            0,
        )

    @router.get(
        "/testing-endpoint-pagination-invalid-cursor",
        response_model=test_auth.ResponseTest,
    )
    async def test_exception_pagination() -> None:
        raise pagination.InvalidCursor(cursor="abcdef")

    @router.get(
        "/testing-endpoint-exception-mergify-not-installed",
        response_model=test_auth.ResponseTest,
    )
    async def test_exception_mergify_not_installed() -> None:
        raise exceptions.MergifyNotInstalled()

    for route in web_server.router.routes:
        if isinstance(route, starlette.routing.Mount):
            app = typing.cast(fastapi.FastAPI, route.app)
            app.include_router(router)
            for _ in range(len(router.routes)):
                app.router.routes.insert(0, app.router.routes.pop(-1))


def generate_endpoints() -> list[str]:
    app = web_root.create_app()
    endpoints = [
        route.path
        for route in app.router.routes
        if isinstance(route, starlette.routing.Mount) and route.path
    ]
    endpoints.insert(0, "")
    assert len(endpoints) == 5
    return endpoints


@pytest.mark.parametrize("endpoint", generate_endpoints())
async def test_handler_exception_rate_limited(
    web_client: httpx.AsyncClient,
    endpoint: str,
) -> None:
    r = await web_client.get(f"{ endpoint }/testing-endpoint-exception-rate-limited")
    assert r.status_code == 403, r.json()
    assert r.json()["message"] == "Organization or user has hit GitHub API rate limit"


@pytest.mark.parametrize("endpoint", generate_endpoints())
async def test_handler_exception_mergify_not_installed(
    web_client: httpx.AsyncClient,
    endpoint: str,
) -> None:
    r = await web_client.get(
        f"{ endpoint }/testing-endpoint-exception-mergify-not-installed",
    )
    assert r.status_code == 403, r.json()
    assert (
        r.json()["message"]
        == "Mergify is not installed or suspended on this organization or repository"
    )


@pytest.mark.parametrize("endpoint", generate_endpoints())
async def test_handler_pagination_invalid_cursor(
    web_client: httpx.AsyncClient,
    endpoint: str,
) -> None:
    r = await web_client.get(f"{ endpoint }/testing-endpoint-pagination-invalid-cursor")
    assert r.status_code == 422, r.json()
    assert r.json()["message"] == "Invalid cursor"
    assert r.json()["cursor"] == "abcdef"
