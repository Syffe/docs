import datetime

import fastapi
import httpx
import pytest

from mergify_engine import exceptions
from mergify_engine import pagination
from mergify_engine.tests.functional.api import test_auth


@pytest.fixture(autouse=True)
def create_testing_router(web_server: fastapi.FastAPI) -> None:
    router = fastapi.APIRouter()

    @router.get(
        "/testing-endpoint-exception-rate-limited",
        response_model=test_auth.ResponseTest,
    )
    async def test_exception_rate_limited() -> None:
        raise exceptions.RateLimited(
            datetime.timedelta(seconds=622, microseconds=280475), 0
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

    api_app = [
        r
        for r in web_server.router.routes
        if r.path == "/v1"  # type: ignore[attr-defined]
    ][0].app
    api_app.include_router(router)
    web_server.include_router(router)


async def test_handler_exception_rate_limited(
    web_client: httpx.AsyncClient,
) -> None:

    endpoints = ["/", "/v1/"]

    for endpoint in endpoints:
        r = await web_client.get(f"{ endpoint }testing-endpoint-exception-rate-limited")
        assert r.status_code == 403, r.json()
        assert (
            r.json()["message"] == "Organization or user has hit GitHub API rate limit"
        )


async def test_handler_exception_mergify_not_installed(
    web_client: httpx.AsyncClient,
) -> None:

    endpoints = ["/", "/v1/"]

    for endpoint in endpoints:
        r = await web_client.get(
            f"{ endpoint }testing-endpoint-exception-mergify-not-installed"
        )
        assert r.status_code == 403, r.json()
        assert (
            r.json()["message"]
            == "Mergify is not installed or suspended on this organization or repository"
        )


async def test_handler_pagination_invalid_cursor(
    web_client: httpx.AsyncClient,
) -> None:

    endpoints = ["/", "/v1/"]

    for endpoint in endpoints:
        r = await web_client.get(
            f"{ endpoint }testing-endpoint-pagination-invalid-cursor"
        )
        assert r.status_code == 422, r.json()
        assert r.json()["message"] == "Invalid cursor"
        assert r.json()["cursor"] == "abcdef"
