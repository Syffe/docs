# -*- encoding: utf-8 -*-
#
# Copyright © 2021 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import datetime

import fastapi
import httpx
import pytest

from mergify_engine import exceptions
from mergify_engine import pagination
from mergify_engine.tests.functional.api import test_auth
from mergify_engine.web import root as web_root
from mergify_engine.web.api import root as api_root


@pytest.fixture(scope="module", autouse=True)
def create_testing_router() -> None:
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

    api_root.app.include_router(router)
    web_root.app.include_router(router)


async def test_handler_exception_rate_limited(
    mergify_web_client: httpx.AsyncClient,
) -> None:

    endpoints = ["/", "/v1/"]

    for endpoint in endpoints:
        r = await mergify_web_client.get(
            f"{ endpoint }testing-endpoint-exception-rate-limited"
        )
        assert r.status_code == 403, r.json()
        assert (
            r.json()["message"] == "Organization or user has hit GitHub API rate limit"
        )


async def test_handler_pagination_invalid_cursor(
    mergify_web_client: httpx.AsyncClient,
) -> None:

    endpoints = ["/", "/v1/"]

    for endpoint in endpoints:
        r = await mergify_web_client.get(
            f"{ endpoint }testing-endpoint-pagination-invalid-cursor"
        )
        assert r.status_code == 422, r.json()
        assert r.json()["message"] == "Invalid cursor"
        assert r.json()["cursor"] == "abcdef"
