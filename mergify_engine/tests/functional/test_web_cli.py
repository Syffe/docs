import asyncio

import httpx
import pytest

from mergify_engine import web_cli
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
def test_clear_token_cache(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
    monkeypatch: pytest.MonkeyPatch,
    recorder: func_conftest.RecorderFixture,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    async def __aenter__(self: httpx.AsyncClient) -> httpx.AsyncClient:
        self._transport = web_client._transport
        return self

    monkeypatch.setattr("asyncio.run", lambda coro: event_loop.run_until_complete(coro))
    monkeypatch.setattr("mergify_engine.web_cli.config.BASE_URL", "http://localhost")
    monkeypatch.setattr(
        "mergify_engine.web_cli.http.AsyncClient.__aenter__", __aenter__
    )
    monkeypatch.setattr(
        "sys.argv",
        ["mergify-clear-token-cache", str(recorder.config["organization_id"])],
    )
    web_cli.clear_token_cache()


@pytest.mark.recorder
def test_refresher(
    web_client: httpx.AsyncClient,
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
    monkeypatch: pytest.MonkeyPatch,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    async def __aenter__(self: httpx.AsyncClient) -> httpx.AsyncClient:
        self._transport = web_client._transport
        return self

    monkeypatch.setattr("asyncio.run", lambda coro: event_loop.run_until_complete(coro))
    monkeypatch.setattr("mergify_engine.web_cli.config.BASE_URL", "http://localhost")
    monkeypatch.setattr(
        "mergify_engine.web_cli.http.AsyncClient.__aenter__", __aenter__
    )
    repo = f"{recorder.config['organization_name']}/{recorder.config['repository_name']}/branch/main"
    monkeypatch.setattr("sys.argv", ["mergify-refresher", "--action=admin", repo])
    web_cli.refresher()
