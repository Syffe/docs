import asyncio

import pytest

from mergify_engine import cli
from mergify_engine.tests.functional import conftest as func_conftest


@pytest.mark.recorder
def test_clear_token_cache(
    dashboard: func_conftest.DashboardFixture,
    monkeypatch: pytest.MonkeyPatch,
    recorder: func_conftest.RecorderFixture,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    monkeypatch.setattr("asyncio.run", lambda coro: event_loop.run_until_complete(coro))
    monkeypatch.setattr(
        "sys.argv",
        ["mergify-clear-token-cache", str(recorder.config["organization_id"])],
    )
    cli.clear_token_cache()


@pytest.mark.recorder
def test_refresher(
    dashboard: func_conftest.DashboardFixture,
    recorder: func_conftest.RecorderFixture,
    monkeypatch: pytest.MonkeyPatch,
    event_loop: asyncio.BaseEventLoop,
) -> None:
    monkeypatch.setattr("asyncio.run", lambda coro: event_loop.run_until_complete(coro))
    repo = f"{recorder.config['organization_name']}/{recorder.config['repository_name']}/branch/main"
    monkeypatch.setattr("sys.argv", ["mergify-refresher", "--action=admin", repo])
    cli.refresher_cli()
