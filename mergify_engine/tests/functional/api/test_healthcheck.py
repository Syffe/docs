from unittest import mock

import pydantic
import pytest
from redis import exceptions as redis_exceptions
import redis.asyncio as redispy

from mergify_engine import settings
from mergify_engine.tests import conftest


async def test_healthcheck(
    web_client: conftest.CustomTestClient,
    monkeypatch: pytest.MonkeyPatch,
    db: None,  # noqa: ARG001
) -> None:
    r = await web_client.get("/healthcheck")
    assert r.status_code == 404

    monkeypatch.setattr(
        settings,
        "HEALTHCHECK_SHARED_TOKEN",
        pydantic.SecretStr("secret"),
    )
    r = await web_client.get(
        "/healthcheck",
        headers={"Authorization": "bearer wrong-token"},
    )
    assert r.status_code == 403

    r = await web_client.get("/healthcheck", headers={"Authorization": "bearer secret"})
    assert r.status_code == 200
    assert r.json() == {
        "postgres": {
            "status": "ok",
        },
        "redis_cache": {
            "status": "ok",
        },
        "redis_queue": {
            "status": "ok",
        },
        "redis_stream": {
            "status": "ok",
        },
        "redis_active_users": {
            "status": "ok",
        },
        "redis_team_members_cache": {
            "status": "ok",
        },
        "redis_team_permissions_cache": {
            "status": "ok",
        },
        "redis_user_permissions_cache": {
            "status": "ok",
        },
        "redis_stats": {
            "status": "ok",
        },
        "redis_authentication": {
            "status": "ok",
        },
    }
    with mock.patch.object(
        redispy.Redis,
        "ping",
        side_effect=mock.AsyncMock(side_effect=redis_exceptions.TimeoutError),
    ):
        r = await web_client.get(
            "/healthcheck",
            headers={"Authorization": "bearer secret"},
        )
        assert r.status_code == 502
        assert r.json() == {
            "postgres": {
                "status": "ok",
            },
            "redis_cache": {
                "status": "failure",
            },
            "redis_queue": {
                "status": "failure",
            },
            "redis_stream": {
                "status": "failure",
            },
            "redis_active_users": {
                "status": "failure",
            },
            "redis_team_members_cache": {
                "status": "failure",
            },
            "redis_team_permissions_cache": {
                "status": "failure",
            },
            "redis_user_permissions_cache": {
                "status": "failure",
            },
            "redis_stats": {
                "status": "failure",
            },
            "redis_authentication": {
                "status": "failure",
            },
        }
