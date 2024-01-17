import datetime
import json
import pathlib
from unittest import mock

import anys
import httpx
import msgpack
import pydantic
import pytest
import respx
import sqlalchemy.ext.asyncio

from mergify_engine import count_seats
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine.models import active_user
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit import conftest


@pytest.fixture(autouse=True)
def _disable_active_users_tracking() -> None:
    # This disables the conftest fixtures
    return


def test_seats_renamed_account_repo() -> None:
    user1 = count_seats.SeatAccount(
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("user1"),
    )
    user1bis = count_seats.SeatAccount(
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("user1bis"),
    )
    user2 = count_seats.SeatAccount(
        github_types.GitHubAccountIdType(456),
        github_types.GitHubLogin("user2"),
    )
    user2bis = count_seats.SeatAccount(
        github_types.GitHubAccountIdType(456),
        github_types.GitHubLogin("user2bis"),
    )

    users = {user1, user2, user2bis, user1bis}
    assert len(users) == 2
    users_iterator = iter(users)
    assert next(users_iterator).login == "user2"
    assert next(users_iterator).login == "user1"

    repo1 = count_seats.SeatRepository(
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo1"),
    )
    repo1bis = count_seats.SeatRepository(
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo1bis"),
    )
    repo2 = count_seats.SeatRepository(
        github_types.GitHubRepositoryIdType(456),
        github_types.GitHubRepositoryName("repo2"),
    )
    repo2bis = count_seats.SeatRepository(
        github_types.GitHubRepositoryIdType(456),
        github_types.GitHubRepositoryName("repo2bis"),
    )

    repos = {repo1, repo2, repo2bis, repo1bis}
    assert repos == {repo1, repo2}


async def test_send_report(
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SUBSCRIPTION_TOKEN", pydantic.SecretStr("something"))
    route = respx_mock.post(
        f"{settings.SUBSCRIPTION_URL}/on-premise/report",
        json={"active_users": 2, "engine_version": "dev"},
    ).respond(201, content="Accepted")

    await count_seats.send_report(count_seats.SeatsCountResult(2))

    assert route.call_count == 1


GITHUB_SAMPLE_EVENTS = {}
_EVENT_DIR = pathlib.Path(__file__).parent / "events"
for file in _EVENT_DIR.iterdir():
    event_type = file.name.split(".")[0]
    with file.open() as event:
        GITHUB_SAMPLE_EVENTS[file.name] = (event_type, json.load(event))


@time_travel("2011-11-11")
@pytest.mark.parametrize(("event_type", "event"), list(GITHUB_SAMPLE_EVENTS.values()))
async def test_store_active_users(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    event_type: str,
    event: github_types.GitHubEvent,
    redis_links: redis_utils.RedisLinks,
) -> None:
    await count_seats.store_active_users(
        redis_links.active_users,
        event_type,
        "whatever",
        event,
    )
    one_month_ago = date.utcnow() - datetime.timedelta(days=30)
    if event_type == "push":
        assert await redis_links.active_users.zrangebyscore(
            "active-users~21031067~Codertocat~186853002~Hello-World",
            min=one_month_ago.timestamp(),
            max="+inf",
            withscores=True,
        ) == [
            (b"21031067~Codertocat", 1320969600.0),
        ]

        stmt = sqlalchemy.select(active_user.ActiveUser)
        result = await db.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1
        assert users[0].user_github_account_id == 21031067
        assert users[0].repository_id == 186853002
        assert users[0].last_seen_at == date.fromtimestamp(1320969600.0)

    elif event_type == "pull_request":
        assert await redis_links.active_users.zrangebyscore(
            "active-users~21031067~Codertocat~186853002~Hello-World",
            min=one_month_ago.timestamp(),
            max="+inf",
            withscores=True,
        ) == [
            (b"12345678~AnotherUser", 1320969600.0),
            (b"21031067~Codertocat", 1320969600.0),
        ]
        assert msgpack.unpackb(
            await redis_links.active_users.get(
                "active-users-events~21031067~186853002~12345678",
            ),
        ) == {
            "action": "opened",
            "received_at": mock.ANY,
            "delivery_id": "whatever",
            "sender": {"id": 21031067, "login": "Codertocat", "type": "User"},
        }

        stmt = sqlalchemy.select(active_user.ActiveUser)
        result = await db.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 2
        assert users[0].user_github_account_id == 12345678
        assert users[0].repository_id == 186853002
        assert users[0].last_seen_at == date.fromtimestamp(1320969600.0)
        assert users[1].user_github_account_id == 21031067
        assert users[1].repository_id == 186853002
        assert users[1].last_seen_at == date.fromtimestamp(1320969600.0)
        assert users[1].last_event == {
            "action": "opened",
            "received_at": mock.ANY,
            "delivery_id": "whatever",
            "sender": {"id": 21031067, "login": "Codertocat", "type": "User"},
        }

    else:
        assert (
            await redis_links.active_users.zrangebyscore(
                "active-users~21031067~Codertocat~186853002~Hello-World",
                min=one_month_ago.timestamp(),
                max="+inf",
            )
            == []
        )

        stmt = sqlalchemy.select(active_user.ActiveUser)
        result = await db.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 0


@time_travel("2011-11-11")
@pytest.mark.parametrize(("event_type", "event"), list(GITHUB_SAMPLE_EVENTS.values()))
async def test_get_usage_count_seats(
    db: sqlalchemy.ext.asyncio.AsyncSession,  # noqa: ARG001
    web_client: httpx.AsyncClient,
    event_type: str,
    event: github_types.GitHubEvent,
    redis_links: redis_utils.RedisLinks,
) -> None:
    await count_seats.store_active_users(
        redis_links.active_users,
        event_type,
        "whatever",
        event,
    )

    reply = await web_client.request("GET", "/subscriptions/organization/1234/usage")
    assert reply.status_code == 403

    web_client.headers[
        "Authorization"
    ] = f"Bearer {settings.SHADOW_OFFICE_TO_ENGINE_API_KEY.get_secret_value()}"
    web_client.headers["Content-Type"] = "application/json; charset=utf8"
    reply = await web_client.request("GET", "/subscriptions/organization/1234/usage")
    assert reply.status_code == 200, reply.content
    assert json.loads(reply.content) == {"repositories": [], "last_seen_at": None}

    reply = await web_client.request(
        "GET",
        "/subscriptions/organization/21031067/usage",
    )
    assert reply.status_code == 200, reply.content
    if event_type == "pull_request":
        assert json.loads(reply.content) == {
            "repositories": [
                {
                    "collaborators": {
                        "active_users": [
                            {
                                "id": 21031067,
                                "login": "Codertocat",
                                "seen_at": anys.ANY_AWARE_DATETIME_STR,
                            },
                            {
                                "id": 12345678,
                                "login": "AnotherUser",
                                "seen_at": anys.ANY_AWARE_DATETIME_STR,
                            },
                        ],
                    },
                    "id": 186853002,
                    "name": "Hello-World",
                },
            ],
            "last_seen_at": None,
        }
    elif event_type == "push":
        assert json.loads(reply.content) == {
            "repositories": [
                {
                    "collaborators": {
                        "active_users": [
                            {
                                "id": 21031067,
                                "login": "Codertocat",
                                "seen_at": anys.ANY_AWARE_DATETIME_STR,
                            },
                        ],
                    },
                    "id": 186853002,
                    "name": "Hello-World",
                },
            ],
            "last_seen_at": None,
        }

    else:
        assert json.loads(reply.content) == {
            "repositories": [],
            "last_seen_at": None,
        }


@time_travel("2011-11-11")
async def test_get_usage_last_seen(
    context_getter: conftest.ContextGetterFixture,
    web_client: httpx.AsyncClient,
    _setup_database: None,
) -> None:
    ctxt = await context_getter(number=1)

    signals.register()

    reply = await web_client.request("GET", "/subscriptions/organization/0/usage")
    assert reply.status_code == 403, reply.content

    web_client.headers[
        "Authorization"
    ] = f"Bearer {settings.SHADOW_OFFICE_TO_ENGINE_API_KEY.get_secret_value()}"
    web_client.headers["Content-Type"] = "application/json; charset=utf8"

    reply = await web_client.request("GET", "/subscriptions/organization/0/usage")
    assert reply.status_code == 200, reply.content
    assert json.loads(reply.content) == {"repositories": [], "last_seen_at": None}

    await signals.send(
        ctxt.repository,
        ctxt.pull["number"],
        ctxt.pull["base"]["ref"],
        "action.refresh",
        signals.EventNoMetadata(),
        "Rule: testing",
    )

    reply = await web_client.request("GET", "/subscriptions/organization/0/usage")
    assert reply.status_code == 200, reply.content
    assert json.loads(reply.content) == {
        "repositories": [],
        "last_seen_at": "2011-11-11T00:00:00+00:00",
    }
