from collections import abc
import datetime
import re
import urllib.parse

import anys
import freezegun
import httpx
import pytest
import respx
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import date
from mergify_engine import eventlogs
from mergify_engine import events as evt_utils
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.models import events as event_models
from mergify_engine.models import github_repository
from mergify_engine.models import github_user
from mergify_engine.rules.config import partition_rules
from mergify_engine.tests import conftest


INITIAL_TIMESTAMP = datetime.datetime(2023, 9, 5, 0, 0, tzinfo=date.UTC)
DB_SWITCHED_TIMESTAMP = (
    datetime.datetime(2023, 9, 5, 0, 0, tzinfo=date.UTC)
    + eventlogs.EVENTLOGS_LONG_RETENTION
    + datetime.timedelta(hours=1)
)


@pytest.fixture
async def switched_to_pg(
    monkeypatch: pytest.MonkeyPatch, redis_links: redis_utils.RedisLinks
) -> abc.AsyncGenerator[None, None]:
    await redis_links.cache.set(eventlogs.DB_SWITCH_KEY, INITIAL_TIMESTAMP.timestamp())
    with freezegun.freeze_time(DB_SWITCHED_TIMESTAMP):
        yield


@pytest.fixture
async def api_token(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    respx_mock: respx.MockRouter,
) -> str:
    user = github_user.GitHubUser(
        id=github_types.GitHubAccountIdType(42),
        login=github_types.GitHubLogin("Mergifyio"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    # Mock different GitHub responses
    # get installation from login
    gh_owner = github_types.GitHubAccount(
        id=github_types.GitHubAccountIdType(0),
        login=github_types.GitHubLogin("Mergifyio"),
        type="User",
        avatar_url="",
    )
    respx_mock.get("https://api.github.com/users/Mergifyio/installation").respond(
        200, json={"account": gh_owner, "id": 42}
    )

    # get the repository
    respx_mock.get("https://api.github.com/repos/Mergifyio/engine").respond(
        200,
        json=github_types.GitHubRepository(  # type: ignore[arg-type]
            {
                "id": github_types.GitHubRepositoryIdType(0),
                "private": False,
                "archived": False,
                "name": github_types.GitHubRepositoryName("engine"),
                "full_name": "Mergifyio/engine",
                "url": "",
                "html_url": "",
                "default_branch": github_types.GitHubRefType("main"),
                "owner": gh_owner,
            }
        ),
    )

    # get membership for the auth user
    respx_mock.get("https://api.github.com/user/memberships/orgs/0").respond(
        status_code=200,
        json={
            "state": "active",
            "role": "admin",
            "user": {"id": user.id, "login": user.login},
            "organization": {"id": 42, "login": "Mergifyio"},
        },
    )

    # get a github access token
    respx_mock.post(
        "https://api.github.com/app/installations/42/access_tokens"
    ).respond(
        200,
        json=github_types.GitHubInstallationAccessToken(
            {
                "token": "gh_token",
                "expires_at": "2111-09-08T17:26:27Z",
            }
        ),  # type: ignore[arg-type]
    )

    # NOTE(sileht): We don't care if access token is used ot not,
    # so call it once to please respx.assert_all_called()
    httpx.post(
        "https://api.github.com/app/installations/42/access_tokens", json={"foo": "bar"}
    )

    # get account subscription to Mergify
    respx_mock.get(
        f"http://localhost:5000/engine/subscription/{gh_owner['id']}"
    ).respond(
        200,
        json={
            "subscription_active": True,
            "subscription_reason": "",
            "features": [f.value for f in subscription.Features],
        },
    )

    # 2) Create app and get an access token
    await web_client.log_as(user.id)
    logged_as = await web_client.logged_as()
    assert logged_as == "Mergifyio"

    # create application key
    resp = await web_client.post(
        f"/front/github-account/{gh_owner['id']}/applications",
        json={"name": "Mergifyio"},
    )

    data = resp.json()
    return f"bearer {data['api_access_key']}{data['api_secret_key']}"


MAIN_TIMESTAMP = datetime.datetime.fromisoformat("2023-08-22T10:00:00+00:00")
LATER_TIMESTAMP = datetime.datetime.fromisoformat("2022-08-22T12:00:00+00:00")


@pytest.fixture
async def insert_data(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    # add events manually instead of evt_utils.insert() to set a mocked timestamp
    repo = await github_repository.GitHubRepository.get_or_create(
        db, fake_repository.repo
    )
    db.add(
        event_models.EventActionComment(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            received_at=MAIN_TIMESTAMP,
            trigger="Rule: some rule",
            message="hello world",
        )
    )
    db.add(
        event_models.EventActionQueueEnter(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            received_at=LATER_TIMESTAMP,
            trigger="Rule: some other rule",
            **signals.EventQueueEnterMetadata(
                {
                    "queue_name": "default",
                    "branch": "refactor_test",
                    "position": 3,
                    "queued_at": date.utcnow(),
                    "partition_name": partition_rules.PartitionRuleName(
                        "default_partition"
                    ),
                }
            ),
        )
    )
    db.add(
        event_models.EventActionMerge(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(2),
            received_at=MAIN_TIMESTAMP,
            trigger="Rule: some other rule",
            branch="merge_branch",
        )
    )
    await db.commit()


async def test_api_response(
    web_client: conftest.CustomTestClient,
    switched_to_pg: None,
    api_token: str,
    insert_data: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=1",
        headers={"Authorization": api_token},
    )
    assert response.json() == {
        "size": 1,
        "per_page": 1,
        "total": 3,
        "events": [
            {
                "id": 3,
                "received_at": "2023-08-22T10:00:00Z",
                "timestamp": "2023-08-22T10:00:00Z",
                "trigger": "Rule: some other rule",
                "repository": "mergify-engine",
                "pull_request": 2,
                "event": "action.merge",
                "type": "action.merge",
                "metadata": {"branch": "merge_branch"},
            }
        ],
    }


async def test_api_query_params(
    web_client: conftest.CustomTestClient,
    switched_to_pg: None,
    api_token: str,
    insert_data: None,
) -> None:
    # pull_request qp
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?pull_request=1",
        headers={"Authorization": api_token},
    )
    r = response.json()
    assert r["total"] == 3
    assert r["size"] == 2
    assert {e["type"] for e in r["events"]} == {"action.comment", "action.queue.enter"}

    # event_type qp
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?event_type=action.comment&event_type=action.merge",
        headers={"Authorization": api_token},
    )
    assert response.json()["size"] == 2

    # received_from and received_to qp
    received_from = (MAIN_TIMESTAMP - datetime.timedelta(minutes=5)).timestamp()
    received_to = (MAIN_TIMESTAMP + datetime.timedelta(minutes=5)).timestamp()
    response = await web_client.get(
        f"/v1/repos/Mergifyio/engine/logs?received_from={received_from}&received_to={received_to}",
        headers={"Authorization": api_token},
    )
    r = response.json()
    assert r["size"] == 2
    assert {e["type"] for e in r["events"]} == {"action.comment", "action.merge"}


def parse_links(links: str) -> dict[str, str]:
    link_re = re.compile(r'<(.*)>; rel="(\w+)"')
    links_dict: dict[str, str] = {}
    for l_ in links.split(","):
        match = link_re.match(l_)
        assert match is not None
        links_dict.update({match.group(2): match.group(1)})
    return links_dict


async def test_api_cursor(
    fake_repository: context.Repository,
    web_client: conftest.CustomTestClient,
    switched_to_pg: None,
    api_token: str,
) -> None:
    for _ in range(6):
        await evt_utils.insert(
            event="action.assign",
            repository=fake_repository.repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            metadata=signals.EventAssignMetadata(
                {
                    "added": ["leo", "charly", "guillaume"],
                    "removed": ["damien", "fabien"],
                }
            ),
            trigger="Rule: some dummmy rule",
        )

    # first 2
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2",
        headers={"Authorization": api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [6, 5]
    links = parse_links(response.headers["link"])
    assert links["next"].split("?")[-1] == "per_page=2&cursor=%2B5"

    # first 2 to 4
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=%2B5",
        headers={"Authorization": api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [4, 3]
    links = parse_links(response.headers["link"])
    assert links["next"].split("?")[-1] == "per_page=2&cursor=%2B3"
    assert links["prev"].split("?")[-1] == "per_page=2&cursor=-4"

    # last 2 with initial last cursor
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=-",
        headers={"Authorization": api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [2, 1]
    links = parse_links(response.headers["link"])
    assert links["next"].split("?")[-1] == "per_page=2&cursor=-2"

    # last 2 to last 4
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=-2",
        headers={"Authorization": api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [4, 3]
    links = parse_links(response.headers["link"])
    assert links["next"].split("?")[-1] == "per_page=2&cursor=-4"
    assert links["prev"].split("?")[-1] == "per_page=2&cursor=%2B3"


async def test_api_links_with_query_params(
    fake_repository: context.Repository,
    web_client: conftest.CustomTestClient,
    api_token: str,
) -> None:
    query_params = {
        ("per_page", "2"),
        ("pull_request", "1"),
        ("event_type", "action.assign"),
        ("event_type", "action.comment"),
        ("received_from", "2023-09-15T07:24:11.715105+00:00"),
    }

    response = await web_client.get(
        f"/v1/repos/Mergifyio/engine/logs?{urllib.parse.urlencode(list(query_params), doseq=True)}",
        headers={"Authorization": api_token},
    )
    links = parse_links(response.headers["link"])
    assert set(urllib.parse.parse_qsl(links["first"].split("?")[-1])) == query_params


async def test_api_cursor_invalid(
    web_client: conftest.CustomTestClient,
    switched_to_pg: None,
    api_token: str,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=INVALID_CURSOR",
        headers={"Authorization": api_token},
    )
    assert response.status_code == 422
    assert response.json() == {"message": "Invalid cursor", "cursor": "INVALID_CURSOR"}

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=+123456_this_part_is_unexpected",
        headers={"Authorization": api_token},
    )
    assert response.status_code == 422

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=-123456_neither_is_this_part",
        headers={"Authorization": api_token},
    )
    assert response.status_code == 422

    # + alone (like any other char alone except for -) is not valid it is equivalent
    # to default - not provided
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2&cursor=+",
        headers={"Authorization": api_token},
    )
    assert response.status_code == 422


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
async def test_new_api_db_switch(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    fake_repository: context.Repository,
    web_client: conftest.CustomTestClient,
    api_token: str,
) -> None:
    monkeypatch.setattr(settings, "EVENTLOG_EVENTS_DB_INGESTION", False)

    timedelta = datetime.timedelta()
    for i in range(6):
        # add redis entries
        with freezegun.freeze_time(INITIAL_TIMESTAMP + timedelta):
            await signals.send(
                repository=fake_repository,
                pull_request=github_types.GitHubPullRequestNumber(1),
                event="action.comment",
                metadata=signals.EventCommentMetadata(message=""),
                trigger=f"redis_evt_{i}",
            )
        timedelta += datetime.timedelta(hours=1)
    # add a postgres entries
    await evt_utils.insert(
        event="action.comment",
        repository=fake_repository.repo,
        pull_request=github_types.GitHubPullRequestNumber(1),
        metadata=signals.EventCommentMetadata(message=""),
        trigger="pg_evt",
    )

    with freezegun.freeze_time(INITIAL_TIMESTAMP, tick=True):
        # first
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/logs?per_page=2",
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert {e["trigger"] for e in response.json()["events"]} == {
            "redis_evt_5",
            "redis_evt_4",
        }
        next_link = parse_links(response.headers["link"])["next"]

        # next
        response = await web_client.get(
            next_link,
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert {e["trigger"] for e in response.json()["events"]} == {
            "redis_evt_3",
            "redis_evt_2",
        }
        prev_link = parse_links(response.headers["link"])["prev"]

        # prev
        response = await web_client.get(
            prev_link,
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert {e["trigger"] for e in response.json()["events"]} == {
            "redis_evt_5",
            "redis_evt_4",
        }

        # event type filtering
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/logs?event_type=action.assign",
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert response.json()["total"] == 0

        # received_at and received_from
        rfrom = (INITIAL_TIMESTAMP + datetime.timedelta(hours=1, minutes=1)).timestamp()
        rto = (INITIAL_TIMESTAMP + datetime.timedelta(hours=4, minutes=1)).timestamp()
        response = await web_client.get(
            f"/v1/repos/Mergifyio/engine/logs?received_from={rfrom}&received_to={rto}",
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert response.json()["total"] == 3

    # switched to postgres
    with freezegun.freeze_time(
        INITIAL_TIMESTAMP
        + eventlogs.EVENTLOGS_LONG_RETENTION
        + datetime.timedelta(hours=1)
    ):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/logs",
            headers={"Authorization": api_token},
        )
        assert response is not None
        assert response.json()["events"][0]["trigger"] == "pg_evt"


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
async def test_old_api_db_switch(
    monkeypatch: pytest.MonkeyPatch,
    redis_links: redis_utils.RedisLinks,
    fake_repository: context.Repository,
    web_client: conftest.CustomTestClient,
    api_token: str,
) -> None:
    await redis_links.cache.set(eventlogs.DB_SWITCH_KEY, INITIAL_TIMESTAMP.timestamp())

    await evt_utils.insert(
        event="action.comment",
        repository=fake_repository.repo,
        pull_request=github_types.GitHubPullRequestNumber(1),
        metadata=signals.EventCommentMetadata(message=""),
        trigger="pg_evt",
    )

    # test one redis event
    monkeypatch.setattr(settings, "EVENTLOG_EVENTS_DB_INGESTION", False)
    with freezegun.freeze_time(INITIAL_TIMESTAMP + datetime.timedelta(minutes=1)):
        await signals.send(
            repository=fake_repository,
            pull_request=github_types.GitHubPullRequestNumber(1),
            event="action.comment",
            metadata=signals.EventCommentMetadata(message=""),
            trigger="redis_evt",
        )

        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/events",
            headers={"Authorization": api_token},
        )
        assert response.json() == {
            "size": 1,
            "per_page": 10,
            "total": 1,
            "events": [
                {
                    "id": anys.ANY_INT,
                    "received_at": anys.ANY_DATETIME_STR,
                    "timestamp": anys.ANY_DATETIME_STR,
                    "trigger": "redis_evt",
                    "repository": "Mergifyio/mergify-engine",
                    "pull_request": 1,
                    "event": "action.comment",
                    "type": "action.comment",
                    "metadata": {"message": ""},
                }
            ],
        }
        assert response.json()["total"] == 1

    # passed the eventlogs TTL the old API will use the postgreSQL backend
    with freezegun.freeze_time(
        INITIAL_TIMESTAMP
        + eventlogs.EVENTLOGS_LONG_RETENTION
        + datetime.timedelta(hours=1)
    ):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/events",
            headers={"Authorization": api_token},
        )
        assert response is not None
        payload = response.json()
        assert payload == {
            "size": 1,
            "per_page": 10,
            "total": 1,
            "events": [
                {
                    "id": 1,
                    "timestamp": anys.ANY_DATETIME_STR,
                    "received_at": anys.ANY_DATETIME_STR,
                    "trigger": "pg_evt",
                    "repository": "mergify-engine",
                    "pull_request": 1,
                    "event": "action.comment",
                    "type": "action.comment",
                    "metadata": {"message": ""},
                }
            ],
        }

        # test the second endpoint (pull request path param)
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/pulls/1/events",
            headers={"Authorization": api_token},
        )
        assert response.json()["total"] == 1
