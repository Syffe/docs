import datetime
import re
import urllib.parse

import anys
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import eventlogs
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import signals
from mergify_engine.models import events as event_models
from mergify_engine.models.github import repository as github_repository
from mergify_engine.pagination import Cursor
from mergify_engine.queue.merge_train import checks
from mergify_engine.rules.config import partition_rules
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


MAIN_TIMESTAMP = datetime.datetime.fromisoformat("2023-08-22T10:00:00+00:00")
LATER_TIMESTAMP = datetime.datetime.fromisoformat("2023-08-22T12:00:00+00:00")


@pytest.fixture()
async def _insert_data(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    # add events manually instead of eventlogs.insert() to set a mocked timestamp
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )
    db.add(
        event_models.EventActionComment(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            base_ref="main",
            received_at=MAIN_TIMESTAMP,
            trigger="Rule: some rule",
            message="hello world",
        ),
    )
    db.add(
        event_models.EventActionQueueEnter(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            base_ref="stable",
            received_at=LATER_TIMESTAMP,
            trigger="Rule: some other rule",
            **signals.EventQueueEnterMetadata(
                {
                    "queue_name": "default",
                    "branch": "refactor_test",
                    "position": 3,
                    "queued_at": date.utcnow(),
                    "partition_name": partition_rules.PartitionRuleName(
                        "default_partition",
                    ),
                },
            ),
        ),
    )
    db.add(
        event_models.EventActionMerge(
            repository=repo,
            pull_request=github_types.GitHubPullRequestNumber(2),
            base_ref="stable",
            received_at=MAIN_TIMESTAMP,
            trigger="Rule: some other rule",
            branch="merge_branch",
        ),
    )
    await db.commit()


async def test_api_response(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_data: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=1",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "size": 1,
        "per_page": 1,
        "events": [
            {
                "id": 3,
                "received_at": "2023-08-22T10:00:00Z",
                "trigger": "Rule: some other rule",
                "repository": "Mergifyio/mergify-engine",
                "pull_request": 2,
                "base_ref": "stable",
                "type": "action.merge",
                "metadata": {"branch": "merge_branch"},
            },
        ],
    }

    await eventlogs.insert(
        "action.queue.checks_end",
        fake_repository.repo,
        pull_request=None,
        base_ref=github_types.GitHubRefType("feature_branch"),
        trigger="whatever",
        metadata=signals.EventQueueChecksEndMetadata(
            {
                "branch": "feature_branch",
                "partition_name": partition_rules.DEFAULT_PARTITION_NAME,
                "position": 3,
                "queue_name": "default",
                "queued_at": date.utcnow(),
                "aborted": True,
                "abort_code": "PR_DEQUEUED",
                "abort_reason": "Pull request has been dequeued.",
                "abort_status": "DEFINITIVE",
                "speculative_check_pull_request": {
                    "number": 456,
                    "in_place": True,
                    "checks_timed_out": False,
                    "checks_conclusion": "pending",
                    "checks_started_at": date.utcnow(),
                    "checks_ended_at": date.utcnow(),
                    "unsuccessful_checks": [
                        checks.QueueCheck.Serialized(
                            {
                                "name": "ruff",
                                "description": "Syntax check",
                                "state": "failure",
                                "url": None,
                                "avatar_url": "some_url",
                            },
                        ),
                    ],
                },
            },
        ),
    )

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=1&event_type=action.queue.checks_end",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "size": 1,
        "per_page": 1,
        "events": [
            {
                "id": 4,
                "received_at": anys.ANY_DATETIME_STR,
                "trigger": "whatever",
                "repository": "Mergifyio/mergify-engine",
                "pull_request": None,
                "base_ref": "feature_branch",
                "type": "action.queue.checks_end",
                "metadata": {
                    "aborted": True,
                    "abort_code": "PR_DEQUEUED",
                    "abort_reason": "Pull request has been dequeued.",
                    "abort_status": "DEFINITIVE",
                    "branch": "feature_branch",
                    "partition_name": "__default__",
                    "position": 3,
                    "queue_name": "default",
                    "queued_at": anys.ANY_DATETIME_STR,
                    "speculative_check_pull_request": {
                        "number": 456,
                        "in_place": True,
                        "checks_timed_out": False,
                        "checks_conclusion": "pending",
                        "checks_started_at": anys.ANY_DATETIME_STR,
                        "checks_ended_at": anys.ANY_DATETIME_STR,
                        "unsuccessful_checks": [
                            {
                                "name": "ruff",
                                "description": "Syntax check",
                                "url": None,
                                "state": "failure",
                                "avatar_url": "some_url",
                            },
                        ],
                    },
                },
            },
        ],
    }

    await eventlogs.insert(
        "queue.pause.create",
        fake_repository.repo,
        pull_request=None,
        base_ref=github_types.GitHubRefType("main"),
        trigger="whatever",
        metadata=signals.EventQueuePauseCreateMetadata(
            {
                "reason": "Incident in production",
                "created_by": {"id": 145, "type": "user", "name": "vegeta"},
            },
        ),
    )

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=1&event_type=queue.pause.create",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "size": 1,
        "per_page": 1,
        "events": [
            {
                "id": 5,
                "received_at": anys.ANY_DATETIME_STR,
                "trigger": "whatever",
                "repository": "Mergifyio/mergify-engine",
                "pull_request": None,
                "base_ref": "main",
                "type": "queue.pause.create",
                "metadata": {
                    "reason": "Incident in production",
                    "created_by": {"type": "user", "id": 145, "name": "vegeta"},
                },
            },
        ],
    }

    await eventlogs.insert(
        "action.review",
        fake_repository.repo,
        pull_request=None,
        base_ref=github_types.GitHubRefType("main"),
        trigger="whatever",
        metadata=signals.EventReviewMetadata(
            {
                "review_type": "APPROVE",
                "reviewer": "John Doe",
                "message": "Looks good to me",
            },
        ),
    )

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=1&event_type=action.review",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "size": 1,
        "per_page": 1,
        "events": [
            {
                "id": 6,
                "received_at": anys.ANY_DATETIME_STR,
                "trigger": "whatever",
                "repository": "Mergifyio/mergify-engine",
                "pull_request": None,
                "base_ref": "main",
                "type": "action.review",
                "metadata": {
                    "review_type": "APPROVE",
                    "reviewer": "John Doe",
                    "message": "Looks good to me",
                },
            },
        ],
    }


async def test_api_query_params(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_data: None,
) -> None:
    # pull_request qp
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?pull_request=1",
        headers={"Authorization": api_token.api_token},
    )
    r = response.json()
    assert r["size"] == 2
    assert {e["type"] for e in r["events"]} == {"action.comment", "action.queue.enter"}

    # base_ref qp
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?base_ref=stable",
        headers={"Authorization": api_token.api_token},
    )
    r = response.json()
    assert r["size"] == 2
    assert {e["type"] for e in r["events"]} == {"action.queue.enter", "action.merge"}

    # event_type qp
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?event_type=action.comment&event_type=action.merge",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json()["size"] == 2

    # received_from and received_to qp
    received_from = (MAIN_TIMESTAMP - datetime.timedelta(minutes=5)).timestamp()
    received_to = (MAIN_TIMESTAMP + datetime.timedelta(minutes=5)).timestamp()
    response = await web_client.get(
        f"/v1/repos/Mergifyio/engine/logs?received_from={received_from}&received_to={received_to}",
        headers={"Authorization": api_token.api_token},
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


async def test_api_cursor_pg(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    for _ in range(6):
        await eventlogs.insert(
            event="action.assign",
            repository=fake_repository.repo,
            pull_request=github_types.GitHubPullRequestNumber(1),
            base_ref=github_types.GitHubRefType("main"),
            metadata=signals.EventAssignMetadata(
                {
                    "added": ["leo", "charly", "guillaume"],
                    "removed": ["damien", "fabien"],
                },
            ),
            trigger="Rule: some dummmy rule",
        )

    # first 2
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs?per_page=2",
        headers={"Authorization": api_token.api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [6, 5]
    links = parse_links(response.headers["link"])

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["next"]).query)
    assert param["per_page"] == ["2"]
    next_cursor = Cursor.from_string(param["cursor"][0])
    assert next_cursor.value(pagination.CursorType[int]) == 5
    assert next_cursor.forward

    # first 2 to 4
    response = await web_client.get(
        links["next"],
        headers={"Authorization": api_token.api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [4, 3]
    links = parse_links(response.headers["link"])

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["next"]).query)
    assert param["per_page"] == ["2"]
    next_cursor = Cursor.from_string(param["cursor"][0])
    assert next_cursor.value(pagination.CursorType[int]) == 3
    assert next_cursor.forward

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["prev"]).query)
    assert param["per_page"] == ["2"]
    prev_cursor = Cursor.from_string(param["cursor"][0])
    assert prev_cursor.value(pagination.CursorType[int]) == 4
    assert prev_cursor.backward

    # last 2 with initial last cursor
    response = await web_client.get(
        links["last"],
        headers={"Authorization": api_token.api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [2, 1]
    links = parse_links(response.headers["link"])

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["next"]).query)
    assert param["per_page"] == ["2"]
    next_cursor = Cursor.from_string(param["cursor"][0])
    assert next_cursor.value(pagination.CursorType[int]) == 2
    assert next_cursor.backward

    # last 2 to last 4
    response = await web_client.get(
        links["next"],
        headers={"Authorization": api_token.api_token},
    )
    resp = response.json()
    assert [r["id"] for r in resp["events"]] == [4, 3]
    links = parse_links(response.headers["link"])

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["next"]).query)
    assert param["per_page"] == ["2"]
    next_cursor = Cursor.from_string(param["cursor"][0])
    assert next_cursor.value(pagination.CursorType[int]) == 4
    assert next_cursor.backward

    param = urllib.parse.parse_qs(urllib.parse.urlparse(links["prev"]).query)
    assert param["per_page"] == ["2"]
    prev_cursor = Cursor.from_string(param["cursor"][0])
    assert prev_cursor.value(pagination.CursorType[int]) == 3
    assert prev_cursor.forward

    # Invalid cursor
    invalid_cursor = Cursor("zerzer", forward=True).to_string()
    response = await web_client.get(
        f"/v1/repos/Mergifyio/engine/logs?per_page=2&cursor={invalid_cursor}",
        headers={"Authorization": api_token.api_token},
    )
    assert response.status_code == 422
    assert response.json() == {
        "message": "Invalid cursor",
        "cursor": invalid_cursor,
    }


async def test_api_links_with_query_params(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
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
        headers={"Authorization": api_token.api_token},
    )
    links = parse_links(response.headers["link"])
    assert len(links) == 4
    for link in links.values():
        parsed_params = urllib.parse.parse_qsl(urllib.parse.urlparse(link).query)
        parsed_params_without_cursor = {
            param for param in parsed_params if param[0] != "cursor"
        }
        assert parsed_params_without_cursor == query_params


@time_travel(LATER_TIMESTAMP)
async def test_delete_outdated_events(
    monkeypatch: pytest.MonkeyPatch,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_data: None,
) -> None:
    monkeypatch.setattr(
        database,
        "CLIENT_DATA_RETENTION_TIME",
        datetime.timedelta(hours=1),
    )

    await eventlogs.delete_outdated_events()

    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/logs",
        headers={"Authorization": api_token.api_token},
    )
    r = response.json()
    assert r["size"] == 1
    assert {e["type"] for e in r["events"]} == {"action.queue.enter"}
