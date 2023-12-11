import typing

from _pytest import fixtures
import pytest
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.models.github import repository as github_repository
from mergify_engine.rules.config import partition_rules
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


def get_request_param_with_defaults(
    pull_request: int,
    received_at: str,
    base_ref: str = "main",
    partition_names: list[str] | None = None,
    queue_name: str = "default",
) -> typing.Any:
    return (
        pull_request,
        received_at,
        base_ref,
        partition_names or ["default"],
        queue_name,
    )


@pytest.fixture()
async def _insert_merged_event(
    request: fixtures.SubRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )

    for row in request.param:
        (
            pull_request,
            received_at,
            base_ref,
            partition_names,
            queue_name,
        ) = get_request_param_with_defaults(
            **row,
        )
        db.add(
            evt_models.EventActionQueueMerged(
                repository=repo,
                pull_request=github_types.GitHubPullRequestNumber(pull_request),
                received_at=received_at,
                base_ref=github_types.GitHubRefType(base_ref),
                partition_names=[
                    partition_rules.PartitionRuleName(p) for p in partition_names
                ],
                queue_name=queue_name,
                queued_at=received_at,
                trigger="Rule: some rule",
                branch="hello world",
            ),
        )
    await db.commit()


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {
                    "pull_request": 1,
                    "received_at": "2023-11-24T00:00:01+00:00",
                    "base_ref": "random_branch",
                },
                {
                    "pull_request": 2,
                    "received_at": "2023-11-24T01:00:00+00:00",
                    "partition_names": ["random_partition"],
                },
                {
                    "pull_request": 3,
                    "received_at": "2023-11-24T19:00:00+00:00",
                    "queue_name": "random_queue",
                },
                {"pull_request": 4, "received_at": "2023-11-24T22:00:00+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_api_simple_filters(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    with time_travel("2023-11-25T00:00:00+00:00"):
        # filter out rows of base_ref="random_branch", partition_name="random_partition"
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/queues_merged_count"
            "?base_ref=main"
            "&partition_name=default"
            "&queue_name=default",
            headers={"Authorization": api_token.api_token},
        )
        assert response.json() == {
            "groups": [
                {
                    "base_ref": "main",
                    "partition_names": ["default"],
                    "queue_name": "default",
                    "stats": [
                        {
                            "start": "2023-11-24T22:00:00Z",
                            "end": "2023-11-24T22:59:59Z",
                            "merged": 1,
                        },
                    ],
                },
            ],
        }


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {
                    "pull_request": 1,
                    "received_at": "2023-11-24T01:01:00+00:00",
                    "partition_names": ["partition_1", "partition_2"],
                },
                {
                    "pull_request": 2,
                    "received_at": "2023-11-24T02:01:00+00:00",
                    "partition_names": ["partition_1", "partition_2"],
                },
                {
                    "pull_request": 3,
                    "received_at": "2023-11-24T01:00:00+00:00",
                    "partition_names": ["partition_2"],
                },
                {
                    "pull_request": 4,
                    "received_at": "2023-11-24T01:00:00+00:00",
                    "partition_names": ["partition_3"],
                },
            ]
        ),
    ],
    indirect=True,
)
async def test_api_partitions_list_filters(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    with time_travel("2023-11-25T00:00:00+00:00"):
        # filter in objects that have "partition_1" and/or "partition_2"
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/queues_merged_count"
            "?partition_name=partition_1&partition_name=partition_2",
            headers={"Authorization": api_token.api_token},
        )
        assert response.json() == {
            "groups": [
                {
                    "base_ref": "main",
                    "partition_names": ["partition_1", "partition_2"],
                    "queue_name": "default",
                    "stats": [
                        {
                            "start": "2023-11-24T01:00:00Z",
                            "end": "2023-11-24T01:59:59Z",
                            "merged": 1,
                        },
                        {
                            "start": "2023-11-24T02:00:00Z",
                            "end": "2023-11-24T02:59:59Z",
                            "merged": 1,
                        },
                    ],
                },
                {
                    "base_ref": "main",
                    "partition_names": ["partition_2"],
                    "queue_name": "default",
                    "stats": [
                        {
                            "start": "2023-11-24T01:00:00Z",
                            "end": "2023-11-24T01:59:59Z",
                            "merged": 1,
                        },
                    ],
                },
            ],
        }


async def test_api_invalid_start_at_end_at(
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
) -> None:
    # provided date in past
    with time_travel("2023-01-01T00:00:00Z"):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2025-11-25T12:00Z",
            headers={"Authorization": api_token.api_token},
        )
        assert response.status_code == 422
        r = response.json()
        assert r["detail"][0]["msg"] == "Value error, Datetime cannot be in the future"
    # provided end_date > start_date
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-11-25T12:00Z&end_at=2022-11-24T12:00Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "provided end_at should be after start_at"}


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {"pull_request": 1, "received_at": "2022-11-25T12:00:00+00:00"},
                {"pull_request": 2, "received_at": "2022-11-25T12:30:00+00:00"},
                {"pull_request": 3, "received_at": "2022-11-26T09:30:00+00:00"},
                {"pull_request": 4, "received_at": "2022-11-26T11:15:00+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_api_response_1_value_per_hour(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    # <= 2 days duration
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-11-25T12:00Z&end_at=2022-11-26T11:15Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_names": ["default"],
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-11-25T12:00:00Z",
                        "end": "2022-11-25T12:59:59Z",
                        "merged": 2,
                    },
                    {
                        "start": "2022-11-26T09:00:00Z",
                        "end": "2022-11-26T09:59:59Z",
                        "merged": 1,
                    },
                    {
                        "start": "2022-11-26T11:00:00Z",
                        "end": "2022-11-26T11:59:59Z",
                        "merged": 1,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {"pull_request": 1, "received_at": "2022-11-25T12:00:00+00:00"},
                {"pull_request": 2, "received_at": "2022-11-25T14:30:00+00:00"},
                {"pull_request": 3, "received_at": "2022-11-26T09:30:00+00:00"},
                {"pull_request": 4, "received_at": "2022-11-30T17:15:00+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_api_response_1_value_per_4_hours(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    # <= 7 days duration
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-11-25T12:00Z&end_at=2022-12-01T12:00Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_names": ["default"],
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-11-25T12:00:00Z",
                        "end": "2022-11-25T15:59:59Z",
                        "merged": 2,
                    },
                    {
                        "start": "2022-11-26T08:00:00Z",
                        "end": "2022-11-26T11:59:59Z",
                        "merged": 1,
                    },
                    {
                        "start": "2022-11-30T16:00:00Z",
                        "end": "2022-11-30T19:59:59Z",
                        "merged": 1,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {"pull_request": 1, "received_at": "2022-11-25T03:00:00+00:00"},
                {"pull_request": 2, "received_at": "2022-11-25T14:30:00+00:00"},
                {"pull_request": 3, "received_at": "2022-12-01T09:30:01+00:00"},
                {"pull_request": 4, "received_at": "2022-12-07T17:15:01+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_api_response_1_value_per_day(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    # <= 60 days duration
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-11-20T12:00Z&end_at=2022-12-25T15:00Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_names": ["default"],
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-11-25T00:00:00Z",
                        "end": "2022-11-25T23:59:59Z",
                        "merged": 2,
                    },
                    {
                        "start": "2022-12-01T00:00:00Z",
                        "end": "2022-12-01T23:59:59Z",
                        "merged": 1,
                    },
                    {
                        "start": "2022-12-07T00:00:00Z",
                        "end": "2022-12-07T23:59:59Z",
                        "merged": 1,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {"pull_request": 1, "received_at": "2022-11-24T03:00:00+00:00"},
                {"pull_request": 2, "received_at": "2022-11-25T14:30:00+00:00"},
                {"pull_request": 3, "received_at": "2022-12-02T09:30:01+00:00"},
                {"pull_request": 4, "received_at": "2023-01-27T17:15:01+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_api_response_1_value_per_2_days(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    # > 60 days duration
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-11-20T12:00Z&end_at=2023-02-01T15:00Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_names": ["default"],
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-11-24T00:00:00Z",
                        "end": "2022-11-25T23:59:59Z",
                        "merged": 2,
                    },
                    {
                        "start": "2022-12-02T00:00:00Z",
                        "end": "2022-12-03T23:59:59Z",
                        "merged": 1,
                    },
                    {
                        "start": "2023-01-27T00:00:00Z",
                        "end": "2023-01-28T23:59:59Z",
                        "merged": 1,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_merged_event",
    [
        (
            [
                {"pull_request": 1, "received_at": "2022-12-04T12:59:59.999999+00:00"},
                {"pull_request": 2, "received_at": "2022-12-04T13:00:00+00:00"},
            ]
        ),
    ],
    indirect=True,
)
async def test_at_intervals_edge(
    fake_repository: context.Repository,
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_merged_event: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queues_merged_count?start_at=2022-12-04T00:00Z&end_at=2022-12-05T00:00Z",
        headers={"Authorization": api_token.api_token},
    )
    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_names": ["default"],
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-12-04T12:00:00Z",
                        "end": "2022-12-04T12:59:59Z",
                        "merged": 1,
                    },
                    {
                        "start": "2022-12-04T13:00:00Z",
                        "end": "2022-12-04T13:59:59Z",
                        "merged": 1,
                    },
                ],
            },
        ],
    }
