import random
import typing

from _pytest import fixtures
import pytest
import sqlalchemy

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.models import events as evt_models
from mergify_engine.models.github import repository as github_repository
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


def get_request_param_evt_queue_change_defaults(
    received_at: str,
    size: int = 0,
    base_ref: str = "main",
    partition_name: str = "default",
    queue_name: str = "default",
) -> typing.Any:
    return {
        "received_at": received_at,
        "size": size,
        "base_ref": base_ref,
        "partition_name": partition_name,
        "queue_name": queue_name,
    }


@pytest.fixture()
async def _insert_action_queue_change(
    request: fixtures.SubRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    fake_repository: context.Repository,
) -> None:
    repo = await github_repository.GitHubRepository.get_or_create(
        db,
        fake_repository.repo,
    )

    for row in request.param:
        defaults = get_request_param_evt_queue_change_defaults(**row)

        db.add(
            evt_models.EventActionQueueChange(
                repository=repo,
                received_at=defaults["received_at"],
                size=defaults["size"],
                pull_request=github_types.GitHubPullRequestNumber(
                    random.randint(1, 100),
                ),
                base_ref=github_types.GitHubRefType(defaults["base_ref"]),
                trigger="Rule: some rule",
                partition_name=defaults["partition_name"],
                queue_name=defaults["queue_name"],
                running_checks=0,
            ),
        )

    await db.commit()


@pytest.mark.parametrize(
    "_insert_action_queue_change",
    [
        (
            (
                {
                    "size": 0,
                    "received_at": "2022-11-24T12:45:00+00:00",
                },
                {
                    "size": 1,
                    "received_at": "2022-11-24T14:20:00+00:00",
                },
                {
                    "size": 2,
                    "received_at": "2022-11-24T14:55:00+00:00",
                },
                {
                    "size": 0,
                    "received_at": "2022-11-24T20:15:00+00:00",
                },
                {
                    "size": 5,
                    "received_at": "2022-11-24T20:30:00+00:00",
                },
            )
        ),
    ],
    indirect=True,
)
async def test_basic_api_response(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_queue_change: None,
) -> None:
    with time_travel("2022-11-25T00:00:00+00:00"):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/queue_size",
            headers={"Authorization": api_token.api_token},
        )

    assert response.json() == {
        "groups": [
            {
                "base_ref": "main",
                "partition_name": "default",
                "queue_name": "default",
                "stats": [
                    {
                        "start": "2022-11-24T12:00:00Z",
                        "end": "2022-11-24T12:59:59Z",
                        "avg_size": 0.0,
                        "max_size": 0,
                        "min_size": 0,
                    },
                    {
                        "start": "2022-11-24T14:00:00Z",
                        "end": "2022-11-24T14:59:59Z",
                        "avg_size": 1.5,
                        "max_size": 2,
                        "min_size": 1,
                    },
                    {
                        "start": "2022-11-24T20:00:00Z",
                        "end": "2022-11-24T20:59:59Z",
                        "avg_size": 2.5,
                        "max_size": 5,
                        "min_size": 0,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_action_queue_change",
    [
        (
            [
                {
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "base_ref": "random_branch",
                },
                {
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "partition_name": "random_partition",
                },
                {
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "queue_name": "random_queue",
                },
                {
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "size": 1,
                    "base_ref": "selected_branch",
                    "partition_name": "selected_partition",
                    "queue_name": "selected_queue",
                },
            ]
        ),
    ],
    indirect=True,
)
async def test_api_simple_filters(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_queue_change: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/queue_size"
        "?start_at=2022-11-24T00:00Z&end_at=2022-11-25T00:00Z"
        "&base_ref=selected_branch"
        "&partition_name=selected_partition"
        "&queue_name=selected_queue",
        headers={"Authorization": api_token.api_token},
    )

    assert response.json() == {
        "groups": [
            {
                "base_ref": "selected_branch",
                "partition_name": "selected_partition",
                "queue_name": "selected_queue",
                "stats": [
                    {
                        "start": "2022-11-24T12:00:00Z",
                        "end": "2022-11-24T12:59:59Z",
                        "avg_size": 1.0,
                        "max_size": 1,
                        "min_size": 1,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_action_queue_change",
    [
        (
            (
                {
                    "size": 1,
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "queue_name": "main",
                },
                {
                    "size": 1,
                    "received_at": "2022-11-24T12:00:00+00:00",
                    "queue_name": "lowprio",
                },
                {
                    "size": 2,
                    "received_at": "2022-11-24T13:00:00+00:00",
                    "queue_name": "main",
                },
                {
                    "size": 2,
                    "received_at": "2022-11-24T13:00:00+00:00",
                    "queue_name": "lowprio",
                },
            )
        ),
    ],
    indirect=True,
)
async def test_api_grouping(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_queue_change: None,
) -> None:
    with time_travel("2022-11-25T00:00:00+00:00"):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/queue_size",
            headers={"Authorization": api_token.api_token},
        )

        assert response.json() == {
            "groups": [
                {
                    "base_ref": "main",
                    "partition_name": "default",
                    "queue_name": "lowprio",
                    "stats": [
                        {
                            "start": "2022-11-24T12:00:00Z",
                            "end": "2022-11-24T12:59:59Z",
                            "avg_size": 1.0,
                            "max_size": 1,
                            "min_size": 1,
                        },
                        {
                            "start": "2022-11-24T13:00:00Z",
                            "end": "2022-11-24T13:59:59Z",
                            "avg_size": 2.0,
                            "max_size": 2,
                            "min_size": 2,
                        },
                    ],
                },
                {
                    "base_ref": "main",
                    "partition_name": "default",
                    "queue_name": "main",
                    "stats": [
                        {
                            "start": "2022-11-24T12:00:00Z",
                            "end": "2022-11-24T12:59:59Z",
                            "avg_size": 1.0,
                            "max_size": 1,
                            "min_size": 1,
                        },
                        {
                            "start": "2022-11-24T13:00:00Z",
                            "end": "2022-11-24T13:59:59Z",
                            "avg_size": 2.0,
                            "max_size": 2,
                            "min_size": 2,
                        },
                    ],
                },
            ],
        }
