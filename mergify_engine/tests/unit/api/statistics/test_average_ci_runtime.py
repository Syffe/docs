import pytest

from mergify_engine import context
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


@pytest.mark.parametrize(
    "_insert_action_checks_end_event",
    [
        (
            [
                {
                    "pull_request": 1,
                    "checks_started_at": "2022-11-24T12:00:00+00:00",
                    "checks_ended_at": "2022-11-24T12:00:30+00:00",
                },
                {
                    "pull_request": 2,
                    "checks_started_at": "2022-11-24T12:00:30+00:00",
                    "checks_ended_at": "2022-11-24T12:00:40+00:00",
                },
                {
                    "pull_request": 3,
                    "checks_started_at": "2022-11-24T13:00:00+00:00",
                    "checks_ended_at": "2022-11-24T13:10:00+00:00",
                },
                {
                    "pull_request": 4,
                    "checks_started_at": "2022-11-24T13:10:00+00:00",
                    "checks_ended_at": "2022-11-24T13:30:00+00:00",
                },
            ]
        ),
    ],
    indirect=True,
)
async def test_basic_api_response(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_checks_end_event: None,
) -> None:
    with time_travel("2022-11-25T00:00:00+00:00"):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/average_ci_runtime",
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
                        "runtime": (30 + 10) / 2,
                    },
                    {
                        "start": "2022-11-24T13:00:00Z",
                        "end": "2022-11-24T13:59:59Z",
                        "runtime": (10 * 60 + 20 * 60) / 2,
                    },
                ],
            },
        ],
    }


@pytest.mark.parametrize(
    "_insert_action_checks_end_event",
    [
        (
            [
                {
                    "pull_request": 1,
                    "checks_started_at": "2022-11-24T12:00:00+00:00",
                    "checks_ended_at": "2022-11-24T12:00:30+00:00",
                    "base_ref": "random_branch",
                },
                {
                    "pull_request": 2,
                    "checks_started_at": "2022-11-24T12:00:00+00:00",
                    "checks_ended_at": "2022-11-24T12:00:30+00:00",
                    "partition_name": "random_partition",
                },
                {
                    "pull_request": 3,
                    "checks_started_at": "2022-11-24T12:00:00+00:00",
                    "checks_ended_at": "2022-11-24T12:00:30+00:00",
                    "queue_name": "random_queue",
                },
                {
                    "pull_request": 4,
                    "checks_started_at": "2022-11-24T12:00:00+00:00",
                    "checks_ended_at": "2022-11-24T12:00:30+00:00",
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
    _insert_action_checks_end_event: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/average_ci_runtime"
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
                        "runtime": 30.0,
                    },
                ],
            },
        ],
    }
