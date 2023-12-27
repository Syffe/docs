import pytest

from mergify_engine import context
from mergify_engine.tests import conftest as tests_conftest
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit.api import conftest as tests_api_conftest


@pytest.mark.parametrize(
    (
        "_insert_action_queue_leave",
        "_insert_action_checks_end_event",
    ),
    (
        (
            [
                {
                    "pull_request": 1,
                    "queued_at": "2022-11-24T12:15:00+00:00",
                    "received_at": "2022-11-24T12:45:00+00:00",
                    "seconds_waiting_for_freeze": 5 * 60,
                },
                {
                    "pull_request": 2,
                    "queued_at": "2022-11-24T14:10:00+00:00",
                    "received_at": "2022-11-24T14:20:00+00:00",
                    "seconds_waiting_for_freeze": 0,
                },
                {
                    "pull_request": 3,
                    "queued_at": "2022-11-24T14:30:00+00:00",
                    "received_at": "2022-11-24T14:55:00+00:00",
                    "seconds_waiting_for_freeze": 10 * 60,
                },
            ],
            [
                {
                    "pull_request": 1,
                    "checks_started_at": "2022-11-24T12:20:00+00:00",
                    "checks_ended_at": "2022-11-24T12:40:00+00:00",
                },
                {
                    "pull_request": 2,
                    "checks_started_at": "2022-11-24T14:10:00+00:00",
                    "checks_ended_at": "2022-11-24T14:15:00+00:00",
                },
                {
                    "pull_request": 3,
                    "checks_started_at": "2022-11-24T14:35:00+00:00",
                    "checks_ended_at": "2022-11-24T14:45:00+00:00",
                },
            ],
        ),
    ),
    indirect=True,
)
async def test_basic_api_response(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_queue_leave: None,
    _insert_action_checks_end_event: None,
) -> None:
    with time_travel("2022-11-25T00:00:00+00:00"):
        response = await web_client.get(
            "/v1/repos/Mergifyio/engine/stats/average_queue_time",
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
                            "queue_time": (30 - 20 - 5) * 60.0,
                        },
                        {
                            "start": "2022-11-24T14:00:00Z",
                            "end": "2022-11-24T14:59:59Z",
                            "queue_time": ((10 - 5) + (25 - 10 - 10)) / 2 * 60.0,
                        },
                    ],
                },
            ],
        }


@pytest.mark.parametrize(
    (
        "_insert_action_queue_leave",
        "_insert_action_checks_end_event",
    ),
    (
        (
            [
                {
                    "pull_request": 1,
                    "queued_at": "2022-11-24T12:15:00+00:00",
                    "received_at": "2022-11-24T12:45:00+00:00",
                    "base_ref": "random_branch",
                },
                {
                    "pull_request": 2,
                    "queued_at": "2022-11-24T14:10:00+00:00",
                    "received_at": "2022-11-24T14:20:00+00:00",
                    "partition_name": "random_partition",
                },
                {
                    "pull_request": 3,
                    "queued_at": "2022-11-24T14:30:00+00:00",
                    "received_at": "2022-11-24T14:55:00+00:00",
                    "queue_name": "random_queue",
                },
                {
                    "pull_request": 4,
                    "queued_at": "2022-11-24T20:00:00+00:00",
                    "received_at": "2022-11-24T20:15:00+00:00",
                    "seconds_waiting_for_freeze": 300,
                    "base_ref": "selected_branch",
                    "partition_name": "selected_partition",
                    "queue_name": "selected_queue",
                },
            ],
            [
                {
                    "pull_request": 4,
                    "checks_started_at": "2022-11-24T20:00:00+00:00",
                    "checks_ended_at": "2022-11-24T20:05:00+00:00",
                },
            ],
        ),
    ),
    indirect=True,
)
async def test_api_simple_filters(
    fake_repository: context.Repository,  # noqa: ARG001
    web_client: tests_conftest.CustomTestClient,
    api_token: tests_api_conftest.TokenUserRepo,
    _insert_action_queue_leave: None,
    _insert_action_checks_end_event: None,
) -> None:
    response = await web_client.get(
        "/v1/repos/Mergifyio/engine/stats/average_queue_time"
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
                        "start": "2022-11-24T20:00:00Z",
                        "end": "2022-11-24T20:59:59Z",
                        "queue_time": 300.0,
                    },
                ],
            },
        ],
    }
