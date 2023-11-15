import typing
from unittest import mock

from mergify_engine.actions import utils
from mergify_engine.tests.unit import conftest


async def test_get_dequeue_reason_from_outcome_outdated_check_run(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    async def fake_merge_queue_check_run_getter() -> dict[str, typing.Any]:
        return {
            "app_avatar_url": "https://avatars.githubusercontent.com/u/123?v=4",
            "app_id": 123,
            "app_name": "Mergify",
            "completed_at": "2023-04-10T20:10:46Z",
            "conclusion": "action_required",
            "external_id": None,
            "head_sha": "123",
            "html_url": "https://github.com/org/repo/runs/123",
            "id": 12642061509,
            "name": "Queue: Embarked in merge train",
            "output": {
                "annotations_count": 0,
                "annotations_url": "https://api.github.com/repos/org/repo/check-runs/123/annotations",
                "summary": "No TrainCarState?",
                "title": "This pull request cannot be embarked for merge",
            },
            "status": "completed",
        }

    context = await context_getter(123)
    with mock.patch.object(
        context,
        "get_merge_queue_check_run",
        fake_merge_queue_check_run_getter,
    ):
        await utils.get_dequeue_reason_from_outcome(context)
