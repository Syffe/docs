import datetime
from unittest import mock

from freezegun import freeze_time
import pytest
import voluptuous

from mergify_engine import branch_updater
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import queue
from mergify_engine import rules
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.unit import conftest
from mergify_engine.tests.unit.merge_train import conftest as mt_conftest


UNQUEUE_REASON_DEQUEUED = queue_utils.PrDequeued(123, "whatever")


async def test_train_inplace_with_speculative_checks_out_of_date(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "inplace")

    await t.add_pull(await context_getter(12345), config, "")
    await t.add_pull(await context_getter(54321), config, "")
    with mock.patch.object(
        merge_train.TrainCar,
        "can_be_checked_inplace",
        side_effect=mock.AsyncMock(return_value=False),
    ):
        await t.refresh()
    assert [[12345], [12345, 54321]] == mt_conftest.get_train_cars_content(t)
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )


async def test_train_inplace_with_speculative_checks_up_to_date(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "inplace")

    await t.add_pull(await context_getter(12345), config, "")
    await t.add_pull(await context_getter(54321), config, "")
    with mock.patch.object(
        merge_train.TrainCar,
        "can_be_checked_inplace",
        side_effect=mock.AsyncMock(side_effect=[True, False]),
    ):
        await t.refresh()
    assert [[12345], [12345, 54321]] == mt_conftest.get_train_cars_content(t)
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.INPLACE
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )


async def test_train_add_pull(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1")

    await t.add_pull(await context_getter(1), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)

    await t.add_pull(await context_getter(2), config, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)

    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_remove_middle_merged(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1")
    await t.add_pull(await context_getter(1), config, "")
    await t.add_pull(await context_getter(2), config, "")
    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    # Merged by someone else
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_remove_middle_not_merged(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", 100),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_remove_head_not_merged(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1")

    await t.add_pull(await context_getter(1), config, "")
    await t.add_pull(await context_getter(2), config, "")
    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(1), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[2], [2, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_remove_head_merged(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1")

    await t.add_pull(await context_getter(1), config, "")
    await t.add_pull(await context_getter(2), config, "")
    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(1),
        "",
        queue_utils.PrMerged(1, github_types.SHAType("new_sha1")),
    )
    await t.refresh()
    assert [[1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_add_remove_pull_idempotent(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", priority=0)

    await t.add_pull(await context_getter(1), config, "")
    await t.add_pull(await context_getter(2), config, "")
    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", priority=10)

    await t.add_pull(await context_getter(1), config, "")
    await t.refresh()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [[1], [1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [[1], [1, 3]] == mt_conftest.get_train_cars_content(t)


async def test_train_multiple_queue(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config_two = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "2x1", priority=0
    )
    config_five = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "5x1", priority=0
    )

    await t.add_pull(await context_getter(1), config_two, "")
    await t.add_pull(await context_getter(2), config_two, "")
    await t.add_pull(await context_getter(3), config_five, "")
    await t.add_pull(await context_getter(4), config_five, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3, 4] == mt_conftest.get_train_waiting_pulls_content(t)

    # Ensure we don't got over the train_size
    await t.add_pull(await context_getter(5), config_two, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [5, 3, 4] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(6), config_five, "")
    await t.add_pull(await context_getter(7), config_five, "")
    await t.add_pull(await context_getter(8), config_five, "")
    await t.add_pull(await context_getter(9), config_five, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [5, 3, 4, 6, 7, 8, 9] == mt_conftest.get_train_waiting_pulls_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [5, 3, 4, 6, 7, 8, 9] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1], [1, 5]] == mt_conftest.get_train_cars_content(
        t
    ), f"{mt_conftest.get_train_cars_content(t)} {mt_conftest.get_train_waiting_pulls_content(t)}"
    assert [3, 4, 6, 7, 8, 9] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(1), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(5), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [
        [3],
        [3, 4],
        [3, 4, 6],
        [3, 4, 6, 7],
        [3, 4, 6, 7, 8],
    ] == mt_conftest.get_train_cars_content(t)
    assert [9] == mt_conftest.get_train_waiting_pulls_content(t)

    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()
    assert [
        [3],
        [3, 4],
        [3, 4, 6],
        [3, 4, 6, 7],
        [3, 4, 6, 7, 8],
    ] == mt_conftest.get_train_cars_content(t)
    assert [9] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_remove_duplicates(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(4),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3, 4] == mt_conftest.get_train_waiting_pulls_content(t)

    # Insert bugs in queue
    t._waiting_pulls.extend(
        [
            merge_train.EmbarkedPull(
                t,
                t._cars[0].still_queued_embarked_pulls[0].user_pull_request_number,
                t._cars[0].still_queued_embarked_pulls[0].config,
                t._cars[0].still_queued_embarked_pulls[0].queued_at,
            ),
            t._waiting_pulls[0],
        ]
    )
    t._cars = t._cars + t._cars
    assert [[1], [1, 2], [1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [1, 3, 3, 4] == mt_conftest.get_train_waiting_pulls_content(t)

    # Everything should be back to normal
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3, 4] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_remove_end_wp(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2, 3] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(3), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_remove_first_wp(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2, 3] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(2), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_remove_last_cars(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2, 3] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(1), "", UNQUEUE_REASON_DEQUEUED
    )
    await t.refresh()
    assert [[2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_with_speculative_checks_decreased(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", 1000)
    await t.add_pull(await context_getter(1), config, "")

    await t.add_pull(await context_getter(2), config, "")
    await t.add_pull(await context_getter(3), config, "")
    await t.add_pull(await context_getter(4), config, "")
    await t.add_pull(await context_getter(5), config, "")

    await t.refresh()
    assert [
        [1],
        [1, 2],
        [1, 2, 3],
        [1, 2, 3, 4],
        [1, 2, 3, 4, 5],
    ] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.remove_pull(
        github_types.GitHubPullRequestNumber(1),
        "",
        queue_utils.PrMerged(1, github_types.SHAType("new_sha1")),
    )

    t.convoy.queue_rules[qr_config.QueueName("5x1")].config["speculative_checks"] = 2

    await t.refresh()
    assert [[1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)
    assert [4, 5] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_queue_config_change(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    t.convoy.queue_rules = voluptuous.Schema(qr_config.QueueRulesSchema)(
        rules.YamlSchema(
            """
queue_rules:
  - name: 2x1
    merge_conditions: []
    speculative_checks: 1
"""
        )["queue_rules"]
    )
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2, 3] == mt_conftest.get_train_waiting_pulls_content(t)


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_config_deleted(
    report_failure: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    t.convoy.queue_rules = voluptuous.Schema(qr_config.QueueRulesSchema)(
        rules.YamlSchema(
            """
queue_rules:
  - name: five
    merge_conditions: []
    speculative_checks: 5
"""
        )["queue_rules"]
    )
    await t.refresh()
    assert [] == mt_conftest.get_train_cars_content(t)
    assert [1, 2, 3] == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(report_failure.mock_calls) == 1


async def test_train_priority_change(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )
    await t.add_pull(
        await context_getter(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 1000),
        "",
    )

    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    assert (
        t._cars[0].still_queued_embarked_pulls[0].config["effective_priority"]
        == mt_conftest.QUEUE_RULES["2x1"].config["priority"]
        * queue.QUEUE_PRIORITY_OFFSET
        + 1000
    )

    # NOTE(sileht): pull request got requeued with new configuration that don't
    # update the position but update the prio
    await t.add_pull(
        await context_getter(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1", 2000),
        "",
    )
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    assert (
        t._cars[0].still_queued_embarked_pulls[0].config["effective_priority"]
        == mt_conftest.QUEUE_RULES["2x1"].config["priority"]
        * queue.QUEUE_PRIORITY_OFFSET
        + 2000
    )


def test_train_batch_split(
    convoy: merge_train.Convoy,
) -> None:
    now = datetime.datetime.utcnow()
    t = merge_train.Train(convoy)
    p1_two = merge_train.EmbarkedPull(
        t,
        github_types.GitHubPullRequestNumber(1),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1"),
        now,
    )
    p2_two = merge_train.EmbarkedPull(
        t,
        github_types.GitHubPullRequestNumber(2),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1"),
        now,
    )
    p3_two = merge_train.EmbarkedPull(
        t,
        github_types.GitHubPullRequestNumber(3),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x1"),
        now,
    )
    p4_five = merge_train.EmbarkedPull(
        t,
        github_types.GitHubPullRequestNumber(4),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x1"),
        now,
    )

    assert ([p1_two], [p2_two, p3_two, p4_five]) == t._get_next_batch(
        [p1_two, p2_two, p3_two, p4_five], "2x1", 1
    )
    assert ([p1_two, p2_two], [p3_two, p4_five]) == t._get_next_batch(
        [p1_two, p2_two, p3_two, p4_five], "2x1", 2
    )
    assert ([p1_two, p2_two, p3_two], [p4_five]) == t._get_next_batch(
        [p1_two, p2_two, p3_two, p4_five], "2x1", 10
    )
    assert ([], [p1_two, p2_two, p3_two, p4_five]) == t._get_next_batch(
        [p1_two, p2_two, p3_two, p4_five], "5x1", 10
    )


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_splitted_on_failure_1x2(
    report_failure: mock.Mock,
    fake_client: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 43):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x2", 1000),
            "",
        )
    for i in range(6, 20):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x2", 1000),
            "",
        )

    await t.refresh()
    assert [[41, 42]] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    assert [[41, 42]] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    await t.test_helper_load_from_redis()
    await t.refresh()
    assert [
        [41],
        [41, 42],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [41] as failed
    t._cars[1].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(41), "", UNQUEUE_REASON_DEQUEUED
    )

    # It's 41 fault, we restart the train on 42
    await t.refresh()
    assert [[42, 6]] == mt_conftest.get_train_cars_content(t)
    assert list(range(7, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_splitted_on_failure_1x5(
    report_failure: mock.Mock,
    fake_client: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 46):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "1x5", 1000),
            "",
        )
    for i in range(6, 20):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "1x5", 1000),
            "",
        )

    await t.refresh()
    assert [[41, 42, 43, 44, 45]] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    assert [[41, 42, 43, 44, 45]] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    await t.test_helper_load_from_redis()
    await t.refresh()
    assert [
        [41, 42],
        [41, 42, 43, 44],
        [41, 42, 43, 44, 45],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert t._cars[1].train_car_state.checks_type is None
    assert (
        t._cars[2].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [43+44] as failed
    t._cars[1].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()

    # nothing should move yet as we don't known yet if [41+42] is broken or not
    await t.refresh()
    assert [
        [41, 42],
        [41, 42, 43, 44],
        [41, 42, 43, 44, 45],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert t._cars[1].train_car_state.checks_type is None
    assert (
        t._cars[2].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [41+42] as ready and merge it
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    await t.save()
    fake_client.update_base_sha("sha41")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(41),
        "",
        queue_utils.PrMerged(41, github_types.SHAType("sha41")),
    )
    fake_client.update_base_sha("sha42")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(42),
        "",
        queue_utils.PrMerged(42, github_types.SHAType("sha42")),
    )

    # [43+44] fail, so it's not 45, but is it 43 or 44?
    await t.refresh()
    assert [
        [41, 42, 43],
        [41, 42, 43, 44],
    ] == mt_conftest.get_train_cars_content(t)
    assert [45, *list(range(6, 20))] == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 2
    assert len(t._cars[1].failure_history) == 1
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert t._cars[1].train_car_state.checks_type is None

    # mark [43] as failure
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(43), "", UNQUEUE_REASON_DEQUEUED
    )

    # Train got cut after 43, and we restart from the begining
    await t.refresh()
    assert [[44, 45, 6, 7, 8]] == mt_conftest.get_train_cars_content(t)
    assert list(range(9, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_splitted_on_failure_2x5(
    report_failure: mock.Mock,
    fake_client: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 46):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x5", 1000),
            "",
        )
    for i in range(6, 20):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x5", 1000),
            "",
        )

    await t.refresh()
    assert [
        [41, 42, 43, 44, 45],
        [41, 42, 43, 44, 45, 6, 7, 8, 9, 10],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(11, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    assert [
        [41, 42, 43, 44, 45],
        [41, 42, 43, 44, 45, 6, 7, 8, 9, 10],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(11, 20)) == mt_conftest.get_train_waiting_pulls_content(t)

    await t.test_helper_load_from_redis()
    await t.refresh()
    assert [
        [41, 42],
        [41, 42, 43, 44],
        [41, 42, 43, 44, 45],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[2].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [43+44] as failed
    t._cars[1].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()

    # nothing should move yet as we don't known yet if [41+42] is broken or not
    await t.refresh()
    assert [
        [41, 42],
        [41, 42, 43, 44],
        [41, 42, 43, 44, 45],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(6, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[2].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [41+42] as ready and merge it
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    await t.save()
    fake_client.update_base_sha("sha41")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(41),
        "",
        queue_utils.PrMerged(41, github_types.SHAType("sha41")),
    )
    fake_client.update_base_sha("sha42")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(42),
        "",
        queue_utils.PrMerged(42, github_types.SHAType("sha42")),
    )

    # [43+44] fail, so it's not 45, but is it 43 or 44?
    await t.refresh()
    assert [
        [41, 42, 43],
        [41, 42, 43, 44],
    ] == mt_conftest.get_train_cars_content(t)
    assert [45, *list(range(6, 20))] == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 2
    assert len(t._cars[1].failure_history) == 1
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )

    # mark [43] as failure
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(43), "", UNQUEUE_REASON_DEQUEUED
    )

    # Train got cut after 43, and we restart from the begining
    await t.refresh()
    assert [
        [44, 45, 6, 7, 8],
        [44, 45, 6, 7, 8, 9, 10, 11, 12, 13],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(14, 20)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 0
    assert len(t._cars[1].failure_history) == 0
    assert (
        t._cars[0].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )
    assert (
        t._cars[1].train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT
    )


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_splitted_on_failure_5x3(
    report_failure: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    fake_client: mock.Mock,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 47):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x3", 1000),
            "",
        )
    for i in range(7, 22):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "5x3", 1000),
            "",
        )

    await t.refresh()
    assert [
        [41, 42, 43],
        [41, 42, 43, 44, 45, 46],
        [41, 42, 43, 44, 45, 46, 7, 8, 9],
        [41, 42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12],
        [41, 42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(16, 22)) == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    assert [
        [41, 42, 43],
        [41, 42, 43, 44, 45, 46],
        [41, 42, 43, 44, 45, 46, 7, 8, 9],
        [41, 42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12],
        [41, 42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(16, 22)) == mt_conftest.get_train_waiting_pulls_content(t)

    await t.test_helper_load_from_redis()
    await t.refresh()
    assert [
        [41],
        [41, 42],
        [41, 42, 43],
    ] == mt_conftest.get_train_cars_content(t)
    assert [
        44,
        45,
        46,
        *list(range(7, 22)),
    ] == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0

    # mark [41] as failed
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(41), "", UNQUEUE_REASON_DEQUEUED
    )

    # nothing should move yet as we don't known yet if [41+42] is broken or not
    await t.refresh()
    assert [
        [42, 43, 44],
        [42, 43, 44, 45, 46, 7],
        [42, 43, 44, 45, 46, 7, 8, 9, 10],
        [42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12, 13],
        [42, 43, 44, 45, 46, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(17, 22)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 0
    assert len(t._cars[1].failure_history) == 0
    assert len(t._cars[2].failure_history) == 0
    assert len(t._cars[3].failure_history) == 0
    assert len(t._cars[4].failure_history) == 0

    # mark [42+43+44] as ready and merge it
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    t._cars[1].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    await t.save()
    fake_client.update_base_sha("sha42")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(42),
        "",
        queue_utils.PrMerged(42, github_types.SHAType("sha42")),
    )
    fake_client.update_base_sha("sha43")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(43),
        "",
        queue_utils.PrMerged(43, github_types.SHAType("sha43")),
    )
    fake_client.update_base_sha("sha44")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(44),
        "",
        queue_utils.PrMerged(44, github_types.SHAType("sha44")),
    )

    await t.refresh()
    assert [
        [42, 43, 44, 45],
        [42, 43, 44, 45, 46],
        [42, 43, 44, 45, 46, 7],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(8, 22)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0

    # mark [45] and [46+46] as success, so it's 7 fault !
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    t._cars[1].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    await t.save()

    # Nothing change yet!
    await t.refresh()
    assert [
        [42, 43, 44, 45],
        [42, 43, 44, 45, 46],
        [42, 43, 44, 45, 46, 7],
    ] == mt_conftest.get_train_cars_content(t)
    assert list(range(8, 22)) == mt_conftest.get_train_waiting_pulls_content(t)
    assert len(t._cars[0].failure_history) == 1
    assert len(t._cars[1].failure_history) == 1
    assert len(t._cars[2].failure_history) == 0
    # Merge 45 and 46
    fake_client.update_base_sha("sha45")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(45),
        "",
        queue_utils.PrMerged(45, github_types.SHAType("sha45")),
    )
    fake_client.update_base_sha("sha46")
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(46),
        "",
        queue_utils.PrMerged(46, github_types.SHAType("sha46")),
    )
    await t.refresh()
    assert [
        [42, 43, 44, 45, 46, 7],
    ] == mt_conftest.get_train_cars_content(t)
    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED
    assert len(t._cars[0].failure_history) == 0

    # remove the failed 7
    await t.remove_pull(
        github_types.GitHubPullRequestNumber(7), "", UNQUEUE_REASON_DEQUEUED
    )

    # Train got cut after 43, and we restart from the begining
    await t.refresh()
    assert [
        [8, 9, 10],
        [8, 9, 10, 11, 12, 13],
        [8, 9, 10, 11, 12, 13, 14, 15, 16],
        [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
    ] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_no_interrupt_add_pull(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-2x5-noint")

    await t.add_pull(await context_getter(1), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(2), config, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    # Inserting high prio didn't break started speculative checks, but the PR
    # move above other
    await t.add_pull(
        await context_getter(4),
        conftest.get_pull_queue_config(
            mt_conftest.QUEUE_RULES, "high-2x5-noint", 20000
        ),
        "",
    )
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [4, 3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_always_interrupt_across_queue(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "low-2x5-noint")

    await t.add_pull(await context_getter(1), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(2), config, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1], [1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    # Inserting pr in high queue always break started speculative checks even
    # if allow_checks_interruption is set
    await t.add_pull(
        await context_getter(4),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-2x5-noint"),
        "",
    )
    await t.refresh()
    assert [[4]] == mt_conftest.get_train_cars_content(t)
    assert [1, 2, 3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_interrupt_mixed_across_queue(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "low-1x5-noint")

    await t.add_pull(await context_getter(1), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(2), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(await context_getter(3), config, "")
    await t.refresh()
    assert [[1]] == mt_conftest.get_train_cars_content(t)
    assert [2, 3] == mt_conftest.get_train_waiting_pulls_content(t)

    # Inserting pr in high queue always break started speculative checks
    await t.add_pull(
        await context_getter(4),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "high-1x2"),
        "",
    )
    await t.refresh()
    assert [[4]] == mt_conftest.get_train_cars_content(t)
    assert [1, 2, 3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_disallow_checks_interruption_scenario_1(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    urgent = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "urgent-1x4")
    fastlane = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "fastlane-1x8-noint"
    )
    regular = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "regular-1x8-noint-from-fastlane-and-regular"
    )

    await t.add_pull(await context_getter(1), fastlane, "")
    await t.add_pull(await context_getter(2), fastlane, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    # regular doesn't interrupt the checks as it's below fastlane
    await t.add_pull(await context_getter(3), regular, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    # fastlane doesn't interrupt the checks because of noint, but goes before
    # regular
    await t.add_pull(await context_getter(4), fastlane, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [4, 3] == mt_conftest.get_train_waiting_pulls_content(t)

    # urgent breaks everything, and all fastlane got pack together, regular move behind
    await t.add_pull(await context_getter(5), urgent, "")
    await t.refresh()
    assert [[5]] == mt_conftest.get_train_cars_content(t)
    assert [1, 2, 4, 3] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_disallow_checks_interruption_scenario_2(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    urgent = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "urgent-1x4")
    fastlane = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "fastlane-1x8-noint"
    )
    regular = conftest.get_pull_queue_config(
        mt_conftest.QUEUE_RULES, "regular-1x8-noint-from-fastlane-and-regular"
    )

    await t.add_pull(await context_getter(1), regular, "")
    await t.add_pull(await context_getter(2), regular, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    # fastlane doesn't interrupt the checks as
    # disallow_checks_interruption_from_queues of regular disallow it
    await t.add_pull(await context_getter(3), fastlane, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

    # fastlane doesn't interrupt the checks because of noint, but goes before
    # regular
    await t.add_pull(await context_getter(4), regular, "")
    await t.refresh()
    assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
    assert [3, 4] == mt_conftest.get_train_waiting_pulls_content(t)

    # urgent breaks everything, then we put the fastlane one, and all regulars goes behind
    await t.add_pull(await context_getter(5), urgent, "")
    await t.refresh()
    assert [[5]] == mt_conftest.get_train_cars_content(t)
    assert [3, 1, 2, 4] == mt_conftest.get_train_waiting_pulls_content(t)


async def test_train_batch_max_wait_time(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    with freeze_time("2021-09-22T08:00:00") as freezed_time:
        t = merge_train.Train(convoy)
        await t.test_helper_load_from_redis()

        config = conftest.get_pull_queue_config(
            mt_conftest.QUEUE_RULES, "batch-wait-time"
        )

        await t.add_pull(await context_getter(1), config, "")
        await t.refresh()
        assert [] == mt_conftest.get_train_cars_content(t)
        assert [1] == mt_conftest.get_train_waiting_pulls_content(t)

        # Enought PR to batch!
        await t.add_pull(await context_getter(2), config, "")
        await t.refresh()
        assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
        assert [] == mt_conftest.get_train_waiting_pulls_content(t)

        await t.add_pull(await context_getter(3), config, "")
        await t.refresh()
        assert [[1, 2]] == mt_conftest.get_train_cars_content(t)
        assert [3] == mt_conftest.get_train_waiting_pulls_content(t)

        d = await delayed_refresh._get_current_refresh_datetime(
            convoy.repository, github_types.GitHubPullRequestNumber(3)
        )
        assert d is not None
        assert d == freezed_time().replace(tzinfo=datetime.UTC) + datetime.timedelta(
            minutes=5
        )

    with freeze_time("2021-09-22T08:05:02"):
        await t.refresh()
        assert [[1, 2], [1, 2, 3]] == mt_conftest.get_train_cars_content(t)
        assert [] == mt_conftest.get_train_waiting_pulls_content(t)


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_pr_with_higher_prio_enters_in_queue_during_merging_1x5(
    report_failure: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    fake_client: mock.Mock,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 46):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "1x5", 1000),
            "",
        )

    await t.refresh()
    assert [[41, 42, 43, 44, 45]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    await t.save()
    await t.refresh()
    assert [[41, 42, 43, 44, 45]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    # merge half of the batch
    for i in range(41, 44):
        fake_client.update_base_sha(f"sha{i}")
        await t.remove_pull(
            github_types.GitHubPullRequestNumber(i),
            "",
            queue_utils.PrMerged(i, github_types.SHAType(f"sha{i}")),
        )

    await t.refresh()
    assert [[44, 45]] == mt_conftest.get_train_cars_content(t)
    assert [] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(
        await context_getter(7),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "1x5", 10000),
        "",
    )
    await t.refresh()
    assert [[44, 45]] == mt_conftest.get_train_cars_content(t)
    assert [7] == mt_conftest.get_train_waiting_pulls_content(t)


@mock.patch("mergify_engine.queue.merge_train.TrainCar._set_creation_failure")
async def test_train_queue_pr_with_higher_prio_enters_in_queue_during_merging_2x5(
    report_failure: mock.Mock,
    context_getter: conftest.ContextGetterFixture,
    fake_client: mock.Mock,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    for i in range(41, 52):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x5", 1000),
            "",
        )

    await t.refresh()
    assert [
        [41, 42, 43, 44, 45],
        [41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
    ] == mt_conftest.get_train_cars_content(t)
    assert [51] == mt_conftest.get_train_waiting_pulls_content(t)

    t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.MERGEABLE
    await t.save()
    await t.refresh()
    assert [
        [41, 42, 43, 44, 45],
        [41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
    ] == mt_conftest.get_train_cars_content(t)
    assert [51] == mt_conftest.get_train_waiting_pulls_content(t)

    # merge half of the batch
    for i in range(41, 44):
        fake_client.update_base_sha(f"sha{i}")
        await t.remove_pull(
            github_types.GitHubPullRequestNumber(i),
            "",
            queue_utils.PrMerged(i, github_types.SHAType(f"sha{i}")),
        )

    await t.refresh()
    assert [
        [44, 45],
        [41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
    ] == mt_conftest.get_train_cars_content(t)
    assert [51] == mt_conftest.get_train_waiting_pulls_content(t)

    await t.add_pull(
        await context_getter(7),
        conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "2x5", 2000),
        "",
    )

    await t.refresh()
    assert [
        [44, 45],
        [44, 45, 7, 46, 47, 48, 49],
    ] == mt_conftest.get_train_cars_content(t)
    assert [50, 51] == mt_conftest.get_train_waiting_pulls_content(t)


def test_embarked_pull_old_serialization() -> None:
    config = queue.PullQueueConfig(
        name=qr_config.QueueName("foo"),
        update_method="merge",
        priority=0,
        effective_priority=0,
        bot_account=None,
        update_bot_account=None,
        autosquash=True,
    )

    now = date.utcnow()
    old_typed = merge_train.EmbarkedPull.OldSerialized(
        github_types.GitHubPullRequestNumber(1234), config, now
    )
    old_untyped = json.loads(json.dumps(old_typed))
    ep = merge_train.EmbarkedPull.deserialize(mock.Mock(), old_untyped)
    assert ep.user_pull_request_number == 1234
    assert ep.config == config
    assert ep.queued_at == now


@pytest.mark.parametrize(
    "creation_state,checks_type,checks_conclusion,ci_state,outcome,ci_has_passed,has_timed_out,outcome_message",
    (
        (
            "pending",
            None,
            check_api.Conclusion.PENDING,
            merge_train.CiState.PENDING,
            merge_train.TrainCarOutcome.UNKNOWN,
            False,
            False,
            "",
        ),
        (
            "created",
            merge_train.TrainCarChecksType.DRAFT,
            check_api.Conclusion.PENDING,
            merge_train.CiState.PENDING,
            merge_train.TrainCarOutcome.UNKNOWN,
            False,
            False,
            "",
        ),
        (
            "updated",
            merge_train.TrainCarChecksType.INPLACE,
            check_api.Conclusion.PENDING,
            merge_train.CiState.PENDING,
            merge_train.TrainCarOutcome.UNKNOWN,
            False,
            False,
            "",
        ),
        (
            "failed",
            merge_train.TrainCarChecksType.FAILED,
            check_api.Conclusion.PENDING,
            merge_train.CiState.PENDING,
            merge_train.TrainCarOutcome.UNKNOWN,
            False,
            False,
            "",
        ),
        (
            "failed",
            merge_train.TrainCarChecksType.FAILED,
            check_api.Conclusion.FAILURE,
            merge_train.CiState.FAILED,
            merge_train.TrainCarOutcome.CHECKS_FAILED,
            False,
            False,
            merge_train.CI_FAILED_MESSAGE,
        ),
        (
            "failed",
            merge_train.TrainCarChecksType.FAILED,
            check_api.Conclusion.PENDING,
            merge_train.CiState.PENDING,
            merge_train.TrainCarOutcome.CHECKS_TIMEOUT,
            False,
            True,
            merge_train.CHECKS_TIMEOUT_MESSAGE,
        ),
        (
            "updated",
            merge_train.TrainCarChecksType.INPLACE,
            check_api.Conclusion.SUCCESS,
            merge_train.CiState.SUCCESS,
            merge_train.TrainCarOutcome.MERGEABLE,
            True,
            False,
            "",
        ),
    ),
)
def test_train_car_old_serialization(
    creation_state: str,
    checks_type: merge_train.TrainCarChecksType,
    checks_conclusion: check_api.Conclusion,
    ci_state: merge_train.CiState,
    outcome: merge_train.TrainCarOutcome,
    ci_has_passed: bool,
    has_timed_out: bool,
    outcome_message: str,
) -> None:
    ep = {
        "user_pull_request_number": 1,
        "config": {},
        "queued_at": date.utcnow().isoformat(),
    }
    old_serialized_payload = {
        "initial_embarked_pulls": [ep],
        "still_queued_embarked_pulls": [ep],
        "parent_pull_request_numbers": [],
        "initial_current_base_sha": github_types.SHAType("toto"),
        "creation_date": date.utcnow(),
        "creation_state": creation_state,
        "checks_conclusion": checks_conclusion,
        "queue_pull_request_number": github_types.GitHubPullRequestNumber(123),
        "failure_history": [],
        "head_branch": "head_branch",
        "last_checks": [],
        "has_timed_out": has_timed_out,
        "checks_ended_timestamp": date.utcnow(),
        "ci_has_passed": ci_has_passed,
        "queue_branch_name": github_types.GitHubRefType("queue_branch"),
    }
    old_serialized_untyped = json.loads(json.dumps(old_serialized_payload))
    train = mock.Mock()
    train_car = merge_train.TrainCar.deserialize(train, old_serialized_untyped)
    train._cars = [train_car]
    assert train_car.train_car_state is not None
    assert not hasattr(train_car, "check_conclusions")
    assert not hasattr(train_car, "has_timed_out")
    assert not hasattr(train_car, "ci_has_passed")
    assert not hasattr(train_car, "creation_date")
    assert not hasattr(train_car, "creation_state")
    assert train_car.train_car_state.checks_type == checks_type
    assert train_car.train_car_state.outcome == outcome
    assert train_car.get_queue_check_run_conclusion() == (
        check_api.Conclusion.FAILURE if has_timed_out else checks_conclusion
    )
    assert train_car.train_car_state.ci_state == ci_state
    assert train_car.train_car_state.outcome_message == outcome_message


async def test_train_load_from_redis_with_None_partition_name(
    context_getter: conftest.ContextGetterFixture,
    repository: context.Repository,
) -> None:
    # This is a retrocompatibility test, it can be removed once all the trains with a None
    # partition_name have disappeared.
    ref = github_types.GitHubRefType("main")
    train_serialized = merge_train.Train.Serialized(
        waiting_pulls=[
            merge_train.EmbarkedPull.Serialized(
                user_pull_request_number=github_types.GitHubPullRequestNumber(12345),
                config=conftest.get_pull_queue_config(
                    mt_conftest.QUEUE_RULES, "inplace"
                ),
                queued_at=date.utcnow(),
            ),
        ],
        current_base_sha=github_types.SHAType("abc123"),
        cars=[],
        partition_name=None,
    )
    train_raw = json.dumps(train_serialized)
    await repository.installation.redis.cache.hset(
        merge_train.get_redis_train_key(repository.installation),
        f"{repository.repo['id']}~{ref}",
        train_raw,
    )

    # ######
    # Loading all trains/convoy with `_get_raw_trains_by_convoy` should not
    # delete anything in redis since there is only the old redis train key
    trains_by_convoy = await merge_train.Convoy._get_raw_trains_by_convoy(
        repository.installation
    )
    assert len(trains_by_convoy[(repository.repo["id"], ref)]) == 1
    assert (
        partr_config.DEFAULT_PARTITION_NAME
        in trains_by_convoy[(repository.repo["id"], ref)]
    )
    redis_trains = await repository.installation.redis.cache.hgetall(
        merge_train.get_redis_train_key(repository.installation)
    )
    assert len(redis_trains) == 1
    assert next(iter(redis_trains.keys())).decode() == f"{repository.repo['id']}~{ref}"

    # ######
    # Loading a convoy with the old default partition name with `load_from_redis`
    # should load it properly with the partition_name of its only train set
    # to the new default value, and also delete the old redis train key
    convoy = merge_train.Convoy(
        repository,
        mt_conftest.QUEUE_RULES,
        partr_config.PartitionRules([]),
        ref,
    )
    await convoy.load_from_redis()

    assert len(convoy._trains) == 1
    assert convoy._trains[0].partition_name == partr_config.DEFAULT_PARTITION_NAME
    await convoy.save()

    # Make sure that `load_from_redis` does not delete the old key if
    # there is not yet the new key.
    redis_trains = await repository.installation.redis.cache.hgetall(
        merge_train.get_redis_train_key(repository.installation)
    )
    assert len(redis_trains) == 2
    redis_trains_keys = sorted(redis_trains.keys())
    assert redis_trains_keys[0].decode() == f"{repository.repo['id']}~{ref}"
    assert (
        redis_trains_keys[1].decode()
        == f"{repository.repo['id']}~{ref}~{partr_config.DEFAULT_PARTITION_NAME}"
    )

    # ######
    # Make sure that `load_from_redis` delete the old redis train key
    # when both the old and new are present.
    await convoy.load_from_redis()
    redis_trains = await repository.installation.redis.cache.hgetall(
        merge_train.get_redis_train_key(repository.installation)
    )
    assert len(redis_trains) == 1
    assert (
        next(iter(redis_trains.keys()))
        .decode()
        .endswith(partr_config.DEFAULT_PARTITION_NAME)
    )
    # ######
    # Re-add the old train key and make sure that `Convoy._get_raw_trains_by_convoy`
    # also properly deletes the old redis key when both are present
    await repository.installation.redis.cache.hset(
        merge_train.get_redis_train_key(repository.installation),
        f"{repository.repo['id']}~{ref}",
        train_raw,
    )
    trains_by_convoy = await merge_train.Convoy._get_raw_trains_by_convoy(
        repository.installation
    )
    assert len(trains_by_convoy[(repository.repo["id"], ref)]) == 1
    assert (
        partr_config.DEFAULT_PARTITION_NAME
        in trains_by_convoy[(repository.repo["id"], ref)]
    )

    redis_trains = await repository.installation.redis.cache.hgetall(
        merge_train.get_redis_train_key(repository.installation)
    )
    assert len(redis_trains) == 1
    assert (
        next(iter(redis_trains.keys()))
        .decode()
        .endswith(partr_config.DEFAULT_PARTITION_NAME)
    )


@pytest.mark.parametrize(
    "config_name, failure_count",
    (
        ("2x8-batch-max-failure-2", 2),
        ("2x8-batch-max-failure-0", 0),
        ("1x8-batch-max-failure-2", 2),
        ("1x8-batch-max-failure-0", 0),
    ),
)
async def test_train_car_has_reached_batch_max_failure(
    context_getter: conftest.ContextGetterFixture,
    config_name: str,
    failure_count: int,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    # Populate train
    for i in range(40, 48):
        await t.add_pull(
            await context_getter(i),
            conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, config_name),
            "",
        )
    await t.refresh()

    # E.g. for batch_max_failure_resolution_attempts=2
    # - First failure, first resolution attempt
    # - Second failure, second resolution attempt
    # - Third failure, maximum reached, outcome should not be unknown
    for _ in range(failure_count):
        assert t._cars[0].train_car_state.outcome == merge_train.TrainCarOutcome.UNKNOWN
        assert not t._cars[0]._has_reached_batch_max_failure()

        t._cars[0].train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED

        await t.save()
        await t.test_helper_load_from_redis()
        await t.refresh()

    first_car = t._cars[0]
    first_car.train_car_state.outcome = merge_train.TrainCarOutcome.CHECKS_FAILED

    assert first_car._has_reached_batch_max_failure()


async def test_train_inplace_branch_update_failure(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "inplace")

    ctxt = await context_getter(12345)
    await t.add_pull(ctxt, config, "")

    with mock.patch.object(
        merge_train.TrainCar,
        "can_be_checked_inplace",
        side_effect=mock.AsyncMock(return_value=True),
    ):
        await t.refresh()

    ctxt.client.put.side_effect = branch_updater.BranchUpdateFailure("oops")  # type: ignore [attr-defined]
    with mock.patch.object(merge_train.TrainCar, "_set_creation_failure"):
        with pytest.raises(merge_train.TrainCarPullRequestCreationFailure):
            await t._cars[0]._start_checking_inplace_merge(ctxt)

    assert (
        t._cars[0].train_car_state.outcome
        == merge_train.TrainCarOutcome.BRANCH_UPDATE_FAILED
    )


async def test_train_inplace_branch_rebase_failure(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    t = merge_train.Train(convoy)
    await t.test_helper_load_from_redis()

    config = conftest.get_pull_queue_config(mt_conftest.QUEUE_RULES, "inplace")

    ctxt = await context_getter(12345)
    await t.add_pull(ctxt, config, "")

    with mock.patch.object(
        merge_train.TrainCar,
        "can_be_checked_inplace",
        side_effect=mock.AsyncMock(return_value=True),
    ):
        await t.refresh()

    with mock.patch.object(merge_train.TrainCar, "_set_creation_failure"), mock.patch(
        "mergify_engine.actions.utils.get_github_user_from_bot_account"
    ), mock.patch(
        "mergify_engine.branch_updater.rebase_with_git",
        side_effect=branch_updater.BranchUpdateFailure("oops"),
    ):
        with pytest.raises(merge_train.TrainCarPullRequestCreationFailure):
            await t._cars[0]._start_checking_inplace_rebase(ctxt)

    assert (
        t._cars[0].train_car_state.outcome
        == merge_train.TrainCarOutcome.BRANCH_UPDATE_FAILED
    )
