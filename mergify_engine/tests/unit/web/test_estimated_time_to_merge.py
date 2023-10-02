import datetime
import typing
from unittest import mock

from freezegun import freeze_time

from mergify_engine import date
from mergify_engine.rules import conditions as rule_conditions
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.queues import estimated_time_to_merge


async def test_compute_estimation_no_batch_first_pr() -> None:
    with freeze_time("2023-08-29"):
        embarked_pull = mock.Mock()
        embarked_pull_position = 0
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 1}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = mock.Mock()
        car.train_car_state.ci_started_at = date.utcnow()
        car.train_car_state.seconds_waiting_for_schedule_pure = 0
        car.train_car_state.seconds_waiting_for_freeze_pure = 0
        car.train_car_state.seconds_spent_outside_schedule_pure = 0
        car.last_merge_conditions_evaluation = (
            rule_conditions.QueueConditionEvaluationResult(
                match=True, label="", is_label_user_input=False
            )
        )
        previous_eta = None

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.utcnow() + checks_duration
        assert actual_eta == expected_eta


async def test_compute_estimation_no_batch_second_pr() -> None:
    with freeze_time("2023-08-29"):
        embarked_pull = mock.Mock()
        embarked_pull_position = 1
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 1}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = None
        previous_eta = date.utcnow() + checks_duration

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.utcnow() + (checks_duration * 2)
        assert actual_eta == expected_eta


async def test_compute_estimation_batch_second_pr() -> None:
    with freeze_time("2023-08-29"):
        embarked_pull = mock.Mock()
        embarked_pull_position = 1
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 2}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = mock.Mock()
        car.train_car_state.seconds_waiting_for_schedule_pure = 0
        car.train_car_state.seconds_waiting_for_freeze_pure = 0
        car.train_car_state.seconds_spent_outside_schedule_pure = 0
        previous_eta = date.utcnow() + checks_duration

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.utcnow() + checks_duration
        assert actual_eta == expected_eta


async def test_compute_estimation_batch_third_pr() -> None:
    with freeze_time("2023-08-29"):
        embarked_pull = mock.Mock()
        embarked_pull_position = 2
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 2}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = None
        previous_eta = date.utcnow() + checks_duration

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.utcnow() + (checks_duration * 2)
        assert actual_eta == expected_eta


async def test_compute_estimation_in_schedule() -> None:
    with freeze_time("2023-08-29 12:00"):
        embarked_pull = mock.Mock()
        embarked_pull.queued_at = date.utcnow()
        embarked_pull_position = 0
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 1}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = mock.Mock()
        car.train_car_state.seconds_waiting_for_schedule_pure = 0
        car.train_car_state.seconds_waiting_for_freeze_pure = 0
        car.train_car_state.seconds_spent_outside_schedule_pure = 0
        car.train_car_state.ci_started_at = date.utcnow()
        car.last_merge_conditions_evaluation = (
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="all of",
                is_label_user_input=False,
                subconditions=[
                    rule_conditions.QueueConditionEvaluationResult(
                        match=True,
                        label="schedule=09:00-17:00",
                        is_label_user_input=True,
                        attribute_name="schedule",
                    )
                ],
            )
        )
        previous_eta = None

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.fromisoformat("2023-08-29 12:10")
        assert actual_eta == expected_eta


async def test_compute_estimation_out_of_schedule() -> None:
    with freeze_time("2023-08-29 00:00"):
        embarked_pull = mock.Mock()
        embarked_pull.queued_at = date.utcnow()
        embarked_pull_position = 0
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 1}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = mock.Mock()
        car.train_car_state.seconds_waiting_for_schedule_pure = 0
        car.train_car_state.seconds_waiting_for_freeze_pure = 0
        car.train_car_state.seconds_spent_outside_schedule_pure = 0
        car.train_car_state.ci_started_at = date.utcnow()
        car.last_merge_conditions_evaluation = (
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="all of",
                is_label_user_input=False,
                subconditions=[
                    rule_conditions.QueueConditionEvaluationResult(
                        match=False,
                        label="schedule=09:00-17:00",
                        is_label_user_input=True,
                        attribute_name="schedule",
                    )
                ],
            )
        )
        previous_eta = None

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.fromisoformat("2023-08-29 09:00:00")
        assert actual_eta == expected_eta


async def test_compute_estimation_checks_end_in_schedule() -> None:
    with freeze_time("2023-08-29 08:55"):
        embarked_pull = mock.Mock()
        embarked_pull.queued_at = date.utcnow()
        embarked_pull_position = 0
        queue_rule_config = typing.cast(
            qr_config.QueueConfig, {"speculative_checks": 1, "batch_size": 1}
        )
        checks_duration = datetime.timedelta(minutes=10)
        car = mock.Mock()
        car.train_car_state.seconds_waiting_for_schedule_pure = 0
        car.train_car_state.seconds_waiting_for_freeze_pure = 0
        car.train_car_state.seconds_spent_outside_schedule_pure = 0
        car.train_car_state.ci_started_at = date.utcnow()
        car.last_merge_conditions_evaluation = (
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="all of",
                is_label_user_input=False,
                subconditions=[
                    rule_conditions.QueueConditionEvaluationResult(
                        match=False,
                        label="schedule=09:00-17:00",
                        is_label_user_input=True,
                        attribute_name="schedule",
                    )
                ],
            )
        )
        previous_eta = None

        actual_eta = await estimated_time_to_merge.compute_estimation(
            embarked_pull,
            embarked_pull_position,
            queue_rule_config,
            checks_duration.total_seconds(),
            car,
            previous_eta,
        )

        expected_eta = date.fromisoformat("2023-08-29 09:05:00")
        assert actual_eta == expected_eta
