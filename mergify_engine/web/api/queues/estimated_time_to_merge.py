from __future__ import annotations

import datetime

from mergify_engine import date
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import types as web_stat_types
from mergify_engine.web.api.statistics import utils as web_stat_utils


async def get_estimation_from_stats(
    train: merge_train.Train,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    checks_duration_stats: dict[
        qr_config.QueueName, web_stat_types.ChecksDurationResponse
    ],
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.convoy.is_queue_frozen(queue_name):
        return None

    queue_checks_duration_stats = checks_duration_stats.get(
        queue_name,
        web_stat_types.ChecksDurationResponse(mean=None, median=None),
    )
    return await compute_estimation(
        embarked_pull,
        embarked_pull_position,
        train.convoy.queue_rules[queue_name].config,
        queue_checks_duration_stats["median"],
        car,
        previous_eta,
    )


async def get_estimation(
    train: merge_train.Train,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.convoy.is_queue_frozen(queue_name):
        return None

    queue_checks_duration_stats = (
        await web_stat_utils.get_checks_duration_stats_for_queue(
            train.convoy.repository,
            train.partition_name,
            queue_name,
            branch_name=train.convoy.ref,
        )
    )
    return await compute_estimation(
        embarked_pull,
        embarked_pull_position,
        train.convoy.queue_rules[queue_name].config,
        queue_checks_duration_stats["median"],
        car,
        previous_eta,
    )


async def compute_estimation(
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    queue_rule_config: qr_config.QueueConfig,
    checks_duration: float | None,
    car: merge_train.TrainCar | None,
    # previous_eta must be the eta of the pr in position `embarked_pull_position - 1`
    previous_eta: datetime.datetime | None = None,
) -> datetime.datetime | None:
    if checks_duration is None or (
        (car is None or car.last_merge_conditions_evaluation is None)
        and previous_eta is None
    ):
        return None

    # `embarked_pull_position` starts at 0
    if previous_eta is not None and queue_utils.is_same_batch(
        embarked_pull_position,
        embarked_pull_position + 1,
        queue_rule_config["batch_size"],
    ):
        # The PR is in the same batch as the PR the `previous_eta` is from
        return previous_eta

    if car is None or car.last_merge_conditions_evaluation is None:
        # The PR is not in the same batch as the previous PR and has
        # no car.
        # mypy thinks previous_eta can be None, but the first `if` of this function prevents it.
        return previous_eta + datetime.timedelta(seconds=checks_duration)  # type: ignore[operator, return-value]

    if car.train_car_state.ci_started_at is None:
        return None

    raw_estimated_time_of_merge = (
        car.train_car_state.ci_started_at + datetime.timedelta(seconds=checks_duration)
    )
    # Evaluate schedule conditions relative to the current time
    farthest_datetime_from_conditions = (
        rules_conditions.get_farthest_datetime_from_non_match_schedule_condition(
            car.last_merge_conditions_evaluation.subconditions,
            embarked_pull.user_pull_request_number,
            date.utcnow(),
        )
    )
    if farthest_datetime_from_conditions is None:
        # Only re-evaluate schedule conditions relative to the raw_estimated_time_of_merge
        # if the current time conditions do not fail.
        # If they do that means then we are already out of schedule, so no point in
        # trying to re-evaluate the schedule conditions relative to the raw_estimated_time_of_merge
        # since we won't return that time in the end.
        re_evaluated_conditions = rules_conditions.re_evaluate_schedule_conditions(
            car.last_merge_conditions_evaluation.copy().subconditions,
            raw_estimated_time_of_merge,
        )
        farthest_datetime_from_conditions = (
            rules_conditions.get_farthest_datetime_from_non_match_schedule_condition(
                re_evaluated_conditions,
                embarked_pull.user_pull_request_number,
                raw_estimated_time_of_merge,
            )
        )

    max_number_of_pr_checked = (
        queue_rule_config["speculative_checks"] * queue_rule_config["batch_size"]
    )
    if farthest_datetime_from_conditions is not None:
        if embarked_pull_position < max_number_of_pr_checked:
            # Substract the checks_duration with the time the queued PR will have spent
            # waiting for the schedule only if it is part of some running speculative checks.
            checks_duration -= (
                farthest_datetime_from_conditions - embarked_pull.queued_at
            ).total_seconds()
            checks_duration = max(0, checks_duration)

        return farthest_datetime_from_conditions + datetime.timedelta(
            seconds=checks_duration
        )

    # No schedule conditions needs to be taken into account
    return raw_estimated_time_of_merge
