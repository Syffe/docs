from __future__ import annotations

import datetime
import statistics

from mergify_engine import database
from mergify_engine import date
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api.statistics import utils as web_stat_utils


async def get_estimation_from_stats(
    train: merge_train.Train,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    checks_duration_stats: web_stat_utils.QueueChecksDurationsPerPartitionQueueBranchT,
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
    previous_car: merge_train.TrainCar | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.convoy.is_queue_frozen(queue_name):
        return None

    queue_checks_duration_stats = (
        checks_duration_stats.get(train.partition_name, {})
        .get(queue_name, {})
        .get(train.convoy.ref, None)
    )
    if queue_checks_duration_stats is None:
        median = None
    else:
        median = statistics.median(queue_checks_duration_stats)
    return await compute_estimation(
        embarked_pull,
        embarked_pull_position,
        train.convoy.queue_rules[queue_name].config,
        median,
        car,
        previous_eta,
        previous_car,
    )


async def get_estimation(
    session: database.Session,
    partition_rules: partr_config.PartitionRules,
    train: merge_train.Train,
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    car: merge_train.TrainCar | None,
    previous_eta: datetime.datetime | None = None,
    previous_car: merge_train.TrainCar | None = None,
) -> datetime.datetime | None:
    queue_name = embarked_pull.config["name"]
    if await train.convoy.is_queue_frozen(queue_name):
        return None

    queue_checks_duration_stats = await web_stat_utils.get_queue_checks_duration(
        session=session,
        repository_ctxt=train.convoy.repository,
        partition_rules=partition_rules,
        queue_names=(queue_name,),
        partition_names=(train.partition_name,),
        branch=train.convoy.ref,
    )
    return await compute_estimation(
        embarked_pull,
        embarked_pull_position,
        train.convoy.queue_rules[queue_name].config,
        queue_checks_duration_stats["median"],
        car,
        previous_eta,
        previous_car,
    )


async def compute_estimation(
    embarked_pull: merge_train.EmbarkedPull,
    embarked_pull_position: int,
    queue_rule_config: qr_config.QueueConfig,
    checks_duration: float | None,
    car: merge_train.TrainCar | None,
    # previous_eta must be the eta of the pr in position `embarked_pull_position - 1`
    previous_eta: datetime.datetime | None = None,
    # previous_car must be the TrainCar of the PR in position `embarked_pull_position - 1`
    previous_car: merge_train.TrainCar | None = None,
) -> datetime.datetime | None:
    if checks_duration is None or (
        (car is None or car.last_merge_conditions_evaluation is None)
        and previous_eta is None
    ):
        return None

    if (
        (car is None or car.train_car_state.ci_started_at is None)
        and previous_eta is not None
        and previous_eta < date.utcnow()
    ):
        # It means we haven't started the CI yet on this car and the ETA has been passed already,
        # so we adjust the new eta accordingly
        return date.utcnow() + datetime.timedelta(seconds=checks_duration)

    if previous_eta is not None and (
        (
            previous_car is None
            # `embarked_pull_position` starts at 0
            and queue_utils.is_same_batch(
                embarked_pull_position,
                embarked_pull_position + 1,
                queue_rule_config["batch_size"],
            )
        )
        or (
            previous_car is not None
            and embarked_pull in previous_car.initial_embarked_pulls
        )
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

    # The time spent waiting for a schedule or freeze isn't time spent waiting for the CI, because the CI is still
    # being executed while outside of schedule
    checks_duration -= max(
        0,
        car.train_car_state.seconds_waiting_for_schedule_pure
        + car.train_car_state.seconds_waiting_for_freeze_pure,
    )

    raw_estimated_time_of_merge = (
        car.train_car_state.ci_started_at
        + datetime.timedelta(
            seconds=car.train_car_state.seconds_waiting_for_schedule_pure
            + car.train_car_state.seconds_waiting_for_freeze_pure
            + car.train_car_state.seconds_spent_outside_schedule_pure
            + checks_duration
        )
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

    # No currently non-matching schedule conditions needs to be taken into account
    return raw_estimated_time_of_merge
