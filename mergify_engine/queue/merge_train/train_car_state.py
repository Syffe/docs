from __future__ import annotations

import base64
import dataclasses
import datetime
import typing

import daiquiri

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import utils
from mergify_engine.queue import freeze
from mergify_engine.queue import merge_train
from mergify_engine.queue import pause
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import train_car
from mergify_engine.queue.merge_train import types as queue_types
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)


class UnexpectedOutcome(Exception):
    pass


@dataclasses.dataclass
class TrainCarStateForSummary:
    outcome: train_car.TrainCarOutcome = train_car.TrainCarOutcome.UNKNOWN
    outcome_message: str | None = None

    @classmethod
    def from_train_car_state(
        cls, train_car_state: TrainCarState
    ) -> TrainCarStateForSummary:
        return cls(train_car_state.outcome, train_car_state.outcome_message)

    class Serialized(typing.TypedDict):
        outcome: train_car.TrainCarOutcome
        outcome_message: str | None

    def serialized(self) -> str:
        data = self.Serialized(
            outcome=self.outcome,
            outcome_message=self.outcome_message,
        )

        return "<!-- " + base64.b64encode(json.dumps(data).encode()).decode() + " -->"

    @classmethod
    def deserialize(
        cls, data: TrainCarStateForSummary.Serialized
    ) -> TrainCarStateForSummary:
        return cls(
            outcome=data["outcome"],
            outcome_message=data["outcome_message"],
        )

    @classmethod
    def deserialize_from_summary(
        cls, summary_check: github_types.CachedGitHubCheckRun | None
    ) -> TrainCarStateForSummary | None:
        line = extract_encoded_train_car_state_data_from_summary(summary_check)
        if line is not None:
            serialized = typing.cast(
                TrainCarStateForSummary.Serialized,
                json.loads(
                    base64.b64decode(utils.strip_comment_tags(line).encode()).decode()
                ),
            )
            return cls.deserialize(serialized)
        return None


@dataclasses.dataclass
class TrainCarState:
    outcome: train_car.TrainCarOutcome = train_car.TrainCarOutcome.UNKNOWN
    ci_state: queue_types.CiState = queue_types.CiState.PENDING
    ci_started_at: datetime.datetime | None = None
    ci_ended_at: datetime.datetime | None = None
    outcome_message: str | None = None
    checks_type: train_car.TrainCarChecksType | None = None
    # NOTE(Syffe): The freeze and pause attributes (frozen_by, paused_by) are stored solely for reporting reasons
    # It should not be used for other purposes
    frozen_by: freeze.QueueFreeze | None = None
    paused_by: pause.QueuePause | None = None
    # NOTE(Greesb): Those 4 variables below are used to compute the time spent waiting
    # for schedule and for freeze. Both of those time, in seconds, are stored
    # in the `action.queue.leave` event and then used in statistics to calculate the
    # time to merge of a PR, minus those 2 times.
    waiting_for_freeze_start_dates: list[datetime.datetime] = dataclasses.field(
        default_factory=list
    )
    waiting_for_freeze_end_dates: list[datetime.datetime] = dataclasses.field(
        default_factory=list
    )
    waiting_for_schedule_start_dates: list[datetime.datetime] = dataclasses.field(
        default_factory=list
    )
    waiting_for_schedule_end_dates: list[datetime.datetime] = dataclasses.field(
        default_factory=list
    )
    # NOTE(Greesb): Those 2 variables are used to compute the time spent outside a schedule
    # condition that would make the CI fail if all the others non-schedule condition were True.
    # This is/will be used during the calculation of the ETA.
    time_spent_outside_schedule_start_dates: list[
        datetime.datetime
    ] = dataclasses.field(default_factory=list)
    time_spent_outside_schedule_end_dates: list[datetime.datetime] = dataclasses.field(
        default_factory=list
    )

    class Serialized(typing.TypedDict):
        outcome: train_car.TrainCarOutcome
        ci_state: queue_types.CiState
        ci_started_at: datetime.datetime | None
        ci_ended_at: datetime.datetime | None
        outcome_message: str | None
        checks_type: train_car.TrainCarChecksType | None
        # None support is for backward compat, introduced in 7.6.0
        frozen_by: freeze.QueueFreeze.Serialized | None
        # None support is for backward compat, introduced in 7.6.0
        paused_by: pause.QueuePause.Serialized | None
        waiting_for_freeze_start_dates: list[datetime.datetime]
        waiting_for_freeze_end_dates: list[datetime.datetime]
        waiting_for_schedule_start_dates: list[datetime.datetime]
        waiting_for_schedule_end_dates: list[datetime.datetime]
        time_spent_outside_schedule_start_dates: list[datetime.datetime]
        time_spent_outside_schedule_end_dates: list[datetime.datetime]

    def serialized(self) -> TrainCarState.Serialized:
        frozen_by = None
        if self.frozen_by is not None:
            frozen_by = self.frozen_by.serialized()

        paused_by = None
        if self.paused_by is not None:
            paused_by = self.paused_by.serialized()

        return self.Serialized(
            outcome=self.outcome,
            ci_state=self.ci_state,
            ci_started_at=self.ci_started_at,
            ci_ended_at=self.ci_ended_at,
            outcome_message=self.outcome_message,
            checks_type=self.checks_type,
            frozen_by=frozen_by,
            paused_by=paused_by,
            waiting_for_freeze_start_dates=self.waiting_for_freeze_start_dates,
            waiting_for_freeze_end_dates=self.waiting_for_freeze_end_dates,
            waiting_for_schedule_start_dates=self.waiting_for_schedule_start_dates,
            waiting_for_schedule_end_dates=self.waiting_for_schedule_end_dates,
            time_spent_outside_schedule_start_dates=self.time_spent_outside_schedule_start_dates,
            time_spent_outside_schedule_end_dates=self.time_spent_outside_schedule_end_dates,
        )

    @classmethod
    def deserialize(
        cls,
        repository: context.Repository,
        queue_rules: qr_config.QueueRules,
        data: TrainCarState.Serialized,
    ) -> TrainCarState:
        # Backward compat, introduced in 7.6.0
        kwargs = {
            "waiting_for_freeze_start_dates": data.get(
                "waiting_for_freeze_start_dates", []
            ),
            "waiting_for_freeze_end_dates": data.get(
                "waiting_for_freeze_end_dates", []
            ),
            "waiting_for_schedule_start_dates": data.get(
                "waiting_for_schedule_start_dates", []
            ),
            "waiting_for_schedule_end_dates": data.get(
                "waiting_for_schedule_end_dates", []
            ),
            "time_spent_outside_schedule_start_dates": data.get(
                "time_spent_outside_schedule_start_dates", []
            ),
            "time_spent_outside_schedule_end_dates": data.get(
                "time_spent_outside_schedule_end_dates", []
            ),
        }

        # Backward compat, introduced in 7.6.0
        legacy_creation_date: datetime.datetime | None = data.get("creation_date")  # type: ignore[assignment]

        # backward compatibility following the implementation of "frozen_by" attribute, introduced in 7.6.0
        frozen_by = None
        if (frozen_by_raw := data.get("frozen_by")) is not None:
            try:
                queue_rule = queue_rules[qr_config.QueueName(frozen_by_raw["name"])]
            except KeyError:
                pass
            else:
                frozen_by = freeze.QueueFreeze.deserialize(
                    repository, queue_rule, frozen_by_raw
                )

        # backward compatibility following the implementation of "paused_by" attribute, introduced in 7.6.0
        paused_by = None
        if (paused_by_raw := data.get("paused_by")) is not None:
            paused_by = pause.QueuePause.deserialize(repository, paused_by_raw)

        return cls(
            outcome=data["outcome"],
            ci_state=data["ci_state"],
            ci_started_at=data.get("ci_started_at", legacy_creation_date),
            ci_ended_at=data.get("ci_ended_at"),
            outcome_message=data["outcome_message"],
            checks_type=data["checks_type"],
            frozen_by=frozen_by,
            paused_by=paused_by,
            **kwargs,
        )

    def ci_has_timed_out(self, timeout: datetime.timedelta) -> bool:
        if self.ci_started_at is None:
            return False
        ci_duration = (
            self.ci_ended_at if self.ci_ended_at is not None else date.utcnow()
        )
        return (ci_duration - self.ci_started_at) > timeout

    def add_waiting_for_schedule_start_date(self) -> None:
        if len(self.waiting_for_schedule_start_dates) == 0 or len(
            self.waiting_for_schedule_start_dates
        ) <= len(self.waiting_for_schedule_end_dates):
            self.waiting_for_schedule_start_dates.append(date.utcnow())

    def add_waiting_for_schedule_end_date(self) -> None:
        if len(self.waiting_for_schedule_start_dates) > len(
            self.waiting_for_schedule_end_dates
        ):
            self.waiting_for_schedule_end_dates.append(date.utcnow())

    def add_waiting_for_freeze_start_date(self) -> None:
        if len(self.waiting_for_freeze_start_dates) == 0 or len(
            self.waiting_for_freeze_start_dates
        ) <= len(self.waiting_for_freeze_end_dates):
            self.waiting_for_freeze_start_dates.append(date.utcnow())

    def add_waiting_for_freeze_end_date(self) -> None:
        if len(self.waiting_for_freeze_start_dates) > len(
            self.waiting_for_freeze_end_dates
        ):
            self.waiting_for_freeze_end_dates.append(date.utcnow())

    def add_time_spent_outside_schedule_start_date(self) -> None:
        if len(self.time_spent_outside_schedule_start_dates) == 0 or len(
            self.time_spent_outside_schedule_start_dates
        ) <= len(self.time_spent_outside_schedule_end_dates):
            self.time_spent_outside_schedule_start_dates.append(date.utcnow())

    def add_time_spent_outside_schedule_end_date(self) -> None:
        if len(self.time_spent_outside_schedule_start_dates) > len(
            self.time_spent_outside_schedule_end_dates
        ):
            self.time_spent_outside_schedule_end_dates.append(date.utcnow())

    @staticmethod
    def _compute_seconds_waiting_from_lists(
        start_dates_list: list[datetime.datetime],
        end_dates_list: list[datetime.datetime],
    ) -> int:
        if len(start_dates_list) != len(end_dates_list):
            raise RuntimeError(
                "Got different sized date list "
                f"(start={len(start_dates_list)} / end={len(end_dates_list)}) "
                "to compute in TrainCarState"
            )

        seconds = 0
        for i in range(len(start_dates_list)):
            seconds += int((end_dates_list[i] - start_dates_list[i]).total_seconds())

        return seconds

    def _compute_time_arrays_in_seconds(
        self,
        start_dates: list[datetime.datetime],
        end_dates: list[datetime.datetime],
        add_missing_end_date: bool,
    ) -> int:
        if len(start_dates) - 1 == len(end_dates):
            # In this case, that means a PR has been unexpectedly unqueued
            # and the train car did not have time to receive an `update_state`.
            if add_missing_end_date:
                end_dates.append(date.utcnow())
            else:
                return self._compute_seconds_waiting_from_lists(
                    start_dates,
                    [*end_dates, date.utcnow()],
                )

        return self._compute_seconds_waiting_from_lists(
            start_dates,
            end_dates,
        )

    @property
    def seconds_spent_outside_schedule(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.time_spent_outside_schedule_start_dates,
            self.time_spent_outside_schedule_end_dates,
            True,
        )

    @property
    def seconds_spent_outside_schedule_pure(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.time_spent_outside_schedule_start_dates,
            self.time_spent_outside_schedule_end_dates,
            False,
        )

    @property
    def seconds_waiting_for_schedule(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.waiting_for_schedule_start_dates,
            self.waiting_for_schedule_end_dates,
            True,
        )

    @property
    def seconds_waiting_for_schedule_pure(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.waiting_for_schedule_start_dates,
            self.waiting_for_schedule_end_dates,
            False,
        )

    @property
    def seconds_waiting_for_freeze(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.waiting_for_freeze_start_dates, self.waiting_for_freeze_end_dates, True
        )

    @property
    def seconds_waiting_for_freeze_pure(self) -> int:
        return self._compute_time_arrays_in_seconds(
            self.waiting_for_freeze_start_dates,
            self.waiting_for_freeze_end_dates,
            False,
        )


def extract_encoded_train_car_state_data_from_summary(
    summary_check: github_types.CachedGitHubCheckRun | None,
) -> str | None:
    if summary_check is not None and summary_check["output"]["summary"] is not None:
        lines = summary_check["output"]["summary"].splitlines()
        if not lines:
            return None

        if lines[-1].startswith("<!-- ") and lines[-1].endswith(" -->"):
            return lines[-1]

        if lines[0].startswith("<!-- ") and lines[0].endswith(" -->"):
            return lines[0]

    return None


def unqueue_reason_from_train_car_state(
    train_car_state: TrainCarState | TrainCarStateForSummary,
) -> queue_utils.BaseUnqueueReason:
    if train_car_state.outcome in (
        merge_train.TrainCarOutcome.DRAFT_PR_CHANGE,
        merge_train.TrainCarOutcome.BASE_BRANCH_CHANGE,
        merge_train.TrainCarOutcome.UPDATED_PR_CHANGE,
    ):
        if train_car_state.outcome_message is None:
            LOG.error(
                "outcome_message is not set", outcome=train_car_state.outcome.value
            )
        return queue_utils.UnexpectedQueueChange(
            change=train_car_state.outcome_message or "unexpected queue change"
        )

    if (
        train_car_state.outcome
        == merge_train.TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE
    ):
        return queue_utils.ChecksStoppedBecauseMergeQueuePause()

    if train_car_state.outcome == merge_train.TrainCarOutcome.CHECKS_FAILED:
        return queue_utils.ChecksFailed()

    if train_car_state.outcome == merge_train.TrainCarOutcome.CHECKS_TIMEOUT:
        return queue_utils.ChecksTimeout()

    if (
        train_car_state.outcome
        == merge_train.TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
    ):
        return queue_utils.MaximumBatchFailureResolutionAttemptsReached()

    if train_car_state.outcome == merge_train.TrainCarOutcome.CONFLICT_WITH_BASE_BRANCH:
        return queue_utils.ConflictWithBaseBranch()

    if train_car_state.outcome == merge_train.TrainCarOutcome.CONFLICT_WITH_PULL_AHEAD:
        return queue_utils.ConflictWithPullAhead()

    if train_car_state.outcome == merge_train.TrainCarOutcome.BRANCH_UPDATE_FAILED:
        return queue_utils.BranchUpdateFailed()

    raise UnexpectedOutcome(
        f"TrainCarState.outcome `{train_car_state.outcome.value}` can't be mapped to an AbortReason"
    )
