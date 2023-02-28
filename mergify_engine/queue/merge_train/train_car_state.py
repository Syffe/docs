import base64
import dataclasses
import datetime
import typing

from mergify_engine import context
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import utils
from mergify_engine.queue import freeze
from mergify_engine.queue.merge_train import train_car
from mergify_engine.queue.merge_train import types as queue_types
from mergify_engine.rules.config import queue_rules as qr_config


@dataclasses.dataclass
class TrainCarStateForSummary:
    outcome: train_car.TrainCarOutcome = train_car.TrainCarOutcome.UNKNOWN
    outcome_message: str | None = None

    @classmethod
    def from_train_car_state(
        cls, train_car_state: "TrainCarState"
    ) -> "TrainCarStateForSummary":
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
        cls, data: "TrainCarStateForSummary.Serialized"
    ) -> "TrainCarStateForSummary":
        return cls(
            outcome=data["outcome"],
            outcome_message=data["outcome_message"],
        )

    @classmethod
    def deserialize_from_summary(
        cls, summary_check: github_types.CachedGitHubCheckRun | None
    ) -> "TrainCarStateForSummary | None":
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
    # NOTE(Syffe): The freeze attribute (frozen_by) is stored solely for reporting reasons
    # It should not be used for other purposes
    frozen_by: freeze.QueueFreeze | None = None
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

    class Serialized(typing.TypedDict):
        outcome: train_car.TrainCarOutcome
        ci_state: queue_types.CiState
        ci_started_at: datetime.datetime | None
        ci_ended_at: datetime.datetime | None
        outcome_message: str | None
        checks_type: train_car.TrainCarChecksType | None
        frozen_by: freeze.QueueFreeze.Serialized | None
        waiting_for_freeze_start_dates: list[datetime.datetime]
        waiting_for_freeze_end_dates: list[datetime.datetime]
        waiting_for_schedule_start_dates: list[datetime.datetime]
        waiting_for_schedule_end_dates: list[datetime.datetime]

    def serialized(self) -> "TrainCarState.Serialized":
        frozen_by = None
        if self.frozen_by is not None:
            frozen_by = self.frozen_by.serialized()

        return self.Serialized(
            outcome=self.outcome,
            ci_state=self.ci_state,
            ci_started_at=self.ci_started_at,
            ci_ended_at=self.ci_ended_at,
            outcome_message=self.outcome_message,
            checks_type=self.checks_type,
            frozen_by=frozen_by,
            waiting_for_freeze_start_dates=self.waiting_for_freeze_start_dates,
            waiting_for_freeze_end_dates=self.waiting_for_freeze_end_dates,
            waiting_for_schedule_start_dates=self.waiting_for_schedule_start_dates,
            waiting_for_schedule_end_dates=self.waiting_for_schedule_end_dates,
        )

    @classmethod
    def deserialize(
        cls,
        repository: context.Repository,
        queue_rules: qr_config.QueueRules,
        data: "TrainCarState.Serialized",
    ) -> "TrainCarState":
        kwargs = {}
        if "waiting_for_freeze_start_dates" in data:
            kwargs["waiting_for_freeze_start_dates"] = data[
                "waiting_for_freeze_start_dates"
            ]

        if "waiting_for_freeze_end_dates" in data:
            kwargs["waiting_for_freeze_end_dates"] = data[
                "waiting_for_freeze_end_dates"
            ]

        if "waiting_for_schedule_start_dates" in data:
            kwargs["waiting_for_schedule_start_dates"] = data[
                "waiting_for_schedule_start_dates"
            ]

        if "waiting_for_schedule_end_dates" in data:
            kwargs["waiting_for_schedule_end_dates"] = data[
                "waiting_for_schedule_end_dates"
            ]

        legacy_creation_date: datetime.datetime | None = data.get("creation_date")  # type: ignore[assignment]

        # backward compatibility following the implementation of "frozen_by" attribute
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

        return cls(
            outcome=data["outcome"],
            ci_state=data["ci_state"],
            ci_started_at=data.get("ci_started_at", legacy_creation_date),
            ci_ended_at=data.get("ci_ended_at"),
            outcome_message=data["outcome_message"],
            checks_type=data["checks_type"],
            frozen_by=frozen_by,
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

    @property
    def seconds_waiting_for_schedule(self) -> int:
        if len(self.waiting_for_schedule_start_dates) - 1 == len(
            self.waiting_for_schedule_end_dates
        ):
            # In this case, that means a PR has been unexpectedly unqueued
            # and the train car did not have time to receive an `update_state`.
            self.waiting_for_schedule_end_dates.append(date.utcnow())

        return self._compute_seconds_waiting_from_lists(
            self.waiting_for_schedule_start_dates,
            self.waiting_for_schedule_end_dates,
        )

    @property
    def seconds_waiting_for_freeze(self) -> int:
        if len(self.waiting_for_freeze_start_dates) - 1 == len(
            self.waiting_for_freeze_end_dates
        ):
            # In this case, that means the queue has been unfrozen but the
            # train car did not have time to receive an `update_state`.
            self.waiting_for_freeze_end_dates.append(date.utcnow())

        return self._compute_seconds_waiting_from_lists(
            self.waiting_for_freeze_start_dates,
            self.waiting_for_freeze_end_dates,
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
        elif lines[0].startswith("<!-- ") and lines[0].endswith(" -->"):
            return lines[0]
    return None
