from __future__ import annotations

import base64
from collections import abc
import dataclasses
import datetime
import enum
import functools
import itertools
import logging
import typing
from urllib import parse
import uuid

import daiquiri
from ddtrace import tracer
import deepdiff
import first
import pydantic
import tenacity

from mergify_engine import branch_updater
from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import queue
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine import worker_pusher
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import user_tokens
from mergify_engine.queue import freeze
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import checks_status
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from mergify_engine.rules import conditions


def build_pr_link(
    repository: context.Repository,
    pull_request_number: github_types.GitHubPullRequestNumber,
    label: str | None = None,
) -> str:
    if label is None:
        label = f"#{pull_request_number}"

    return f"[{label}](/{repository.installation.owner_login}/{repository.repo['name']}/pull/{pull_request_number})"


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class BaseBranchVanished(Exception):
    branch_name: github_types.GitHubRefType


CheckStateT = typing.Literal[
    "success",
    "failure",
    "error",
    "cancelled",
    "skipped",
    "action_required",
    "timed_out",
    "pending",
    "neutral",
    "stale",
]

CHECK_ASSERTS: dict[CheckStateT | None, str] = {
    # green check mark
    "success": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/check-green-16.png",
    # red x
    "failure": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/x-red-16.png",
    "error": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/x-red-16.png",
    "cancelled": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/x-red-16.png",
    "action_required": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/x-red-16.png",
    "timed_out": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/x-red-16.png",
    # yellow dot
    "pending": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/dot-yellow-16.png",
    None: "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/dot-yellow-16.png",
    # grey square
    "neutral": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/square-grey-16.png",
    "skipped": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/square-grey-16.png",
    "stale": "https://raw.githubusercontent.com/Mergifyio/mergify-engine/master/assets/square-grey-16.png",
}

# Lower value will be displayed first
CHECK_SORTING: dict[CheckStateT | None, int] = {
    "failure": 0,
    "error": 0,
    "cancelled": 0,
    "action_required": 0,
    "timed_out": 0,
    "pending": 1,
    None: 1,
    "neutral": 2,
    "skipped": 2,
    "stale": 2,
    "success": 100,
}


def is_base_branch_not_exists_exception(exc: BaseException) -> bool:
    return isinstance(exc, http.HTTPNotFound) and "Base does not exist" in exc.message


class UnexpectedChange:
    pass


class QueueRuleReport(typing.NamedTuple):
    name: str
    summary: str


@dataclasses.dataclass
class UnexpectedDraftPullRequestChange(UnexpectedChange):
    draft_pull_request_number: github_types.GitHubPullRequestNumber

    def __str__(self) -> str:
        return f"the draft pull request #{self.draft_pull_request_number} has sustained unexpected changes from external sources"


@dataclasses.dataclass
class UnexpectedUpdatedPullRequestChange(UnexpectedChange):
    updated_pull_request_number: github_types.GitHubPullRequestNumber

    def __str__(self) -> str:
        return f"the updated pull request #{self.updated_pull_request_number} has been manually updated"


@dataclasses.dataclass
class UnexpectedBaseBranchChange(UnexpectedChange):
    base_sha: github_types.SHAType

    def __str__(self) -> str:
        return f"an external action moved the target branch head to {self.base_sha}"


@dataclasses.dataclass
class TrainCarPullRequestCreationPostponed(Exception):
    car: "TrainCar"


@dataclasses.dataclass
class TrainCarPullRequestCreationFailure(Exception):
    car: "TrainCar"


class EmbarkedPullWithCar(typing.NamedTuple):
    embarked_pull: "EmbarkedPull"
    car: "TrainCar" | None


@dataclasses.dataclass
class EmbarkedPull:
    train: "Train" = dataclasses.field(repr=False)
    user_pull_request_number: github_types.GitHubPullRequestNumber
    config: queue.PullQueueConfig
    queued_at: datetime.datetime

    class Serialized(typing.TypedDict):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime

    class OldSerialized(typing.NamedTuple):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime

    @classmethod
    def deserialize(
        cls,
        train: "Train",
        data: "EmbarkedPull.Serialized" | "EmbarkedPull.OldSerialized",
    ) -> "EmbarkedPull":
        if isinstance(data, (tuple, list)):
            user_pull_request_number = data[0]
            config = data[1]
            queued_at = data[2]

            return cls(
                train=train,
                user_pull_request_number=user_pull_request_number,
                config=config,
                queued_at=queued_at,
            )
        else:
            return cls(
                train=train,
                user_pull_request_number=data["user_pull_request_number"],
                config=data["config"],
                queued_at=data["queued_at"],
            )

    def serialized(self) -> "EmbarkedPull.Serialized":
        return self.Serialized(
            user_pull_request_number=self.user_pull_request_number,
            config=self.config,
            queued_at=self.queued_at,
        )


@pydantic.dataclasses.dataclass
class QueueCheck:
    name: str = dataclasses.field(metadata={"description": "Check name"})
    description: str = dataclasses.field(metadata={"description": "Check description"})
    url: str | None = dataclasses.field(metadata={"description": "Check detail url"})
    state: CheckStateT = dataclasses.field(metadata={"description": "Check state"})
    avatar_url: str | None = dataclasses.field(
        metadata={"description": "Check avatar_url"}
    )

    class Serialized(typing.TypedDict):
        name: str
        description: str
        url: str | None
        state: CheckStateT
        avatar_url: str | None


def get_check_list_ordered(
    checks: list[QueueCheck],
) -> list[QueueCheck]:
    checks_cpy = checks.copy()
    checks_cpy.sort(
        key=lambda c: (
            CHECK_SORTING.get(c.state, CHECK_SORTING["neutral"]),
            c.name,
            c.description,
        )
    )
    return checks_cpy


@dataclasses.dataclass
class DraftPullRequestCreationTemporaryFailure(Exception):
    reason: str


# NOTE(Syffe): type of the TrainCar's checks, can be created by rebasing a PR (inplace)
# or by creating a separate draft PR (draft)
# TODO "failed" needs to be refactored in another way since it is not relevant as a type anymore
@enum.unique
class TrainCarChecksType(enum.Enum):
    INPLACE = "inplace"
    DRAFT = "draft"
    FAILED = "failed"


@enum.unique
class CiState(enum.Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


# FIXME(jd): restore me when the UNKNOWN type is fixed
# @enum.unique
class TrainCarOutcome(enum.Enum):
    MERGEABLE = "mergeable"
    CHECKS_TIMEOUT = "checks_timeout"
    CHECKS_FAILED = "checks_failed"
    BASE_BRANCH_CHANGE = "base_branch_change"
    DRAFT_PR_CHANGE = "draft_pr_change"
    UPDATED_PR_CHANGE = "updated_pr_change"
    UNKNOWN = "unknown"
    BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS = "batch_max_failure_resolution_attempts"
    # FIXME(jd): remove me once all serialization are up to date
    UNKNWON = "unknown"


json.register_type(CiState)
json.register_type(TrainCarOutcome)
json.register_type(TrainCarChecksType)

CHECKS_TIMEOUT_MESSAGE = "The checks have timed out."
CI_FAILED_MESSAGE = "The CI checks have failed."

UNEXPECTED_CHANGE_COMPATIBILITY = {
    UnexpectedDraftPullRequestChange: TrainCarOutcome.DRAFT_PR_CHANGE,
    UnexpectedBaseBranchChange: TrainCarOutcome.BASE_BRANCH_CHANGE,
    UnexpectedUpdatedPullRequestChange: TrainCarOutcome.UPDATED_PR_CHANGE,
}


@dataclasses.dataclass
class TrainCarStateForSummary:
    outcome: TrainCarOutcome = TrainCarOutcome.UNKNOWN
    outcome_message: str | None = None

    @classmethod
    def from_train_car_state(
        cls, train_car_state: "TrainCarState"
    ) -> "TrainCarStateForSummary":
        return cls(train_car_state.outcome, train_car_state.outcome_message)

    class Serialized(typing.TypedDict):
        outcome: TrainCarOutcome
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
    ) -> "TrainCarStateForSummary" | None:
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
    outcome: TrainCarOutcome = TrainCarOutcome.UNKNOWN
    ci_state: CiState = CiState.PENDING
    ci_started_at: datetime.datetime | None = None
    ci_ended_at: datetime.datetime | None = None
    outcome_message: str | None = None
    checks_type: TrainCarChecksType | None = None
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
        outcome: TrainCarOutcome
        ci_state: CiState
        ci_started_at: datetime.datetime | None
        ci_ended_at: datetime.datetime | None
        outcome_message: str | None
        checks_type: TrainCarChecksType | None
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


@dataclasses.dataclass
class TrainCar:
    train: "Train" = dataclasses.field(repr=False)
    train_car_state: "TrainCarState" = dataclasses.field(repr=False)
    initial_embarked_pulls: list[EmbarkedPull]
    still_queued_embarked_pulls: list[EmbarkedPull]
    parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    initial_current_base_sha: github_types.SHAType
    queue_pull_request_number: None | (
        github_types.GitHubPullRequestNumber
    ) = dataclasses.field(default=None)
    failure_history: list["TrainCar"] = dataclasses.field(
        default_factory=list, repr=False
    )
    head_branch: str | None = None
    last_checks: list[QueueCheck] = dataclasses.field(default_factory=list)
    last_conditions_evaluation: conditions.QueueConditionEvaluationResult | None = None
    checks_ended_timestamp: datetime.datetime | None = None
    queue_branch_name: github_types.GitHubRefType | None = None

    QUEUE_BRANCH_PREFIX: typing.ClassVar[str] = "tmp-"

    class Serialized(typing.TypedDict):
        train_car_state: TrainCarState.Serialized
        initial_embarked_pulls: list[EmbarkedPull.Serialized]
        still_queued_embarked_pulls: list[EmbarkedPull.Serialized]
        parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
        initial_current_base_sha: github_types.SHAType
        queue_pull_request_number: github_types.GitHubPullRequestNumber | None
        failure_history: list[TrainCar.Serialized]
        head_branch: str | None
        last_checks: list[QueueCheck.Serialized]
        last_conditions_evaluation: conditions.QueueConditionEvaluationResult.Serialized | None
        checks_ended_timestamp: datetime.datetime | None
        queue_branch_name: github_types.GitHubRefType | None

    def serialized(self) -> "TrainCar.Serialized":
        if self.last_conditions_evaluation is not None:
            last_conditions_evaluation = self.last_conditions_evaluation.serialized()
        else:
            last_conditions_evaluation = None

        return self.Serialized(
            train_car_state=self.train_car_state.serialized(),
            initial_embarked_pulls=[
                ep.serialized() for ep in self.initial_embarked_pulls
            ],
            still_queued_embarked_pulls=[
                ep.serialized() for ep in self.still_queued_embarked_pulls
            ],
            parent_pull_request_numbers=self.parent_pull_request_numbers,
            initial_current_base_sha=self.initial_current_base_sha,
            queue_pull_request_number=self.queue_pull_request_number,
            failure_history=[fh.serialized() for fh in self.failure_history],
            head_branch=self.head_branch,
            last_checks=[
                typing.cast(
                    QueueCheck.Serialized,
                    dataclasses.asdict(c),
                )
                for c in self.last_checks
            ],
            last_conditions_evaluation=last_conditions_evaluation,
            checks_ended_timestamp=self.checks_ended_timestamp,
            queue_branch_name=self.queue_branch_name,
        )

    @classmethod
    def deserialize(
        cls,
        train: "Train",
        data: "TrainCar.Serialized",
    ) -> "TrainCar":
        # Avoid circular import
        from mergify_engine.rules import conditions

        if "initial_embarked_pulls" in data:
            initial_embarked_pulls = [
                EmbarkedPull.deserialize(train, ep)
                for ep in data["initial_embarked_pulls"]
            ]
            still_queued_embarked_pulls = [
                EmbarkedPull.deserialize(train, ep)
                for ep in data["still_queued_embarked_pulls"]
            ]

        else:
            # old format
            initial_embarked_pulls = [
                EmbarkedPull(
                    train,
                    data["user_pull_request_number"],
                    data["config"],
                    data["queued_at"],
                )
            ]
            still_queued_embarked_pulls = initial_embarked_pulls.copy()

        checks_type: TrainCarChecksType | None = None
        if "creation_state" in data:
            if data["creation_state"] == "updated":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.INPLACE
            elif data["creation_state"] == "created":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.DRAFT
            elif data["creation_state"] == "failed":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.FAILED
        elif "state" in data:
            if data["state"] == "updated":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.INPLACE
            elif data["state"] == "created":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.DRAFT
            elif data["state"] == "failed":  # type: ignore[typeddict-item]
                checks_type = TrainCarChecksType.FAILED
        elif "train_car_state" in data:
            checks_type = data["train_car_state"]["checks_type"]

        if "failure_history" in data:
            failure_history = [
                TrainCar.deserialize(train, fh) for fh in data["failure_history"]
            ]
        else:
            failure_history = []

        if "last_checks" in data:
            last_checks = [QueueCheck(**c) for c in data["last_checks"]]
        else:
            last_checks = []

        if (
            checks_type == TrainCarChecksType.INPLACE
            and data["queue_pull_request_number"] is None
        ):
            data["queue_pull_request_number"] = still_queued_embarked_pulls[
                0
            ].user_pull_request_number

        if "head_branch" not in data:
            if checks_type == TrainCarChecksType.DRAFT:
                data["head_branch"] = cls._get_pulls_branch_ref(
                    initial_embarked_pulls,
                    data["parent_pull_request_numbers"],
                )
            else:
                data["head_branch"] = None

        # Retrocompatibility
        if "queue_branch_name" not in data:
            data["queue_branch_name"] = github_types.GitHubRefType(
                f"{constants.MERGE_QUEUE_BRANCH_PREFIX}{train.ref}/{cls._get_pulls_branch_ref(initial_embarked_pulls)}"
            )

        # NOTE(Syffe): Backward compatibility for old TrainCar without TrainCarState attribute
        # (Released in version 6.0)
        train_car_state = cls._deserialize_train_car_state(
            train.repository, train.queue_rules, data, checks_type
        )

        if (
            "last_conditions_evaluation" in data
            and data["last_conditions_evaluation"] is not None
        ):
            last_conditions_evaluation = (
                conditions.QueueConditionEvaluationResult.deserialize(
                    data["last_conditions_evaluation"]
                )
            )
        else:
            last_conditions_evaluation = None

        return cls(
            train,
            train_car_state=train_car_state,
            initial_embarked_pulls=initial_embarked_pulls,
            still_queued_embarked_pulls=still_queued_embarked_pulls,
            parent_pull_request_numbers=data["parent_pull_request_numbers"],
            initial_current_base_sha=data["initial_current_base_sha"],
            queue_pull_request_number=data["queue_pull_request_number"],
            failure_history=failure_history,
            head_branch=data["head_branch"],
            last_checks=last_checks,
            last_conditions_evaluation=last_conditions_evaluation,
            checks_ended_timestamp=data.get("checks_ended_timestamp"),
            queue_branch_name=data["queue_branch_name"],
        )

    @classmethod
    def _deserialize_train_car_state(
        cls,
        repository: context.Repository,
        queue_rules: qr_config.QueueRules,
        data: "TrainCar.Serialized",
        checks_type: TrainCarChecksType | None,
    ) -> "TrainCarState":
        if "train_car_state" in data:
            return TrainCarState.deserialize(
                repository, queue_rules, data["train_car_state"]
            )
        else:
            outcome = TrainCarOutcome.UNKNOWN
            ci_state = CiState.PENDING
            outcome_message = ""
            legacy_queue_conditions_conclusion = check_api.Conclusion.PENDING

            if "checks_conclusion" in data:
                legacy_queue_conditions_conclusion = data["checks_conclusion"]

            if "has_timed_out" in data and data["has_timed_out"]:
                outcome = TrainCarOutcome.CHECKS_TIMEOUT
                outcome_message = CHECKS_TIMEOUT_MESSAGE

            if "ci_has_passed" in data:
                if data["ci_has_passed"]:
                    ci_state = CiState.SUCCESS
                elif legacy_queue_conditions_conclusion == check_api.Conclusion.FAILURE:
                    ci_state = CiState.FAILED
                    if outcome == TrainCarOutcome.UNKNOWN:
                        outcome = TrainCarOutcome.CHECKS_FAILED
                        outcome_message = CI_FAILED_MESSAGE

            if (
                legacy_queue_conditions_conclusion == check_api.Conclusion.SUCCESS
                and ci_state == CiState.SUCCESS
                and outcome == TrainCarOutcome.UNKNOWN
            ):
                outcome = TrainCarOutcome.MERGEABLE

            if "creation_date" in data:
                creation_date = data["creation_date"]
            else:
                creation_date = date.utcnow()

            return TrainCarState(
                outcome=outcome,
                ci_state=ci_state,
                outcome_message=outcome_message,
                checks_type=checks_type,
                ci_started_at=creation_date,
            )

    @property
    def is_batch_failure_resolved(self) -> bool:
        return (
            self.has_previous_car_status_succeeded()
            and len(self.initial_embarked_pulls) == 1
        )

    @property
    def last_evaluated_conditions(self) -> str:
        if self.last_conditions_evaluation is not None:
            return self.last_conditions_evaluation.as_markdown()
        else:
            return ""

    def can_be_interrupted(self) -> bool:
        return self.train_car_state.outcome == TrainCarOutcome.UNKNOWN or (
            self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED
            and not self.is_batch_failure_resolved
        )

    def get_queue_check_run_conclusion(self) -> check_api.Conclusion:
        # Set the state report on GitHub for the `Queue:` check-runs, so the action known
        # if the PR need to merge, removed from the queue.
        if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            return check_api.Conclusion.SUCCESS
        elif self.train_car_state.outcome in (
            TrainCarOutcome.UNKNOWN,
            TrainCarOutcome.BASE_BRANCH_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
        ):
            return check_api.Conclusion.PENDING
        elif self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_TIMEOUT,
            TrainCarOutcome.DRAFT_PR_CHANGE,
            TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS,
        ):
            return check_api.Conclusion.FAILURE
        elif self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED:
            if self.is_batch_failure_resolved:
                return check_api.Conclusion.FAILURE
            else:
                return check_api.Conclusion.PENDING
        else:
            raise RuntimeError(
                f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
            )

    def _generate_draft_pr_branch_suffix(self) -> str:
        namespace_bytes = self.train.ref.encode()
        if len(namespace_bytes) > 16:
            namespace_bytes = namespace_bytes[:16]
        elif len(namespace_bytes) < 16:
            namespace_bytes = namespace_bytes + (b"\x00" * (16 - len(namespace_bytes)))

        namespace = uuid.UUID(bytes=namespace_bytes)

        name = self._get_pulls_branch_ref(
            self.initial_embarked_pulls, self.parent_pull_request_numbers
        )

        return uuid.uuid5(namespace, name).hex[:10]

    def _get_user_refs(
        self,
        create_link: bool = True,
        embarked_pulls: list[EmbarkedPull] | None = None,
    ) -> str:
        if embarked_pulls is None:
            embarked_pulls = self.initial_embarked_pulls
        refs = [
            build_pr_link(self.train.repository, ep.user_pull_request_number)
            if create_link
            else f"#{ep.user_pull_request_number}"
            for ep in embarked_pulls
        ]
        if len(refs) == 1:
            return refs[0]
        else:
            return f"[{' + '.join(refs)}]"

    def _get_embarked_refs(
        self, include_my_self: bool = True, markdown: bool = False
    ) -> str:
        if markdown:
            refs = [
                f"Branch **{self.train.ref}** ({self.initial_current_base_sha[:7]})"
            ]
        else:
            refs = [f"{self.train.ref} ({self.initial_current_base_sha[:7]})"]

        refs += [
            build_pr_link(self.train.repository, p) if markdown else f"#{p}"
            for p in self.parent_pull_request_numbers
        ]

        if include_my_self:
            return f"{', '.join(refs)} and {self._get_user_refs(create_link=markdown)}"
        elif len(refs) == 1:
            return refs[-1]
        else:
            return f"{', '.join(refs[:-1])} and {refs[-1]}"

    async def get_pull_requests_to_evaluate(self) -> list[context.BasePullRequest]:
        if self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
        ):
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's spec checks type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            tmp_ctxt = await self.train.repository.get_pull_request_context(
                self.queue_pull_request_number
            )
            return [
                context.QueuePullRequest(
                    await self.train.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    ),
                    tmp_ctxt,
                )
                for ep in self.still_queued_embarked_pulls
            ]
        elif self.train_car_state.checks_type == TrainCarChecksType.FAILED:
            # Will be splitted or dropped soon
            return [
                (
                    await self.train.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    )
                ).pull_request
                for ep in self.still_queued_embarked_pulls
            ]
        else:
            raise RuntimeError(
                f"Invalid spec checks type: {self.train_car_state.checks_type}"
            )

    async def get_context_to_evaluate(self) -> context.Context | None:
        if self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
        ):
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's spec check type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            return await self.train.repository.get_pull_request_context(
                self.queue_pull_request_number
            )
        else:
            return None

    def get_queue_name(self) -> "qr_config.QueueName":
        return self.initial_embarked_pulls[0].config["name"]

    async def is_behind(self) -> bool:
        ctxt = await self.train.repository.get_pull_request_context(
            self.still_queued_embarked_pulls[0].user_pull_request_number
        )
        return await ctxt.is_behind

    @tracer.wrap("TrainCar.start_inplace_checks", span_type="worker")
    async def start_checking_inplace(self) -> None:
        if len(self.still_queued_embarked_pulls) != 1:
            raise RuntimeError("multiple embarked_pulls but state==updated")

        embarked_pull = self.still_queued_embarked_pulls[0]

        ctxt = await self.train.repository.get_pull_request_context(
            embarked_pull.user_pull_request_number
        )

        if await ctxt.is_behind:
            bot_account = self.still_queued_embarked_pulls[0].config.get(
                "update_bot_account"
            )
            github_user: user_tokens.UserTokensUser | None = None
            if bot_account:
                tokens = await ctxt.repository.installation.get_user_tokens()
                github_user = tokens.get_token_for(bot_account)
                if not github_user:
                    await self._set_creation_failure(
                        f"Unable to update: user `{bot_account}` is unknown.\n\n"
                        f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                        operation="updated",
                    )
                    raise TrainCarPullRequestCreationFailure(self)

            # TODO(sileht): fallback to "merge" and None until all configs has
            # the new fields
            method = self.still_queued_embarked_pulls[0].config.get(
                "update_method", "merge"
            )
            try:
                if method == "merge":
                    # FIXME(sileht): we should have passed on_behalf to update_with_api ...
                    # MRGFY-1742
                    await branch_updater.update_with_api(ctxt)
                else:
                    # FIXME(sileht): should we enabled autosquash here? MRGFY-279
                    await branch_updater.rebase_with_git(ctxt, github_user, False)
            except branch_updater.BranchUpdateFailure as exc:
                await self._set_creation_failure(
                    f"{exc.title}\n\n{exc.message}", operation="updated"
                )
                raise TrainCarPullRequestCreationFailure(self) from exc

            # NOTE(sileht): We must update head_sha of the pull request otherwise
            # next temporary pull request may be created on a vanished reference.
            await ctxt.update()

        else:
            # Already done, just refresh it to merge it
            await refresher.send_pull_refresh(
                self.train.repository.installation.redis.stream,
                ctxt.pull["base"]["repo"],
                pull_request_number=ctxt.pull["number"],
                action="internal",
                source="updated pull need to be merge",
                priority=worker_pusher.Priority.immediate,
            )

        await self._set_initial_state(
            TrainCarChecksType.INPLACE,
            embarked_pull.user_pull_request_number,
        )

    @staticmethod
    def _get_pulls_branch_ref(
        embarked_pulls: list[EmbarkedPull],
        parent_pr_numbers: None | (list[github_types.GitHubPullRequestNumber]) = None,
    ) -> str:
        pr_numbers = [ep.user_pull_request_number for ep in embarked_pulls]
        if parent_pr_numbers:
            pr_numbers += parent_pr_numbers

        pr_numbers_str = list(map(str, sorted(pr_numbers)))
        return "-".join(pr_numbers_str)

    @tracer.wrap("TrainCar._create_draft_pull_request", span_type="worker")
    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            DraftPullRequestCreationTemporaryFailure
        ),
        stop=tenacity.stop_after_attempt(2),
    )
    async def _create_draft_pull_request(
        self,
        branch_name: github_types.GitHubRefType,
        github_user: user_tokens.UserTokensUser | None,
    ) -> github_types.GitHubPullRequest:
        try:
            title = f"merge queue: embarking {self._get_embarked_refs()} together"
            body = await self.generate_merge_queue_summary(for_queue_pull_request=True)
            response = await self.train.repository.installation.client.post(
                f"/repos/{self.train.repository.installation.owner_login}/{self.train.repository.repo['name']}/pulls",
                json={
                    "title": title,
                    "body": body,
                    "base": self.train.ref,
                    "head": branch_name,
                    "draft": True,
                },
                oauth_token=github_user["oauth_access_token"] if github_user else None,
            )
        except http.HTTPClientSideError as e:
            if "A pull request already exists for" in e.message:
                # NOTE(sileht): filter pull request on head is dangerous.
                # head must be organization:ref-name, if the left or the right side of the : is empty
                # all pull requests are returned. So it's better to double checks
                if not (self.train.repository.installation.owner_login and branch_name):
                    raise RuntimeError("Invalid merge queue head branch")

                head = f"{self.train.repository.installation.owner_login}:{branch_name}"
                closed_pulls = set()
                async for pull in typing.cast(
                    abc.AsyncIterator[github_types.GitHubPullRequest],
                    self.train.repository.installation.client.items(
                        f"/repos/{self.train.repository.installation.owner_login}/{self.train.repository.repo['name']}/pulls",
                        params={"head": head},
                        resource_name="pulls",
                        page_limit=20,
                    ),
                ):
                    closed_pulls.add(pull["number"])
                    await self.train._close_pull_request(pull["number"])

                self.train.log.info(
                    "fail to create a merge queue pull request, because the pull request already exists.",
                    head=branch_name,
                    title=title,
                    github_user=github_user["login"] if github_user else None,
                    parent_pull_request_numbers=self.parent_pull_request_numbers,
                    still_queued_embarked_pull_numbers=[
                        ep.user_pull_request_number
                        for ep in self.still_queued_embarked_pulls
                    ],
                    exc_info=True,
                    closed_pulls=closed_pulls,
                )
                raise DraftPullRequestCreationTemporaryFailure(e.message)

            if "Draft pull requests are not supported" not in e.message:
                self.train.log.error(
                    "fail to create a merge queue pull request",
                    head=branch_name,
                    title=title,
                    github_user=github_user["login"] if github_user else None,
                    parent_pull_request_numbers=self.parent_pull_request_numbers,
                    still_queued_embarked_pull_numbers=[
                        ep.user_pull_request_number
                        for ep in self.still_queued_embarked_pulls
                    ],
                    exc_info=True,
                )

            await self._set_creation_failure(e.message, report_as_error=True)
            raise TrainCarPullRequestCreationFailure(self) from e

        else:
            return typing.cast(github_types.GitHubPullRequest, response.json())

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(tenacity.TryAgain),
        stop=tenacity.stop_after_attempt(2),
        reraise=True,
    )
    async def _prepare_draft_pr_branch(
        self,
        branch_name: github_types.GitHubRefType,
        base_sha: github_types.SHAType,
        github_user: user_tokens.UserTokensUser | None,
    ) -> None:
        try:
            await self.train.repository.installation.client.post(
                f"/repos/{self.train.repository.installation.owner_login}/{self.train.repository.repo['name']}/git/refs",
                oauth_token=github_user["oauth_access_token"] if github_user else None,
                json={
                    "ref": f"refs/heads/{branch_name}",
                    "sha": base_sha,
                },
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 422 and "Reference already exists" in exc.message:
                try:
                    await self._delete_branch()
                except http.HTTPClientSideError as exc_patch:
                    await self._set_creation_failure(exc_patch.message)
                    raise TrainCarPullRequestCreationFailure(self) from exc_patch

                raise tenacity.TryAgain
            else:
                await self._set_creation_failure(exc.message, report_as_error=True)
                raise TrainCarPullRequestCreationFailure(self) from exc

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(tenacity.TryAgain),
        stop=tenacity.stop_after_attempt(2),
        reraise=True,
    )
    async def _rename_branch(
        self,
        current_branch_name: github_types.GitHubRefType,
        new_branch_name: github_types.GitHubRefType,
        github_user: user_tokens.UserTokensUser | None,
    ) -> None:
        try:
            await self.train.repository.installation.client.post(
                f"/repos/{self.train.repository.installation.owner_login}/{self.train.repository.repo['name']}/branches/{current_branch_name}/rename",
                oauth_token=github_user["oauth_access_token"] if github_user else None,
                json={"new_name": new_branch_name},
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 404:
                # NOTE(sileht): the merge queue we just created is missing ???, just retry just in case
                raise tenacity.TryAgain

            elif exc.status_code == 422 and "New branch already exists" in exc.message:
                try:
                    await self._delete_branch()
                except http.HTTPClientSideError as exc_patch:
                    await self._set_creation_failure(exc_patch.message)
                    raise TrainCarPullRequestCreationFailure(self) from exc_patch

            else:
                await self._set_creation_failure(exc.message, report_as_error=True)
                raise TrainCarPullRequestCreationFailure(self) from exc

    async def _get_draft_pr_setup(
        self,
        queue_rule: "qr_config.QueueRule",
        previous_car: "TrainCar | None",
    ) -> tuple[github_types.SHAType, list[github_types.GitHubPullRequestNumber]]:
        pulls_in_draft = []
        queue_branch_merge_method = queue_rule.config["queue_branch_merge_method"]
        if queue_branch_merge_method is None:
            pulls_in_draft += self.parent_pull_request_numbers
            base_sha = self.initial_current_base_sha
        elif queue_branch_merge_method == "fast-forward":
            if previous_car is None:
                base_sha = self.initial_current_base_sha
            elif previous_car.train_car_state.checks_type in (
                TrainCarChecksType.INPLACE,
                TrainCarChecksType.DRAFT,
            ):
                if previous_car.queue_pull_request_number is None:
                    raise RuntimeError("previous_car without queue_pull_request_number")
                ctxt = await self.train.repository.get_pull_request_context(
                    previous_car.queue_pull_request_number
                )
                base_sha = ctxt.pull["head"]["sha"]
            else:
                raise RuntimeError(
                    f"previous_car with invalid spec checks type: {previous_car.train_car_state.checks_type}"
                )
        else:
            raise RuntimeError(
                f"invalid queue_branch_merge_method: {queue_branch_merge_method}"
            )

        pulls_in_draft += [
            ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
        ]
        return base_sha, pulls_in_draft

    @tracer.wrap("TrainCar.start_checking_with_draft", span_type="worker")
    async def start_checking_with_draft(self, previous_car: "TrainCar | None") -> None:
        queue_rule = self.get_queue_rule()
        self.head_branch = self._get_pulls_branch_ref(
            self.initial_embarked_pulls, self.parent_pull_request_numbers
        )
        if self.queue_branch_name is None:
            self.queue_branch_name = github_types.GitHubRefType(
                f"{queue_rule.config['queue_branch_prefix']}{self._generate_draft_pr_branch_suffix()}"
            )

        bot_account = queue_rule.config["draft_bot_account"]
        github_user: user_tokens.UserTokensUser | None = None
        if bot_account:
            tokens = await self.train.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                await self._set_creation_failure(
                    f"Unable to create draft pull request: user `{bot_account}` is unknown. "
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )
                raise TrainCarPullRequestCreationFailure(self)

        base_sha, pulls_in_draft = await self._get_draft_pr_setup(
            queue_rule, previous_car
        )

        self.queue_branch_name = github_types.GitHubRefType(
            f"{self.QUEUE_BRANCH_PREFIX}{self.queue_branch_name}"
        )
        await self._prepare_draft_pr_branch(
            self.queue_branch_name, base_sha, github_user
        )

        for pull_number in pulls_in_draft:
            try:
                await self.train.repository.installation.client.post(
                    f"/repos/{self.train.repository.installation.owner_login}/{self.train.repository.repo['name']}/merges",
                    oauth_token=github_user["oauth_access_token"]
                    if github_user
                    else None,
                    json={
                        "base": self.queue_branch_name,
                        "head": f"refs/pull/{pull_number}/head",
                        "commit_message": f"Merge of #{pull_number}",
                    },
                )
            except http.HTTPClientSideError as e:
                if (
                    e.status_code == 403
                    and "Resource not accessible by integration" in e.message
                ):
                    self.train.log.info(
                        "fail to create the queue pull request due to GitHub App restriction",
                        embarked_pulls=[
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                        error_message=e.message,
                    )
                    await self._delete_branch()
                    raise TrainCarPullRequestCreationPostponed(self) from e
                elif "Merge conflict" in e.message:
                    pull_requests_ahead = self.parent_pull_request_numbers[:]
                    for ep in self.still_queued_embarked_pulls:
                        if ep.user_pull_request_number == pull_number:
                            break
                        pull_requests_ahead.append(ep.user_pull_request_number)
                    message = "The pull request conflicts with at least one pull request ahead in queue: "
                    message += ", ".join([f"#{p}" for p in pull_requests_ahead])
                    await self._set_creation_failure(
                        message, pull_requests_to_remove=[pull_number]
                    )
                    await self._delete_branch()
                    raise TrainCarPullRequestCreationFailure(self) from e
                else:
                    await self._set_creation_failure(
                        e.message,
                        pull_requests_to_remove=[pull_number],
                        report_as_error=True,
                    )
                    await self._delete_branch()
                    raise TrainCarPullRequestCreationFailure(self) from e

        new_branch_name = github_types.GitHubRefType(
            self.queue_branch_name.replace(self.QUEUE_BRANCH_PREFIX, "", 1)
        )
        await self._rename_branch(self.queue_branch_name, new_branch_name, github_user)
        self.queue_branch_name = new_branch_name

        try:
            tmp_pull = await self._create_draft_pull_request(
                self.queue_branch_name,
                github_user,
            )
        except DraftPullRequestCreationTemporaryFailure as e:
            await self._delete_branch()
            raise TrainCarPullRequestCreationPostponed(self) from e
        await self._set_initial_state(TrainCarChecksType.DRAFT, tmp_pull["number"])

    async def _set_initial_state(
        self,
        checks_type: typing.Literal[
            TrainCarChecksType.INPLACE, TrainCarChecksType.DRAFT
        ],
        pull_request_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        self.train_car_state.ci_started_at = date.utcnow()
        self.train_car_state.checks_type = checks_type
        self.queue_pull_request_number = pull_request_number

        queue_rule = self.get_queue_rule()
        queue_pull_requests = await self.get_pull_requests_to_evaluate()
        evaluated_queue_rule = await queue_rule.get_evaluated_queue_rule(
            self.train.repository,
            self.train.ref,
            queue_pull_requests,
        )
        await self.update_state(check_api.Conclusion.PENDING, evaluated_queue_rule)
        await self.update_summaries()
        await self.send_refresh_to_user_pull_requests()

        for ep in self.still_queued_embarked_pulls:
            position, _ = self.train.find_embarked_pull(ep.user_pull_request_number)
            if position is None:
                raise RuntimeError("TrainCar with embarked_pull not in train...")

            await signals.send(
                self.train.repository,
                ep.user_pull_request_number,
                "action.queue.checks_start",
                signals.EventQueueChecksStartMetadata(
                    {
                        "queue_name": ep.config["name"],
                        "branch": self.train.ref,
                        "position": position,
                        "queued_at": ep.queued_at,
                        "speculative_check_pull_request": {
                            "number": self.queue_pull_request_number,
                            "in_place": self.train_car_state.checks_type
                            == TrainCarChecksType.INPLACE,
                            "checks_conclusion": self.get_queue_check_run_conclusion().value
                            or "pending",
                            "checks_timed_out": self.train_car_state.outcome
                            == TrainCarOutcome.CHECKS_TIMEOUT,
                            "checks_ended_at": self.checks_ended_timestamp,
                        },
                    }
                ),
                "merge queue internal",
            )

    async def generate_merge_queue_summary(
        self,
        *,
        for_queue_pull_request: bool = False,
        show_queue: bool = True,
        headline: str | None = None,
        pull_rule: "prr_config.EvaluatedPullRequestRule" | None = None,
    ) -> str:
        description = ""
        if headline:
            description += f"**{headline}**\n\n"

        description += (
            f"{self._get_embarked_refs(markdown=True)} are embarked together for merge."
        )

        if for_queue_pull_request:
            description += f"""

This pull request has been created by Mergify to speculatively check the mergeability of {self._get_user_refs()}.
You don't need to do anything. Mergify will close this pull request automatically when it is complete.
"""

        description += await self.train.generate_merge_queue_summary_footer(
            queue_rule_report=QueueRuleReport(
                self.still_queued_embarked_pulls[0].config["name"],
                self.last_evaluated_conditions,
            ),
            pull_rule=pull_rule,
            show_queue=show_queue,
            for_queue_pull_request=for_queue_pull_request,
        )

        if for_queue_pull_request:
            description += "\n\n"
            description += await self.generate_yaml_infos()

        return description.strip()

    async def generate_yaml_infos(self) -> str:
        yaml_dict = {
            "pull_requests": [
                {"number": ep.user_pull_request_number}
                for ep in self.initial_embarked_pulls
            ],
        }
        description = "```yaml\n---\n"
        # TODO(sileht): use regular dumper, to use the C parser
        description += yaml.dump_with_indented_list(yaml_dict)
        description += "...\n\n```"

        return description

    async def end_checking(
        self,
        reason: queue_utils.BaseUnqueueReason | None,
        not_reembarked_pull_requests: dict[
            github_types.GitHubPullRequestNumber, queue_utils.BaseUnqueueReason
        ],
    ) -> None:
        if self.queue_pull_request_number is None:
            return

        remaning_embarked_pulls = [
            ep
            for ep in self.initial_embarked_pulls
            if ep.user_pull_request_number not in not_reembarked_pull_requests
        ]

        if self.train_car_state.checks_type == TrainCarChecksType.INPLACE:
            return

        elif self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
            if reason is not None:
                await self._set_final_draft_pr_summary(reason, remaning_embarked_pulls)
            await self._delete_branch()

    async def _set_final_draft_pr_summary(
        self,
        reason: queue_utils.BaseUnqueueReason | None,
        reembarked_pulls: list[EmbarkedPull],
    ) -> None:
        if (
            self.train_car_state.checks_type != TrainCarChecksType.DRAFT
            or self.queue_pull_request_number is None
        ):
            raise RuntimeError("can be called only on draft pr")
        tmp_pull_ctxt = await self.train.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        summary = await tmp_pull_ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        if (
            summary is None
            or summary["conclusion"] == check_api.Conclusion.PENDING.value
        ):
            headline = f"✨ {reason}."
            if reembarked_pulls:
                headline += f" The pull request {self._get_user_refs(embarked_pulls=reembarked_pulls)} has been re-embarked."
            headline += " ✨"

            body = await self.generate_merge_queue_summary(
                for_queue_pull_request=True,
                headline=headline,
                show_queue=False,
            )

            if tmp_pull_ctxt.body != body:
                await tmp_pull_ctxt.client.patch(
                    f"{tmp_pull_ctxt.base_url}/pulls/{self.queue_pull_request_number}",
                    json={"body": body},
                )

            await tmp_pull_ctxt.set_summary_check(
                check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    title=f"The pull request {self._get_user_refs(create_link=False)} has been re-embarked for merge",
                    summary=headline,
                )
            )
            tmp_pull_ctxt.log.info("train car deleted", reason=reason)

    async def send_checks_end_signal(
        self,
        user_pull_request_number: github_types.GitHubPullRequestNumber,
        unqueue_reason: queue_utils.BaseUnqueueReason,
        abort_status: typing.Literal["DEFINITIVE", "REEMBARKED"],
    ) -> None:
        if (
            self.queue_pull_request_number is None
            or self.train_car_state.ci_started_at is None
        ):
            # NOTE(sileht): Maybe add something to eventlog here?
            return

        position, ep_with_car = self.train.find_embarked_pull(user_pull_request_number)
        if ep_with_car is None or position is None:
            raise RuntimeError("Sending signal for a none embarked pull request")

        ep = ep_with_car.embarked_pull
        abort_code: queue_utils.AbortCodeT | None
        if isinstance(unqueue_reason, queue_utils.PrMerged):
            aborted = False
            abort_reason_str = ""
            abort_code = None
        else:
            aborted = True
            abort_reason_str = str(unqueue_reason)
            abort_code = typing.cast(
                queue_utils.AbortCodeT, unqueue_reason.unqueue_code
            )

        metadata = signals.EventQueueChecksEndMetadata(
            {
                "aborted": aborted,
                "abort_reason": abort_reason_str,
                "abort_code": abort_code,
                "abort_status": abort_status,
                "queue_name": ep.config["name"],
                "branch": self.train.ref,
                "position": position,
                "queued_at": ep.queued_at,
                "speculative_check_pull_request": {
                    "number": self.queue_pull_request_number,
                    "in_place": self.train_car_state.checks_type
                    == TrainCarChecksType.INPLACE,
                    "checks_conclusion": self.get_queue_check_run_conclusion().value
                    or "pending",
                    "checks_timed_out": self.train_car_state.outcome
                    == TrainCarOutcome.CHECKS_TIMEOUT,
                    "checks_ended_at": self.checks_ended_timestamp,
                    "checks_started_at": self.train_car_state.ci_started_at,
                },
            }
        )

        await signals.send(
            self.train.repository,
            ep.user_pull_request_number,
            "action.queue.checks_end",
            metadata,
            "merge queue internal",
        )

    async def _delete_branch(self) -> None:
        if self.queue_pull_request_number is not None:
            await self.train._close_pull_request(self.queue_pull_request_number)

        if self.queue_branch_name is not None:
            await self.train.repository.delete_branch_if_exists(self.queue_branch_name)

    async def _set_creation_failure(
        self,
        details: str,
        *,
        operation: typing.Literal["created", "updated"] = "created",
        pull_requests_to_remove: None
        | (list[github_types.GitHubPullRequestNumber]) = None,
        report_as_error: bool = False,
    ) -> None:
        self.train_car_state.checks_type = TrainCarChecksType.FAILED

        title = "This pull request cannot be embarked for merge"

        if self.queue_pull_request_number is None:
            summary = f"The merge queue pull request can't be {operation}"
        else:
            summary = f"The merge queue pull request (#{self.queue_pull_request_number}) can't be prepared"

        # Append a `>` after a double newlines because otherwise
        # the quote breaks.
        details_as_markdown = details.replace("\n\n", "\n\n>")
        summary += f"""\nDetails:
> {details_as_markdown}
"""

        if pull_requests_to_remove is None:
            embarked_pulls_to_remove = self.still_queued_embarked_pulls
        else:
            embarked_pulls_to_remove = [
                embarked_pull
                for embarked_pull in self.still_queued_embarked_pulls
                if embarked_pull.user_pull_request_number in pull_requests_to_remove
            ]

        log_level = (
            logging.ERROR
            if report_as_error or not embarked_pulls_to_remove
            else logging.INFO
        )
        self.train.log.log(
            log_level,
            "train car creation failed",
            gh_pull=self.queue_pull_request_number,
            gh_pulls_queued=[
                ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
            ],
            gh_pulls_initially_queued=[
                ep.user_pull_request_number for ep in self.initial_embarked_pulls
            ],
            operation=operation,
            title=title,
            summary=summary,
            pull_requests_to_remove=pull_requests_to_remove,
            details=details,
            exc_info=True,
        )

        # Set the error on the original Pull Requests
        for embarked_pull in embarked_pulls_to_remove:
            original_ctxt = await self.train.repository.get_pull_request_context(
                embarked_pull.user_pull_request_number
            )
            original_ctxt.log.info(
                "pull request cannot be embarked for merge",
                conclusion=check_api.Conclusion.ACTION_REQUIRED,
                title=title,
                summary=summary,
                details=details,
                exc_info=True,
            )
            await check_api.set_check_run(
                original_ctxt,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    title=title,
                    summary=summary,
                ),
            )

            await refresher.send_pull_refresh(
                self.train.repository.installation.redis.stream,
                original_ctxt.pull["base"]["repo"],
                pull_request_number=original_ctxt.pull["number"],
                action="internal",
                source="draft pull creation error",
                priority=worker_pusher.Priority.immediate,
            )

    def _get_conditions_without_checks(
        cls,
        evaluated_queue_rule: "qr_config.EvaluatedQueueRule",
    ) -> "conditions.QueueRuleMergeConditions":
        conditions_with_only_checks = evaluated_queue_rule.merge_conditions.copy()
        for condition_with_only_checks in conditions_with_only_checks.walk():
            attr = condition_with_only_checks.get_attribute_name()
            if not attr.startswith(("check-", "status-")):
                condition_with_only_checks.make_always_true()
        return conditions_with_only_checks

    async def _have_unexpected_draft_pull_request_changes(
        self,
    ) -> bool:
        if self.queue_pull_request_number is None:
            raise RuntimeError(
                "_have_unexpected_draft_pull_request_changes expected the train car to be started"
            )

        checked_ctxt = await self.train.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        mergify_bot = await github.GitHubAppInfo.get_bot(
            self.train.repository.installation.redis.cache_bytes
        )
        unexpected_event = first.first(
            (source for source in checked_ctxt.sources),
            key=lambda s: s["event_type"] == "pull_request"
            and typing.cast(github_types.GitHubEventPullRequest, s["data"])["action"]
            in ["closed", "reopened", "synchronize"]
            and s["data"]["sender"]["id"] != mergify_bot["id"],
        )
        if unexpected_event:
            checked_ctxt.log.info(
                "train car received an unexpected event",
                unexpected_event=unexpected_event,
            )
            return True

        return False

    async def get_unexpected_change(
        self,
        queue_rule: "qr_config.QueueRule",
    ) -> UnexpectedChange | None:
        if self.queue_pull_request_number is None:
            raise RuntimeError(
                "get_unexpected_change expected the train car to be started"
            )

        checked_ctxt = await self.train.repository.get_pull_request_context(
            self.queue_pull_request_number
        )

        try:
            current_base_sha = await self.train.get_base_sha()
        except BaseBranchVanished:
            checked_ctxt.log.warning(
                "target branch vanished, the merge queue will be deleted soon"
            )
            return None

        unexpected_changes: UnexpectedChange | None = None

        if not await self.train.is_synced_with_the_base_branch(current_base_sha):
            unexpected_changes = UnexpectedBaseBranchChange(current_base_sha)
        elif (
            self.train_car_state.checks_type == TrainCarChecksType.INPLACE
            and await checked_ctxt.synchronized_by_user_at() is not None
        ):
            unexpected_changes = UnexpectedUpdatedPullRequestChange(
                checked_ctxt.pull["number"]
            )
        elif (
            self.train_car_state.checks_type == TrainCarChecksType.DRAFT
            and not queue_rule.config["allow_queue_branch_edit"]
            and await self._have_unexpected_draft_pull_request_changes()
        ):
            unexpected_changes = UnexpectedDraftPullRequestChange(
                self.queue_pull_request_number
            )
        return unexpected_changes

    async def check_mergeability(
        self,
        origin: typing.Literal[
            "original_pull_request", "draft_pull_request", "batch_split"
        ],
        original_pull_request_rule: prr_config.EvaluatedPullRequestRule | None,
        original_pull_request_number: github_types.GitHubPullRequestNumber | None,
    ) -> None:
        if self.queue_pull_request_number is None:
            # Nothing to do the car has not been started yet
            return

        checked_ctxt = await self.train.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        queue_rule = self.get_queue_rule()
        unexpected_changes = await self.get_unexpected_change(queue_rule)
        if isinstance(unexpected_changes, UnexpectedBaseBranchChange):
            checked_ctxt.log.info(
                "train will be reset",
                gh_pull_queued=[
                    ep.user_pull_request_number
                    for ep in self.still_queued_embarked_pulls
                ],
                unexpected_changes=unexpected_changes,
            )
            await self.train.reset(unexpected_changes)

            if self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
                await checked_ctxt.client.post(
                    f"{checked_ctxt.base_url}/issues/{checked_ctxt.pull['number']}/comments",
                    json={
                        "body": f"This pull request has unexpected changes: {unexpected_changes}. The whole train will be reset."
                    },
                )
            return

        saved_last_conditions_evaluation = self.last_conditions_evaluation
        saved_outcome = self.train_car_state.outcome
        saved_ci_state = self.train_car_state.ci_state

        check = await checked_ctxt.get_engine_check_run(
            constants.MERGE_QUEUE_SUMMARY_NAME
        )
        saved_queue_check_run_conclusion = check["conclusion"] if check else None
        saved_freeze_data = self.train_car_state.frozen_by

        if (
            origin == "original_pull_request"
            and self.train_car_state.checks_type == TrainCarChecksType.DRAFT
            and saved_queue_check_run_conclusion is not None
        ):
            # NOTE(sileht): The draft PR state is final, no need to reevaluate.
            return

        pull_requests = await self.get_pull_requests_to_evaluate()
        evaluated_queue_rule = await queue_rule.get_evaluated_queue_rule(
            checked_ctxt.repository,
            checked_ctxt.pull["base"]["ref"],
            pull_requests,
            # NOTE(sileht): For INPLACE checks we inject the
            # original_pull_request_rule because we also need to wait for
            # the original_pull_request check-runs to finish or timeout.
            evaluated_pull_request_rule=original_pull_request_rule
            if self.train_car_state.checks_type == TrainCarChecksType.INPLACE
            else None,
        )

        if (
            self.train_car_state.checks_type == TrainCarChecksType.INPLACE
            and await checked_ctxt.is_behind
        ):
            # NOTE(sileht): The PR has been updated, but GitHub still return the old head sha
            # So we should not looks at CIs yet. Reporting may not be awesome as CIs will
            # look passing for a couple of second.
            status = check_api.Conclusion.PENDING
        else:
            status = await checks_status.get_rule_checks_status(
                checked_ctxt.log,
                checked_ctxt.repository,
                pull_requests,
                evaluated_queue_rule,
                wait_for_schedule_to_match=True,
            )

        await self.update_state(
            status,
            evaluated_queue_rule,
            unexpected_change=unexpected_changes,
        )

        if self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            for pull_request in pull_requests:
                await delayed_refresh.plan_next_refresh(
                    checked_ctxt,
                    [evaluated_queue_rule],
                    pull_request,
                    only_if_earlier=True,
                )

            # NOTE(sileht): we are supposed to be triggered by GitHub events, but in
            # case we miss some of them due to an outage, this is a seatbelt to recover
            # automatically after 3 minutes
            refresh_at = date.utcnow() + datetime.timedelta(minutes=3)
            await delayed_refresh.plan_refresh_at_least_at(
                self.train.repository, self.queue_pull_request_number, refresh_at
            )

        diff_result = deepdiff.DeepDiff(
            saved_last_conditions_evaluation,
            self.last_conditions_evaluation,
            ignore_order=True,
            exclude_types=[date.Schedule],
            # No need to refresh summary if related_checks or next_evaluation_at change
            exclude_regex_paths=(
                r"\['related_checks'\]\[\d+\]",
                r"\['next_evaluation_at'\]",
            ),
        )
        require_summaries_update = (
            len(diff_result.affected_paths) > 0
            or saved_outcome != self.train_car_state.outcome
            or saved_ci_state != self.train_car_state.ci_state
            or saved_freeze_data != self.train_car_state.frozen_by
            or saved_queue_check_run_conclusion
            != self.get_queue_check_run_conclusion().value
        )
        if require_summaries_update:
            await self.update_summaries()
            await self._refresh_next_pull_request_to_merge(
                origin, original_pull_request_number
            )

        await self.train.save()

        checked_ctxt.log.info(
            "train car pull request evaluation",
            origin=origin,
            gh_pull_queued=[
                ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
            ],
            require_summaries_update=require_summaries_update,
            unexpected_changes=unexpected_changes,
            status=status,
            event_types=[se["event_type"] for se in checked_ctxt.sources],
            diff_result=str(diff_result),
            new_state={
                "outcome": self.train_car_state.outcome,
                "ci_state": self.train_car_state.ci_state,
                "ci_started_at": self.train_car_state.ci_started_at,
                "ci_ended_at": self.train_car_state.ci_ended_at,
                "checks_end_at": self.checks_ended_timestamp,
                "queue_check_conclusion": self.get_queue_check_run_conclusion(),
            },
            previous_state={
                "outcome": saved_outcome,
                "ci_state": saved_ci_state,
                "queue_check_conclusion": saved_queue_check_run_conclusion,
            },
        )

    async def _refresh_next_pull_request_to_merge(
        self,
        origin: typing.Literal[
            "original_pull_request", "draft_pull_request", "batch_split"
        ],
        original_pull_request_number: github_types.GitHubPullRequestNumber | None,
    ) -> None:
        if self.train_car_state.checks_type != TrainCarChecksType.DRAFT:
            # NOTE(sileht): No need to refresh INPLACE checks as merge() is always
            # called after check_mergeability()
            return

        if not self.train._cars or self.train._cars[0] is not self:
            # NOTE(sileht): No need to refresh as this train car is not the first one in queue
            return

        first_pull_request_in_car = self.still_queued_embarked_pulls[
            0
        ].user_pull_request_number

        if (
            origin == "original_pull_request"
            and first_pull_request_in_car == original_pull_request_number
        ):
            # NOTE(sileht): No need to refresh as check_mergeability() is called
            # by the action queue and merge() will be run just after
            return

        if self.get_queue_check_run_conclusion() is check_api.Conclusion.PENDING:
            # NOTE(sileht): No need to refresh as the Draft PR state is not final
            return

        await refresher.send_pull_refresh(
            self.train.repository.installation.redis.stream,
            self.train.repository.repo,
            pull_request_number=first_pull_request_in_car,
            action="internal",
            source="draft pull request state change",
            priority=worker_pusher.Priority.immediate,
        )

    def get_queue_rule(self) -> "qr_config.QueueRule":
        queue_name = self.initial_embarked_pulls[0].config["name"]
        try:
            return self.train.queue_rules[queue_name]
        except IndexError:
            raise RuntimeError(
                f"The rule for queue `{queue_name}` is missing from configuration"
            )

    async def update_state(
        self,
        queue_conditions_conclusion: check_api.Conclusion,
        evaluated_queue_rule: "qr_config.EvaluatedQueueRule",
        unexpected_change: UnexpectedChange | None = None,
    ) -> None:
        self.last_conditions_evaluation = (
            evaluated_queue_rule.merge_conditions.get_evaluation_result()
        )
        outside_schedule = False
        has_failed_check_other_than_schedule = False

        rule = self.get_queue_rule()
        timeout = rule.config["checks_timeout"]

        # Update checks end
        if (
            self.checks_ended_timestamp is None
            and queue_conditions_conclusion != check_api.Conclusion.PENDING
        ):
            self.checks_ended_timestamp = date.utcnow()

        # Update CI state
        if queue_conditions_conclusion == check_api.Conclusion.SUCCESS:
            self.train_car_state.ci_state = CiState.SUCCESS
            self.train_car_state.ci_ended_at = date.utcnow()
        elif queue_conditions_conclusion == check_api.Conclusion.FAILURE:
            self.train_car_state.ci_state = CiState.FAILED
            self.train_car_state.ci_ended_at = date.utcnow()
        elif queue_conditions_conclusion == check_api.Conclusion.PENDING:
            queue_pull_requests = await self.get_pull_requests_to_evaluate()
            conditions_with_only_checks = self._get_conditions_without_checks(
                evaluated_queue_rule
            )

            if await conditions_with_only_checks(queue_pull_requests):
                self.train_car_state.ci_state = CiState.SUCCESS
                self.train_car_state.ci_ended_at = date.utcnow()
            else:
                self.train_car_state.ci_state = CiState.PENDING
                self.train_car_state.ci_ended_at = None
        else:
            raise RuntimeError(
                f"Unhandled queue_conditions_conclusion: {queue_conditions_conclusion}"
            )

        # Register schedule period
        if queue_conditions_conclusion in (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.FAILURE,
        ):
            self.train_car_state.add_waiting_for_schedule_end_date()
        elif queue_conditions_conclusion == check_api.Conclusion.PENDING:
            # FIXME(sileht) this is unperfect has conditions tree may don't care about the schedule we are looking at, eg:
            # or:
            #   - label=foobar
            #   - schedule=XXXXX
            # if this label is set we should ignore this schedule attribute
            for condition in evaluated_queue_rule.merge_conditions.walk():
                attr = condition.get_attribute_name()
                if not condition.match:
                    if attr == "schedule":
                        outside_schedule = True
                    else:
                        has_failed_check_other_than_schedule = True

            if outside_schedule and not has_failed_check_other_than_schedule:
                self.train_car_state.add_waiting_for_schedule_start_date()
            else:
                self.train_car_state.add_waiting_for_schedule_end_date()
        else:
            raise RuntimeError(
                f"Unhandled queue_conditions_conclusion: {queue_conditions_conclusion}"
            )

        # Register freeze period
        qf = await self.train.is_queue_frozen(
            self.still_queued_embarked_pulls[0].config["name"],
        )
        if not qf:
            self.train_car_state.add_waiting_for_freeze_end_date()
        elif qf and self.train_car_state.ci_state == CiState.SUCCESS:
            # Add start date only if the reason this isnt getting merged is because of the freeze
            self.train_car_state.add_waiting_for_freeze_start_date()

        # Update Outcome
        if unexpected_change is not None:
            # Unexpected change always override any outcome
            self.train_car_state.outcome = UNEXPECTED_CHANGE_COMPATIBILITY[
                type(unexpected_change)
            ]
            self.train_car_state.outcome_message = str(unexpected_change)
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            if (
                self.train_car_state.ci_state == CiState.PENDING
                and timeout is not None
                and self.train_car_state.ci_has_timed_out(timeout)
            ):
                # NOTE(Syffe): The timeout state has a priority over CI success or failure.
                # if we notice that a timeout has occured, the reporting should notify of the timeout
                # because it has occured before assessing the CI's state.
                self.train_car_state.outcome = TrainCarOutcome.CHECKS_TIMEOUT
                self.train_car_state.outcome_message = CHECKS_TIMEOUT_MESSAGE
            elif queue_conditions_conclusion == check_api.Conclusion.SUCCESS:
                self.train_car_state.outcome = TrainCarOutcome.MERGEABLE
                self.train_car_state.outcome_message = None
            elif queue_conditions_conclusion == check_api.Conclusion.FAILURE:
                self.train_car_state.outcome = TrainCarOutcome.CHECKS_FAILED
                self.train_car_state.outcome_message = CI_FAILED_MESSAGE
        elif (
            self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED
            and self._has_reached_batch_max_failure()
        ):
            self.train_car_state.outcome = (
                TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
            )

        # Calcule next timeout refresh
        if (
            self.train_car_state.outcome == TrainCarOutcome.UNKNOWN
            and self.train_car_state.ci_state == CiState.PENDING
            and timeout is not None
            and self.queue_pull_request_number is not None  # to please mypy
            and self.train_car_state.ci_started_at is not None  # to please mypy
            and not self.train_car_state.ci_has_timed_out(timeout)
        ):
            await delayed_refresh.plan_refresh_at_least_at(
                self.train.repository,
                self.queue_pull_request_number,
                self.train_car_state.ci_started_at + timeout,
            )

        self.train_car_state.frozen_by = await self.train.get_current_queue_freeze(
            self.still_queued_embarked_pulls[0].config["name"]
        )

        # Save the status of all check-runs/statuses for API/UI reporting
        if self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
        ):
            await self._save_check_runs()

    def _has_reached_batch_max_failure(self) -> bool:
        rule = self.get_queue_rule()
        batch_max_failure = rule.config["batch_max_failure_resolution_attempts"]

        if batch_max_failure is None:
            return False

        return len(self.failure_history) >= batch_max_failure

    async def _save_check_runs(self) -> None:
        self.last_checks = []

        if self.queue_pull_request_number is None:
            raise RuntimeError(
                f"car's spec checks type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
            )

        checked_ctxt = await self.train.repository.get_pull_request_context(
            self.queue_pull_request_number
        )

        for check in await checked_ctxt.pull_check_runs:
            # Don't copy Summary/Rule/Queue/... checks
            if check["app_id"] == config.INTEGRATION_ID:
                continue

            output_title = ""
            if check["output"] and check["output"]["title"]:
                output_title = f" — {check['output']['title']}"

            self.last_checks.append(
                QueueCheck(
                    name=f"{check['app_name']}/{check['name']}",
                    description=output_title,
                    avatar_url=check["app_avatar_url"],
                    url=check["html_url"],
                    state=check["conclusion"] or "pending",
                )
            )

        for status in await checked_ctxt.pull_statuses:
            self.last_checks.append(
                QueueCheck(
                    name=status["context"],
                    description=status["description"] or "",
                    avatar_url=status["avatar_url"] or "",
                    url=status["target_url"] or "",
                    state=status["state"] or "pending",
                )
            )

    async def update_summaries(self) -> None:
        queue_rule = self.get_queue_rule()
        refs = self._get_user_refs(create_link=False)
        has_unexpected_change = self.train_car_state.outcome in (
            TrainCarOutcome.DRAFT_PR_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
            TrainCarOutcome.BASE_BRANCH_CHANGE,
        )
        if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            if len(self.initial_embarked_pulls) == 1:
                tmp_pull_title = f"The pull request {refs} is mergeable"
            else:
                tmp_pull_title = f"The pull requests {refs} are mergeable"
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            if len(self.initial_embarked_pulls) == 1:
                tmp_pull_title = f"The pull request {refs} is embarked for merge"
            else:
                tmp_pull_title = f"The pull requests {refs} are embarked for merge"
        elif has_unexpected_change:
            if len(self.initial_embarked_pulls) == 1:
                tmp_pull_title = f"The pull request {refs} cannot be merged, due to unexpected changes in this draft PR, and has been disembarked."
            else:
                tmp_pull_title = f"The pull requests {refs} cannot be merged, due to unexpected changes in this draft PR, and have been disembarked."
        elif self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_FAILED,
            TrainCarOutcome.CHECKS_TIMEOUT,
        ):
            if len(self.initial_embarked_pulls) == 1:
                tmp_pull_title = f"The pull request {refs} cannot be merged and has been disembarked."
            else:
                tmp_pull_title = (
                    f"The pull requests {refs} cannot be merged and will be split."
                )
        elif (
            self.train_car_state.outcome
            == TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
        ):
            if len(self.initial_embarked_pulls) == 1:
                tmp_pull_title = f"The pull request {refs} cannot be merged, due to maximum batch failure resolution attempts reached, and has been disembarked."
            else:
                tmp_pull_title = f"The pull requests {refs} cannot be merged, due to maximum batch failure resolution attempts reached, and have been disembarked."
        else:
            raise RuntimeError(
                f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
            )

        queue_summary = "\n\nRequired conditions for merge:\n\n"
        queue_summary += self.last_evaluated_conditions
        timeout_summary = ""
        timeout = queue_rule.config["checks_timeout"]

        if self.train_car_state.outcome == TrainCarOutcome.CHECKS_TIMEOUT:
            timeout_summary = "\n\n❌⏲️️  The checks have timed out ⏲❌"
        elif (
            timeout is not None
            and self.train_car_state.ci_started_at is not None  # to please mypy
            and self.train_car_state.outcome == TrainCarOutcome.UNKNOWN
        ):
            expected_finish_dt = self.train_car_state.ci_started_at + timeout
            expected_finish_pretty = (
                date.pretty_datetime(expected_finish_dt)
                if expected_finish_dt.date() > date.utcnow().date()
                else date.pretty_time(expected_finish_dt)
            )
            timeout_summary = (
                f"\n\n⏲️  The checks have to pass before {expected_finish_pretty} ⏲"
            )

        queue_summary += timeout_summary

        if self.failure_history:
            batch_failure_summary = f"\n\nThe pull request {self._get_user_refs()} is part of a speculative checks batch that previously failed:\n\n"
            batch_failure_summary += (
                "| Pull request | Parents pull requests | Speculative checks |\n"
            )
            batch_failure_summary += "| ---: | :--- | :--- |\n"
            for failure in self.failure_history:
                if failure.train_car_state.checks_type == TrainCarChecksType.INPLACE:
                    speculative_checks = "[in place]"
                elif failure.train_car_state.checks_type == TrainCarChecksType.DRAFT:
                    if failure.queue_pull_request_number is None:
                        raise RuntimeError(
                            "car's spec checks type is draft, but queue_pull_request_number is None"
                        )
                    speculative_checks = build_pr_link(
                        self.train.repository, failure.queue_pull_request_number
                    )
                else:
                    speculative_checks = ""
            batch_failure_summary += f"| {self._get_user_refs()} | {self._get_embarked_refs(include_my_self=False, markdown=True)} | {speculative_checks} |"
        else:
            batch_failure_summary = ""

        if self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
            summary = f"Embarking {self._get_embarked_refs(markdown=True)} together"
            summary += queue_summary + batch_failure_summary

            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    "car's spec checks type is draft, but queue_pull_request_number is None"
                )

            tmp_pull_ctxt = await self.train.repository.get_pull_request_context(
                self.queue_pull_request_number
            )

            headline: str | None = None
            if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
                headline = "🎉 This combination of pull requests has been checked successfully 🎉"
                show_queue = False
            elif self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED:
                if len(self.initial_embarked_pulls) == 1:
                    headline = f"🙁 This pull request has failed checks. {self._get_user_refs()} will be removed from the queue. 🙁"
                elif self.has_previous_car_status_succeeded():
                    headline = "🕵️ This combination of pull requests has failed checks. Mergify will split this batch to understand which pull request is responsible for the failure. 🕵️"
                else:
                    headline = "🕵️ This combination of pull requests has failed checks. Mergify is waiting for other pull requests ahead in the queue to understand which one is responsible for the failure. 🕵️"
                show_queue = False
            elif self.train_car_state.outcome == TrainCarOutcome.CHECKS_TIMEOUT:
                headline = (
                    "🙁 This combination of pull requests has timed out. "
                    f"{self._get_user_refs()} will be removed from the queue. 🙁"
                )
                show_queue = False
            elif has_unexpected_change:
                headline = f"✨ Unexpected queue change: {self.train_car_state.outcome_message}. The pull request {self._get_user_refs()} will be re-embarked soon. ✨"
                show_queue = True
            elif (
                self.train_car_state.outcome
                == TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
            ):
                headline = (
                    "🙁 This combination of pull requests has failed checks and the maximum resolution attempts has been reached. "
                    f"{self._get_user_refs()} will be removed from the queue. 🙁"
                )
                show_queue = False
            elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
                headline = None
                show_queue = True
            else:
                raise RuntimeError(
                    f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
                )

            body = await self.generate_merge_queue_summary(
                for_queue_pull_request=True,
                headline=headline,
                show_queue=show_queue,
            )

            if tmp_pull_ctxt.body != body:
                await tmp_pull_ctxt.client.patch(
                    f"{tmp_pull_ctxt.base_url}/pulls/{self.queue_pull_request_number}",
                    json={"body": body},
                )

            await tmp_pull_ctxt.set_summary_check(
                check_api.Result(
                    self.get_queue_check_run_conclusion(),
                    title=tmp_pull_title,
                    summary=summary,
                )
            )

            checked_pull = self.queue_pull_request_number
        elif self.train_car_state.checks_type == TrainCarChecksType.INPLACE:
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    "car's spec checks type is inplace, but queue_pull_request_number is None"
                )
            checked_pull = self.queue_pull_request_number
        else:
            checked_pull = github_types.GitHubPullRequestNumber(0)

        if self.last_checks:
            checks_copy_summary = (
                "\n\nCheck-runs and statuses of the embarked "
                f"pull request #{checked_pull}:\n\n<table>"
            )
            for qcheck in get_check_list_ordered(self.last_checks):
                qcheck_icon_url = CHECK_ASSERTS.get(
                    qcheck.state, CHECK_ASSERTS["neutral"]
                )

                checks_copy_summary += (
                    "<tr>"
                    f'<td align="center" width="48" height="48"><img src="{qcheck_icon_url}" width="16" height="16" /></td>'
                    f'<td align="center" width="48" height="48"><img src="{qcheck.avatar_url}&s=40" width="16" height="16" /></td>'
                    f"<td><b>{qcheck.name}</b>{qcheck.description}</td>"
                    f'<td><a href="{qcheck.url}">details</a></td>'
                    "</tr>"
                )
            checks_copy_summary += "</table>\n"
        else:
            checks_copy_summary = ""

        # Update the original Pull Request
        if self.train_car_state.outcome == TrainCarOutcome.DRAFT_PR_CHANGE:
            original_pull_title = "The pull request has been removed from the queue"
        elif self.train_car_state.outcome in (
            TrainCarOutcome.BASE_BRANCH_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
        ):
            original_pull_title = "The pull request is going to be re-embarked soon"

        elif self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            original_pull_title = f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} is mergeable"
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            original_pull_title = f"The pull request is embarked with {self._get_embarked_refs(include_my_self=False)} for merge"
        elif self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_FAILED,
            TrainCarOutcome.CHECKS_TIMEOUT,
            TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS,
        ):
            original_pull_title = f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} cannot be merged and has been disembarked"
        else:
            raise RuntimeError(
                f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
            )

        unexpected_change_summary = ""
        if has_unexpected_change:
            unexpected_change_summary = f"\n\n❌ Unexpected queue change: {self.train_car_state.outcome_message}. ❌"

        self.train.log.info(
            "pull request train car status update",
            legacy_conclusion=self.get_queue_check_run_conclusion(),
            outcome=self.train_car_state.outcome,
            outcome_message=self.train_car_state.outcome_message,
            original_pull_title=original_pull_title,
            gh_pull=checked_pull,
            queue_summary=queue_summary.strip(),
            batch_failure_summary=batch_failure_summary.strip(),
            checks_copy_summary={check.name: check.state for check in self.last_checks},
            still_embarked_pull_numbers=[
                ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
            ],
        )

        queue_freeze_summary = ""
        if self.train_car_state.frozen_by is not None:
            queue_freeze_summary = self.train_car_state.frozen_by.get_freeze_message()

        train_car_state_summary = TrainCarStateForSummary.from_train_car_state(
            self.train_car_state
        ).serialized()

        original_pull_summary = (
            train_car_state_summary
            + unexpected_change_summary
            + queue_freeze_summary
            + queue_summary
            + checks_copy_summary
            + batch_failure_summary
        )

        report = check_api.Result(
            self.get_queue_check_run_conclusion(),
            title=original_pull_title,
            summary=original_pull_summary,
        )

        original_ctxts = [
            await self.train.repository.get_pull_request_context(
                ep.user_pull_request_number
            )
            for ep in self.still_queued_embarked_pulls
        ]

        for original_ctxt in original_ctxts:
            await check_api.set_check_run(
                original_ctxt,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                report,
            )

        if self.train_car_state.checks_type != TrainCarChecksType.DRAFT:
            return

        if self.get_queue_check_run_conclusion() != check_api.Conclusion.PENDING:
            await tmp_pull_ctxt.client.post(
                f"{tmp_pull_ctxt.base_url}/issues/{self.queue_pull_request_number}/comments",
                json={"body": tmp_pull_title},
            )
            await tmp_pull_ctxt.client.patch(
                f"{tmp_pull_ctxt.base_url}/pulls/{self.queue_pull_request_number}",
                json={"state": "closed"},
            )

    async def send_refresh_to_user_pull_requests(self) -> None:
        for i, ep in enumerate(self.still_queued_embarked_pulls):
            # NOTE(sileht): refresh it, so the queue action will merge it and delete the
            # tmp_pull_ctxt branch
            await refresher.send_pull_refresh(
                self.train.repository.installation.redis.stream,
                self.train.repository.repo,
                pull_request_number=ep.user_pull_request_number,
                action="internal",
                source="draft pull request state change",
                priority=worker_pusher.Priority.immediate
                if i == 0
                and self.train_car_state.checks_type == TrainCarChecksType.INPLACE
                else worker_pusher.Priority.medium,
            )

    def _get_previous_car(self) -> "TrainCar" | None:
        position = self.train._cars.index(self)
        if position == 0:
            return None
        else:
            return self.train._cars[position - 1]

    def has_previous_car_status_succeeded(self) -> bool:
        position = self.train._cars.index(self)
        if position == 0:
            return True
        return all(
            c.train_car_state.outcome == TrainCarOutcome.MERGEABLE
            for c in self.train._cars[:position]
        )


class QueueFreezeWithPriority(typing.TypedDict):
    queue_freeze: freeze.QueueFreeze
    priority: int


@dataclasses.dataclass
class Train:
    repository: context.Repository
    queue_rules: "qr_config.QueueRules"
    ref: github_types.GitHubRefType

    # Stored in redis
    _cars: list[TrainCar] = dataclasses.field(default_factory=list)
    _waiting_pulls: list[EmbarkedPull] = dataclasses.field(default_factory=list)
    _current_base_sha: github_types.SHAType | None = dataclasses.field(default=None)
    _queues_with_freeze: dict[
        qr_config.QueueName, freeze.QueueFreeze | None
    ] | None = None

    class Serialized(typing.TypedDict):
        cars: list[TrainCar.Serialized]
        waiting_pulls: list[EmbarkedPull.Serialized]
        current_base_sha: github_types.SHAType | None

    @classmethod
    async def from_context(
        cls, ctxt: context.Context, queue_rules: "qr_config.QueueRules"
    ) -> "Train":
        q = cls(ctxt.repository, queue_rules, ctxt.pull["base"]["ref"])
        await q.load()
        return q

    def _get_redis_key(self) -> str:
        return f"merge-trains~{self.repository.installation.owner_id}"

    def _get_redis_hash_key(self) -> str:
        return f"{self.repository.repo['id']}~{self.ref}"

    @classmethod
    @tracer.wrap("Train.refresh_trains", span_type="worker")
    async def refresh_trains(
        cls,
        installation: context.Installation,
    ) -> None:
        trains_key = f"merge-trains~{installation.owner_id}"
        for key in await installation.redis.cache.hkeys(trains_key):
            repo_id_str, ref_str = key.split("~")
            ref = github_types.GitHubRefType(ref_str)
            repo_id = github_types.GitHubRepositoryIdType(int(repo_id_str))
            try:
                repository = await installation.get_repository_by_id(repo_id)
            except http.HTTPNotFound:
                LOG.warning(
                    "repository with active merge queue is unaccessible, deleting merge queue",
                    gh_owner=installation.owner_login,
                    gh_repo_id=repo_id,
                )
                await installation.redis.cache.hdel(trains_key, key)
                continue

            try:
                mergify_config = await repository.get_mergify_config()
            except mergify_conf.InvalidRules as e:  # pragma: no cover
                LOG.warning(
                    "train can't be refreshed, the mergify configuration is invalid",
                    gh_owner=repository.installation.owner_login,
                    gh_repo=repository.repo["name"],
                    gh_branch=ref,
                    summary=str(e),
                    annotations=e.get_annotations(e.filename),
                )
                continue

            queue_rules = mergify_config["queue_rules"]

            train = cls(repository, queue_rules, ref)
            await train.load()
            await train.refresh()

    @classmethod
    async def iter_trains(
        cls,
        repository: context.Repository,
        queue_rules: "qr_config.QueueRules",
        *,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> abc.AsyncIterator["Train"]:
        repo_filter: (github_types.GitHubRepositoryIdType | typing.Literal["*"]) = "*"
        if repository is not None:
            repo_filter = repository.repo["id"]

        async for key, train_raw in repository.installation.redis.cache.hscan_iter(
            f"merge-trains~{repository.installation.owner_id}",
            f"{repo_filter}~*",
            count=10000,
        ):
            repo_id_str, ref_str = key.split("~")
            ref = github_types.GitHubRefType(ref_str)
            if exclude_ref is not None and ref == exclude_ref:
                continue

            train = cls(repository, queue_rules, ref)
            await train.load(train_raw)
            yield train

    async def load(self, train_raw: str | None = None) -> None:
        if train_raw is None:
            train_raw = await self.repository.installation.redis.cache.hget(
                self._get_redis_key(), self._get_redis_hash_key()
            )

        if train_raw:
            train = typing.cast(Train.Serialized, json.loads(train_raw))
            self._waiting_pulls = [
                EmbarkedPull.deserialize(self, wp) for wp in train["waiting_pulls"]
            ]
            self._current_base_sha = train["current_base_sha"]
            self._cars = [TrainCar.deserialize(self, c) for c in train["cars"]]
        else:
            self._cars = []
            self._waiting_pulls = []
            self._current_base_sha = None

        self._queues_with_freeze = None

    @property
    def log_queue_extras(self) -> dict[str, typing.Any]:
        return {
            "train_cars": [
                [ep.user_pull_request_number for ep in c.still_queued_embarked_pulls]
                for c in self._cars
            ],
            "train_waiting_pulls": [
                wp.user_pull_request_number for wp in self._waiting_pulls
            ],
        }

    @functools.cached_property
    def log(self) -> daiquiri.KeywordArgumentAdapter:
        return daiquiri.getLogger(
            __name__,
            gh_owner=self.repository.installation.owner_login,
            gh_repo=self.repository.repo["name"],
            gh_branch=self.ref,
            **self.log_queue_extras,
        )

    async def save(self) -> None:
        if self._waiting_pulls or self._cars:
            prepared = self.Serialized(
                waiting_pulls=[ep.serialized() for ep in self._waiting_pulls],
                current_base_sha=self._current_base_sha,
                cars=[c.serialized() for c in self._cars],
            )
            raw = json.dumps(prepared)
            await self.repository.installation.redis.cache.hset(
                self._get_redis_key(), self._get_redis_hash_key(), raw
            )
        else:
            await self.repository.installation.redis.cache.hdel(
                self._get_redis_key(), self._get_redis_hash_key()
            )

    def get_car(self, ctxt: context.Context) -> TrainCar | None:
        return first.first(
            self._cars,
            key=lambda car: ctxt.pull["number"]
            in [ep.user_pull_request_number for ep in car.still_queued_embarked_pulls],
        )

    def get_car_by_tmp_pull(self, ctxt: context.Context) -> TrainCar | None:
        return first.first(
            self._cars,
            key=lambda car: car.queue_pull_request_number == ctxt.pull["number"],
        )

    async def refresh(self) -> None:
        # NOTE(sileht): workaround for cleaning unwanted PRs queued by this bug:
        # https://github.com/Mergifyio/mergify-engine/pull/2958
        await self._remove_duplicate_pulls()
        await self._sync_configuration_change()
        await self._split_failed_batches()
        try:
            await self._populate_cars()
        except BaseBranchVanished:
            self.log.warning("target branch vanished, deleting merge queue.")
            for embarked_pull, _ in list(self._iter_embarked_pulls()):
                await self._remove_pull(
                    embarked_pull.user_pull_request_number,
                    "merge queue internal",
                    queue_utils.TargetBranchMissing(self.ref),
                )
        await self.save()

    async def _remove_duplicate_pulls(self) -> None:
        known_prs = set()
        for i, car in enumerate(self._cars):
            for embarked_pull in car.still_queued_embarked_pulls:
                if embarked_pull.user_pull_request_number in known_prs:
                    await self._slice_cars(
                        i,
                        reason=queue_utils.PrQueuedTwice(),
                    )
                    break
                else:
                    known_prs.add(embarked_pull.user_pull_request_number)
            else:
                continue
            break

        wp_to_keep = []
        for wp in self._waiting_pulls:
            if wp.user_pull_request_number not in known_prs:
                known_prs.add(wp.user_pull_request_number)
                wp_to_keep.append(wp)
        self._waiting_pulls = wp_to_keep

    async def _sync_configuration_change(self) -> None:
        for i, (embarked_pull, _) in enumerate(list(self._iter_embarked_pulls())):
            queue_rule = self.queue_rules.get(embarked_pull.config["name"])
            if queue_rule is None:
                # NOTE(sileht): We just slice the cars list here, so when the
                # car will be recreated if the rule doesn't exists anymore, the
                # failure will be reported properly
                await self._slice_cars(
                    i,
                    reason=queue_utils.QueueRuleMissing(),
                )
                return None

    async def reset(self, unexpected_change: UnexpectedChange) -> None:
        await self._slice_cars(
            0, reason=queue_utils.UnexpectedQueueChange(change=str(unexpected_change))
        )
        await self.save()
        self.log.info("train cars reset")

    async def _slice_cars(
        self,
        new_queue_size: int,
        reason: queue_utils.BaseUnqueueReason,
        drop_pull_requests: dict[
            github_types.GitHubPullRequestNumber, queue_utils.BaseUnqueueReason
        ]
        | None = None,
    ) -> None:
        if drop_pull_requests is None:
            drop_pull_requests = {}

        sliced = False
        new_cars: list[TrainCar] = []
        new_waiting_pulls: list[EmbarkedPull] = []
        for c in self._cars:
            new_queue_size -= len(c.still_queued_embarked_pulls)
            if new_queue_size >= 0:
                new_cars.append(c)
            else:
                sliced = True
                new_waiting_pulls.extend(c.still_queued_embarked_pulls)
                for ep in c.still_queued_embarked_pulls:
                    signal_reason = drop_pull_requests.get(
                        ep.user_pull_request_number, reason
                    )
                    await c.send_checks_end_signal(
                        ep.user_pull_request_number,
                        signal_reason,
                        "DEFINITIVE"
                        if ep.user_pull_request_number in drop_pull_requests
                        else "REEMBARKED",
                    )
                await c.end_checking(
                    reason, not_reembarked_pull_requests=drop_pull_requests
                )

        if sliced:
            self.log.info(
                "queue has been sliced",
                new_queue_size=new_queue_size,
                reason=str(reason),
            )

        self._cars = new_cars
        self._waiting_pulls = [
            ep
            for ep in new_waiting_pulls + self._waiting_pulls
            if ep.user_pull_request_number not in drop_pull_requests
        ]

    def find_embarked_pull(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> (
        tuple[int, EmbarkedPullWithCar]
        | tuple[typing.Literal[None], typing.Literal[None]]
    ):
        for position, embarked_pull_with_car in enumerate(self._iter_embarked_pulls()):
            if (
                embarked_pull_with_car.embarked_pull.user_pull_request_number
                == pull_number
            ):
                return position, embarked_pull_with_car
        return None, None

    @staticmethod
    def _waiting_pulls_sorter(
        pull: EmbarkedPull,
    ) -> tuple[int, datetime.datetime]:
        return (
            pull.config["effective_priority"] * -1,
            pull.queued_at,
        )

    def _get_waiting_pulls_ordered_by_priority(
        self,
        ignored_queues: set[str] | frozenset[str] = frozenset(),
    ) -> tuple[list[EmbarkedPull], list[EmbarkedPull]]:
        ignored_pulls = []
        waiting_pulls = []
        for embarked_pull in self._waiting_pulls:
            if embarked_pull.config["name"] in ignored_queues:
                ignored_pulls.append(embarked_pull)
            else:
                waiting_pulls.append(embarked_pull)
        return (
            sorted(
                waiting_pulls,
                key=self._waiting_pulls_sorter,
            ),
            ignored_pulls,
        )

    def _iter_embarked_pulls(
        self,
        ignored_queues: set[str] | frozenset[str] = frozenset(),
    ) -> abc.Iterator[EmbarkedPullWithCar]:
        for car in self._cars:
            for embarked_pull in car.still_queued_embarked_pulls:
                yield EmbarkedPullWithCar(embarked_pull, car)

        (
            waiting_pulls_ordered_by_priority,
            _,
        ) = self._get_waiting_pulls_ordered_by_priority(ignored_queues=ignored_queues)
        for embarked_pull in waiting_pulls_ordered_by_priority:
            # NOTE(sileht): NamedTuple doesn't support multiple inheritance
            # the Protocol can't be inherited
            yield EmbarkedPullWithCar(embarked_pull, None)

    async def is_queue_frozen(self, queue_name: qr_config.QueueName) -> bool:
        return queue_name in await self.get_frozen_queue_names()

    async def get_queue_freezes_by_names(
        self,
    ) -> dict[qr_config.QueueName, freeze.QueueFreeze | None]:
        # NOTE(sileht): this does not return the queue freeze associated with the queue, but
        # queue freeze that block the queue (think cascading effect)
        if self._queues_with_freeze is None:
            queue_freezes = {
                queue_freeze.name: queue_freeze
                async for queue_freeze in freeze.QueueFreeze.get_all(
                    self.repository, self.queue_rules
                )
            }

            self._queues_with_freeze = {}

            # NOTE(sileht): queue_rules are always ordered by priority
            ongoing_freeze = None
            for queue_rule in self.queue_rules:
                # If the queue is not freeze we pick the nearest queue
                new_freeze = queue_freezes.get(queue_rule.name, ongoing_freeze)
                self._queues_with_freeze[queue_rule.name] = new_freeze
                if new_freeze is not None and new_freeze.cascading:
                    ongoing_freeze = new_freeze

        return self._queues_with_freeze

    async def get_current_queue_freeze(
        self, current_queue_name: qr_config.QueueName
    ) -> freeze.QueueFreeze | None:
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return queues_with_freeze.get(current_queue_name)

    async def get_frozen_queue_names(self) -> set[qr_config.QueueName]:
        # NOTE(Syffe): When checking for where to position a newly added PR in queues,
        # all unfrozen queues with lower priorities than the highest frozen queue have
        # to be considered as non-usable to queue the newly added PR.
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return {name for name, qf in queues_with_freeze.items() if qf is not None}

    async def add_pull(
        self,
        ctxt: context.Context,
        config: queue.PullQueueConfig,
        signal_trigger: str,
    ) -> None:
        # NOTE(sileht): first, ensure the pull is not in another branch
        await self.force_remove_pull(
            self.repository,
            self.queue_rules,
            ctxt.pull["number"],
            signal_trigger,
            exclude_ref=ctxt.pull["base"]["ref"],
        )

        # NOTE(charly): ensure there is no configuration change since the train
        # creation
        for embarked_pull, _ in list(self._iter_embarked_pulls()):
            queue_rule = self.queue_rules.get(embarked_pull.config["name"])
            if queue_rule is None:
                await self._remove_pull(
                    embarked_pull.user_pull_request_number,
                    signal_trigger,
                    queue_utils.QueueRuleMissing(),
                )
        new_pull_queue_rule = self.queue_rules[config["name"]]

        best_position = -1
        need_to_be_readded = False
        frozen_queues = await self.get_frozen_queue_names()

        for position, (embarked_pull, car) in enumerate(self._iter_embarked_pulls()):
            embarked_pull_queue_rule = self.queue_rules[embarked_pull.config["name"]]
            car_can_be_interrupted = car is None or (
                (
                    car.can_be_interrupted()
                    or (
                        embarked_pull.config["name"] != config["name"]
                        # NOTE(Syffe): If we don't consider unfrozen queues with lower priority
                        # than the highest frozen queue, this condition will be false,
                        # and so car_can_be_interrupted will also be. In that case, adding
                        # an urgent PR in the urgent queue will be impossible since embarked pull
                        # in lower priority queues that are not frozen will not validate this condition
                        # and thus, the best_position of the urgent PR will still be -1
                        # See:
                        and embarked_pull.config["name"] in frozen_queues
                    )
                )
                and new_pull_queue_rule.config["priority"]
                >= embarked_pull_queue_rule.config["priority"]
                and config["name"]
                not in embarked_pull_queue_rule.config[
                    "disallow_checks_interruption_from_queues"
                ]
            )

            if embarked_pull.user_pull_request_number == ctxt.pull["number"]:
                if (
                    config["effective_priority"]
                    != embarked_pull.config["effective_priority"]
                    or config["name"] != embarked_pull.config["name"]
                ) and car_can_be_interrupted:
                    ctxt.log.info(
                        "pull request already in train but misplaced",
                        config=config,
                        **self.log_queue_extras,
                    )
                    need_to_be_readded = True
                    break

                # already in queue at right place, we are good
                ctxt.log.info(
                    "pull request already in train",
                    config=config,
                    **self.log_queue_extras,
                )
                return

            if (
                best_position == -1
                and config["effective_priority"]
                > embarked_pull.config["effective_priority"]
                and car_can_be_interrupted
            ):
                # We found a car with lower priority
                best_position = position

        if need_to_be_readded:
            # FIXME(sileht): this can be optimised by not dropping spec checks,
            # if the position in the queue does not change
            await self._remove_pull_from_context(
                ctxt.pull["number"],
                signal_trigger,
                queue_utils.PrWithHigherPriorityQueued(ctxt.pull["number"]),
            )
            await self.add_pull(ctxt, config, signal_trigger)
            return

        new_embarked_pull = EmbarkedPull(
            self, ctxt.pull["number"], config, date.utcnow()
        )
        self._waiting_pulls.append(new_embarked_pull)

        if best_position != -1:
            await self._slice_cars(
                best_position,
                reason=queue_utils.PrWithHigherPriorityQueued(
                    pr_number=ctxt.pull["number"]
                ),
            )

        await self.save()

        final_position, _ = self.find_embarked_pull(ctxt.pull["number"])
        if final_position is None:
            raise RuntimeError(
                "Could not find the pull request we just added in the queue"
            )

        ctxt.log.info(
            "pull request added to train",
            gh_pull=ctxt.pull["number"],
            position=final_position,
            queue_name=config["name"],
            **self.log_queue_extras,
        )
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.queue.enter",
            signals.EventQueueEnterMetadata(
                {
                    "queue_name": new_embarked_pull.config["name"],
                    "branch": self.ref,
                    "position": final_position,
                    "queued_at": new_embarked_pull.queued_at,
                }
            ),
            signal_trigger,
        )
        # Refresh summary of all pull requests
        await self.refresh_pulls(
            source=f"pull {ctxt.pull['number']} added to queue",
        )

    async def remove_pull(
        self,
        ctxt: context.Context,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        # NOTE(sileht): Remove the pull request from all trains, just in case
        # the base branch change in the meantime
        await self.force_remove_pull(
            self.repository,
            self.queue_rules,
            ctxt.pull["number"],
            signal_trigger,
            exclude_ref=ctxt.pull["base"]["ref"],
        )
        await self._remove_pull_from_context(
            ctxt.pull["number"], signal_trigger, unqueue_reason
        )

    async def _remove_pull_from_context(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        if not self.is_queued(pull_number):
            self.log.info(
                "already absent from train",
                gh_pull=pull_number,
                gh_branch=self.ref,
                **self.log_queue_extras,
            )
            return
        if isinstance(unqueue_reason, queue_utils.PrMerged):
            await self._remove_merged_head_of_train(
                pull_number, signal_trigger, unqueue_reason
            )
        else:
            await self._remove_pull(pull_number, signal_trigger, unqueue_reason)

    async def _remove_merged_head_of_train(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.PrMerged,
    ) -> None:
        embarked_pull = self._cars[0].still_queued_embarked_pulls[0]
        if embarked_pull.user_pull_request_number != pr_number:
            raise RuntimeError("The head of train is not the expected pull_request")

        # Need to create the event here because the `self._cars[0]` might get deleted in the `if` below
        event_metadata = signals.EventQueueLeaveMetadata(
            {
                "reason": str(unqueue_reason),
                "merged": True,
                "queue_name": embarked_pull.config["name"],
                "branch": self.ref,
                "position": 0,
                "queued_at": embarked_pull.queued_at,
                "seconds_waiting_for_schedule": self._cars[
                    0
                ].train_car_state.seconds_waiting_for_schedule,
                "seconds_waiting_for_freeze": self._cars[
                    0
                ].train_car_state.seconds_waiting_for_freeze,
            }
        )
        # Head of the train was merged and the base_sha haven't changed, we can keep
        # other running cars
        await self._cars[0].send_checks_end_signal(
            self._cars[0].still_queued_embarked_pulls[0].user_pull_request_number,
            unqueue_reason,
            "DEFINITIVE",
        )
        del self._cars[0].still_queued_embarked_pulls[0]
        if len(self._cars[0].still_queued_embarked_pulls) == 0:
            deleted_car = self._cars[0]
            await deleted_car.end_checking(reason=None, not_reembarked_pull_requests={})
            self._cars = self._cars[1:]

        self._current_base_sha = unqueue_reason.sha

        await self.save()
        self.log.info(
            "removed from head train",
            position=0,
            gh_pull=pr_number,
            gh_branch=self.ref,
            **self.log_queue_extras,
        )
        await self.refresh_pulls(
            source=f"merged pull {pr_number} removed from queue",
            additional_pull_request=pr_number,
            # process quickly the next one,
            priority_first_pull_request=worker_pusher.Priority.immediate,
        )
        await signals.send(
            self.repository,
            pr_number,
            "action.queue.leave",
            event_metadata,
            signal_trigger,
        )

    async def _remove_pull(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        position, embarked_pull_with_car = self.find_embarked_pull(pr_number)
        if position is None or embarked_pull_with_car is None:
            return

        other_prs_reason: queue_utils.BaseUnqueueReason
        if isinstance(unqueue_reason, queue_utils.UnexpectedQueueChange):
            other_prs_reason = unqueue_reason
        else:
            other_prs_reason = queue_utils.PrAheadDequeued(pr_number=pr_number)

        await self._slice_cars(
            position,
            reason=other_prs_reason,
            drop_pull_requests={pr_number: unqueue_reason},
        )
        event_metadata = signals.EventQueueLeaveMetadata(
            {
                "reason": str(unqueue_reason),
                "merged": False,
                "queue_name": embarked_pull_with_car.embarked_pull.config["name"],
                "branch": self.ref,
                "position": position,
                "queued_at": embarked_pull_with_car.embarked_pull.queued_at,
            }
        )
        if embarked_pull_with_car.car is not None:
            event_metadata.update(
                {
                    "seconds_waiting_for_schedule": embarked_pull_with_car.car.train_car_state.seconds_waiting_for_schedule,
                    "seconds_waiting_for_freeze": embarked_pull_with_car.car.train_car_state.seconds_waiting_for_freeze,
                }
            )

        await self.save()
        await signals.send(
            self.repository,
            pr_number,
            "action.queue.leave",
            event_metadata,
            signal_trigger,
        )

        self.log.info(
            "removed from train",
            position=position,
            gh_pull=pr_number,
            gh_branch=self.ref,
            **self.log_queue_extras,
        )
        await self.refresh_pulls(
            source=f"pull {pr_number} removed from queue",
            additional_pull_request=pr_number,
        )

    async def _split_failed_train_car(self, car: TrainCar) -> None:
        current_queue_position = sum(
            len(c.still_queued_embarked_pulls)
            for c in itertools.takewhile(lambda c: c is not car, self._cars)
        ) + len(car.still_queued_embarked_pulls)
        self.log.info("spliting failed car", position=current_queue_position, car=car)

        queue_name = car.get_queue_name()
        try:
            queue_rule = self.queue_rules[queue_name]
        except KeyError:
            # We just need to wait the pull request has been removed from
            # the queue by the action
            self.log.info(
                "cant split failed batch TrainCar, queue rule does not exist anymore",
                queue_rules=self.queue_rules,
                queue_name=queue_name,
            )
            return

        # NOTE(sileht): This batch failed, we can drop everything else
        # after has we known now they will not work, and split this one
        # in two
        await self._slice_cars(
            current_queue_position, reason=queue_utils.PrAheadFailedToMerge()
        )

        # We move this car later at the end to not retest it
        del self._cars[-1]

        # NOTE(sileht): if speculative_checks == 1 we split the batch
        # in two parts, but check only the first one
        parts = max(2, queue_rule.config["speculative_checks"])

        parents: list[EmbarkedPull] = []
        for pos, pulls in enumerate(
            utils.split_list(car.still_queued_embarked_pulls[:-1], parts)
        ):
            self._cars.append(
                TrainCar(
                    train=self,
                    train_car_state=TrainCarState(),
                    initial_embarked_pulls=pulls,
                    still_queued_embarked_pulls=pulls.copy(),
                    parent_pull_request_numbers=car.parent_pull_request_numbers
                    + [ep.user_pull_request_number for ep in parents],
                    initial_current_base_sha=car.initial_current_base_sha,
                    failure_history=car.failure_history + [car],
                )
            )

            parents += pulls
            # NOTE(sileht): if speculative_checks == 1 we must check
            # only the first car, keep the second one as pending.
            # _populate_cars() will create the second one, when the
            # first car has finished and passed
            if queue_rule.config["speculative_checks"] > 1 or pos == 0:
                try:
                    previous_car = self._cars[-2]
                except IndexError:
                    previous_car = None
                try:
                    await self._start_checking_car(
                        self._cars[-1],
                        previous_car,
                    )
                except (
                    TrainCarPullRequestCreationPostponed,
                    TrainCarPullRequestCreationFailure,
                ):
                    self.log.info(
                        "failed to create draft pull request",
                        car=car,
                        exc_info=True,
                    )

        # Update the car to pull that was part of the batch into parent, but keep
        # the result as we already test it.
        car.parent_pull_request_numbers = car.parent_pull_request_numbers + [
            ep.user_pull_request_number for ep in parents
        ]
        car.still_queued_embarked_pulls = [car.still_queued_embarked_pulls[-1]]
        car.initial_embarked_pulls = car.still_queued_embarked_pulls.copy()
        self._cars.append(car)

        # Refresh summary of others
        await self.refresh_pulls(source="batch got split")

    async def _split_failed_batches(self) -> None:
        if (
            len(self._cars) == 1
            and self._cars[0].train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED
            and len(self._cars[0].initial_embarked_pulls) == 1
        ):
            # we refresh the state, to set the final conclusion
            await self._cars[0].check_mergeability(
                origin="batch_split",
                # NOTE(sileht): We should pass the original pull request rule
                # in case of inplace checks, but since the outcome is
                # TrainCarOutcome.CHECKS_FAILED, it's not a bug deal.
                original_pull_request_rule=None,
                original_pull_request_number=None,
            )
            return

        # NOTE(sileht): Looks for batch failure and split if needed
        first_failed_batch_train_car = first.first(
            car
            for car in self._cars
            if (
                car.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED
                and car.has_previous_car_status_succeeded()
                and len(car.initial_embarked_pulls) > 1
            )
        )
        if first_failed_batch_train_car is not None:
            await self._split_failed_train_car(first_failed_batch_train_car)

        # NOTE(sileht): speculative_checks=1 may create car without the
        # attached draft pull request if this car become the first, it means
        # the previous car has been merged and we can start testing it
        if (
            self._cars
            and len(self._cars[0].failure_history) > 0
            and self._cars[0].train_car_state.checks_type is None
        ):
            queue_name = self._cars[0].get_queue_name()
            try:
                self.queue_rules[queue_name]
            except KeyError:
                # We just need to wait the pull request has been removed from
                # the queue by the action
                self.log.info(
                    "can't start testing second half of a failed batch TrainCar, queue rule does not exist anymore",
                    queue_rules=self.queue_rules,
                    queue_name=queue_name,
                )
                return

            try:
                await self._start_checking_car(self._cars[0], None)
            except TrainCarPullRequestCreationPostponed:
                return
            except TrainCarPullRequestCreationFailure:
                # NOTE(sileht): We posted failure merge queue check-run on
                # car.user_pull_request_number and refreshed it, so it will be removed
                # from the train soon. We don't need to create remaining cars now.
                # When this car will be removed the remaining one will be created
                return

    async def _slice_frozen_cars(self, frozen_queues: set[str]) -> None:
        for i, car in enumerate(self._cars):
            for embarked_pull in car.still_queued_embarked_pulls:
                if embarked_pull.config["name"] in frozen_queues:
                    await self._slice_cars(
                        i,
                        reason=queue_utils.PrFrozenNoCascading(),
                    )
                    return

    async def _populate_cars(self) -> None:
        if self._cars and (
            self._cars[-1].train_car_state.checks_type == TrainCarChecksType.FAILED
            or self._cars[-1].train_car_state.outcome
            not in (TrainCarOutcome.MERGEABLE, TrainCarOutcome.UNKNOWN)
        ):
            # We are searching the responsible of a failure don't touch anything
            return

        non_cascading_queue_freeze_filter = {
            queue_freeze.name
            async for queue_freeze in freeze.QueueFreeze.get_all_non_cascading(
                self.repository, self.queue_rules
            )
        }

        try:
            head = next(
                self._iter_embarked_pulls(
                    ignored_queues=non_cascading_queue_freeze_filter
                )
            ).embarked_pull
        except StopIteration:
            return

        if self._current_base_sha is None or not self._cars:
            self._current_base_sha = await self.get_base_sha()

        if non_cascading_queue_freeze_filter and self._cars:
            await self._slice_frozen_cars(
                frozen_queues=non_cascading_queue_freeze_filter
            )

        try:
            queue_rule = self.queue_rules[head.config["name"]]
        except KeyError:
            # We just need to wait the pull request has been removed from
            # the queue by the action
            self.log.info(
                "cant populate cars, queue rule does not exist",
                queue_rules=self.queue_rules,
                queue_name=head.config["name"],
            )
            car = TrainCar(
                self, TrainCarState(), [head], [head], [], self._current_base_sha
            )
            await car._set_creation_failure(
                f"queue named `{head.config['name']}` does not exist anymore",
            )
            return

        speculative_checks = queue_rule.config["speculative_checks"]
        missing_cars = speculative_checks - len(self._cars)

        if missing_cars < 0:
            # Too many cars
            new_queue_size = sum(
                [
                    len(car.still_queued_embarked_pulls)
                    for car in self._cars[:speculative_checks]
                ]
            )
            await self._slice_cars(
                new_queue_size,
                reason=queue_utils.SpeculativeCheckNumberReduced(),
            )

        elif missing_cars > 0 and self._waiting_pulls:
            # Not enough cars
            for _ in range(missing_cars):
                (
                    waiting_pulls_ordered_by_priority,
                    frozen_pulls,
                ) = self._get_waiting_pulls_ordered_by_priority(
                    ignored_queues=non_cascading_queue_freeze_filter
                )

                pulls_to_check, remaining_pulls = self._get_next_batch(
                    waiting_pulls_ordered_by_priority,
                    head.config["name"],
                    queue_rule.config["batch_size"],
                )

                if frozen_pulls:
                    remaining_pulls += frozen_pulls

                if not pulls_to_check:
                    return

                enough_to_batch = len(pulls_to_check) == queue_rule.config["batch_size"]
                wait_enough_time_to_batch = (
                    date.utcnow() - pulls_to_check[0].queued_at
                    >= queue_rule.config["batch_max_wait_time"]
                )
                if not enough_to_batch and not wait_enough_time_to_batch:
                    await delayed_refresh.plan_refresh_at_least_at(
                        self.repository,
                        pulls_to_check[0].user_pull_request_number,
                        pulls_to_check[0].queued_at
                        + queue_rule.config["batch_max_wait_time"],
                    )

                    return

                self._waiting_pulls = remaining_pulls

                # NOTE(sileht): still_queued_embarked_pulls is always in sync with self._current_base_sha.
                # A TrainCar can be partially deleted and the next car may looks wierd as some parent PRs
                # may look missing but because the current_base_sha as moved too, this is safe.
                parent_pull_request_numbers = [
                    ep.user_pull_request_number
                    for ep in itertools.chain.from_iterable(
                        [car.still_queued_embarked_pulls for car in self._cars]
                    )
                ]

                car = TrainCar(
                    self,
                    TrainCarState(),
                    pulls_to_check,
                    pulls_to_check.copy(),
                    parent_pull_request_numbers,
                    self._current_base_sha,
                )

                if self._cars:
                    previous_car = self._cars[-1]
                else:
                    previous_car = None

                self._cars.append(car)

                try:
                    await self._start_checking_car(car, previous_car)
                except TrainCarPullRequestCreationPostponed:
                    return
                except TrainCarPullRequestCreationFailure:
                    # NOTE(sileht): We posted failure merge queue check-run on
                    # car.user_pull_request_number and refreshed it, so it will be removed
                    # from the train soon. We don't need to create remaining cars now.
                    # When this car will be removed the remaining one will be created
                    return

    async def _start_checking_car(
        self,
        car: TrainCar,
        previous_car: "TrainCar | None",
    ) -> None:
        queue_rule = car.get_queue_rule()
        can_be_updated = (
            self._cars[0] == car
            and len(car.still_queued_embarked_pulls) == 1
            and len(car.parent_pull_request_numbers) == 0
        )
        if can_be_updated and queue_rule.config["allow_inplace_checks"]:
            # smart mode
            if (
                queue_rule.config["speculative_checks"] == 1
                and queue_rule.config["batch_size"] == 1
            ):
                do_inplace_checks = True
            else:
                do_inplace_checks = not await car.is_behind()
        else:
            do_inplace_checks = False

        try:
            # get_next_batch() ensure all embarked_pulls has same config
            if do_inplace_checks:
                # No need to create a pull request
                await car.start_checking_inplace()
            else:
                await car.start_checking_with_draft(previous_car)

        except TrainCarPullRequestCreationPostponed:
            # NOTE(sileht): We can't create the tmp pull request, we will
            # retry later. In worse case, that will be retried until the pull
            # request become the first one in queue
            del self._cars[-1]
            self._waiting_pulls.extend(car.still_queued_embarked_pulls)
            raise

    async def get_base_sha(self) -> github_types.SHAType:
        escaped_branch_name = parse.quote(self.ref, safe="")
        try:
            branch = typing.cast(
                github_types.GitHubBranch,
                await self.repository.installation.client.item(
                    f"repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/branches/{escaped_branch_name}"
                ),
            )
        except http.HTTPNotFound:
            raise BaseBranchVanished(self.ref)
        return branch["commit"]["sha"]

    async def is_synced_with_the_base_branch(
        self, base_sha: github_types.SHAType
    ) -> bool:
        if not self._cars:
            return True

        if base_sha == self._current_base_sha:
            return True

        if not self._cars:
            # NOTE(sileht): the PR that call this method will be deleted soon
            return False

        # Base branch just moved but the last merged PR is the one we have on top on our
        # train, we just not yet received the event that have called Train.remove_pull()
        # NOTE(sileht): I wonder if it's robust enough, these cases should be enough to
        # catch everything I have in mind
        # * We run it when we remove the top car
        # * We run it when a tmp PR is refreshed
        # * We run it on each push events
        # * We run it before merge
        pull: github_types.GitHubPullRequest = await self.repository.installation.client.item(
            f"{self.repository.base_url}/pulls/{self._cars[0].still_queued_embarked_pulls[0].user_pull_request_number}"
        )
        return pull["merged"] and base_sha == pull["merge_commit_sha"]

    async def get_config(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> queue.PullQueueConfig:
        _, item = self.find_embarked_pull(pull_number)
        if item is not None:
            return item.embarked_pull.config

        raise RuntimeError("get_config on unknown pull request")

    def is_queued(self, pull: github_types.GitHubPullRequestNumber) -> bool:
        return any(
            item.embarked_pull.user_pull_request_number == pull
            for item in self._iter_embarked_pulls()
        )

    async def get_pulls(self) -> list[github_types.GitHubPullRequestNumber]:
        return [
            item.embarked_pull.user_pull_request_number
            for item in self._iter_embarked_pulls()
        ]

    async def is_first_pull(self, ctxt: context.Context) -> bool:
        item = first.first(self._iter_embarked_pulls())
        return (
            item is not None
            and item.embarked_pull.user_pull_request_number == ctxt.pull["number"]
        )

    @staticmethod
    def _get_next_batch(
        pulls: list[EmbarkedPull], queue_name: str, batch_size: int = 1
    ) -> tuple[list[EmbarkedPull], list[EmbarkedPull]]:
        if not pulls:
            return [], []

        for _i, pull in enumerate(pulls[:batch_size]):
            if pull.config["name"] != queue_name:
                # The queue change, wait first queue to be empty before processing
                # the next queue
                break
        else:
            _i += 1
        return pulls[:_i], pulls[_i:]

    @classmethod
    async def force_remove_pull(
        cls,
        repository: context.Repository,
        queue_rules: qr_config.QueueRules,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        *,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> None:
        async for train in cls.iter_trains(
            repository,
            queue_rules,
            exclude_ref=exclude_ref,
        ):
            await train._remove_pull_from_context(
                pull_number,
                signal_trigger,
                queue_utils.TargetBranchChanged(),
            )

    async def generate_merge_queue_summary_footer(
        self,
        queue_rule_report: QueueRuleReport,
        *,
        pull_rule: "prr_config.EvaluatedPullRequestRule" | None = None,
        show_queue: bool = True,
        for_queue_pull_request: bool = False,
    ) -> str:
        description = f"\n\n**Required conditions of queue** `{queue_rule_report.name}` **for merge:**\n\n"
        description += queue_rule_report.summary

        if pull_rule and not pull_rule.conditions.match:
            description += "\n\n**Required conditions to stay in the queue:**\n\n"
            description += pull_rule.conditions.get_unmatched_summary()

        if show_queue:
            table = [
                "| | Pull request | Queue/Priority | Speculative checks | Queued",
                "| ---: | :--- | :--- | :--- | :--- |",
            ]
            for i, (embarked_pull, car) in enumerate(self._iter_embarked_pulls()):
                title = await self.repository.get_pull_request_title(
                    embarked_pull.user_pull_request_number
                )
                # NOTE(sileht): we use this wierd url format to not trigger the GitHub pull request cross references
                # [#1234](/Mergifyio/mergify-engine/pull/1234]
                try:
                    fancy_priority = queue.PriorityAliases(
                        embarked_pull.config["priority"]
                    ).name
                except ValueError:
                    fancy_priority = str(embarked_pull.config["priority"])

                speculative_checks = ""
                if car is not None:
                    if car.train_car_state.checks_type == TrainCarChecksType.INPLACE:
                        speculative_checks = build_pr_link(
                            self.repository,
                            embarked_pull.user_pull_request_number,
                            "in place",
                        )
                    elif car.train_car_state.checks_type == TrainCarChecksType.DRAFT:
                        if car.queue_pull_request_number is None:
                            raise RuntimeError(
                                "car's spec checks type is draft, but queue_pull_request_number is None"
                            )

                        speculative_checks = build_pr_link(
                            self.repository, car.queue_pull_request_number
                        )

                queued_at = date.pretty_datetime(embarked_pull.queued_at)
                table.append(
                    f"| {i + 1} "
                    f"| {title} ({build_pr_link(self.repository, embarked_pull.user_pull_request_number)}) "
                    f"| {embarked_pull.config['name']}/{fancy_priority} "
                    f"| {speculative_checks} "
                    f"| {queued_at}"
                    "|"
                )

            description += (
                "\n\n**The following pull requests are queued:**\n\n" + "\n".join(table)
            )

        description += constants.MERGIFY_MERGE_QUEUE_PULL_REQUEST_DOC

        if for_queue_pull_request:
            # FIXME(sileht): This should be on top of the description in case
            # of the summary is truncated
            description += utils.get_mergify_payload(constants.MERGE_QUEUE_BODY_INFO)
        return description

    async def get_pull_summary(
        self,
        ctxt: context.Context,
        queue_rule: "qr_config.QueueRule",
        pull_rule: "prr_config.EvaluatedPullRequestRule" | None = None,
    ) -> str:
        # NOTE(sileht): beware before using this method, car.update_state() must have been called earlier
        # to have up2date informations
        _, ep = self.find_embarked_pull(ctxt.pull["number"])
        if ep is None:
            return ""
        if ep.car is None:
            description = f"#{ctxt.pull['number']} is queued for merge."
            description += await self.generate_merge_queue_summary_footer(
                queue_rule_report=QueueRuleReport(
                    name=ep.embarked_pull.config["name"],
                    summary=queue_rule.merge_conditions.get_summary(),
                ),
                pull_rule=pull_rule,
            )
            return description.strip()
        else:
            return await ep.car.generate_merge_queue_summary(pull_rule=pull_rule)

    async def _close_pull_request(
        self, pull_request_number: github_types.GitHubPullRequestNumber
    ) -> None:
        try:
            await self.repository.installation.client.patch(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/pulls/{pull_request_number}",
                json={"state": "closed"},
            )
        except http.HTTPNotFound:
            self.log.warning(
                "fail to close merge queue pull request",
                pull_request_number=pull_request_number,
                exc_info=True,
            )

    async def refresh_pulls(
        self,
        source: str,
        priority_first_pull_request: worker_pusher.Priority = worker_pusher.Priority.medium,
        additional_pull_request: None | (github_types.GitHubPullRequestNumber) = None,
    ) -> None:
        pulls = await self.get_pulls()
        if additional_pull_request and additional_pull_request not in pulls:
            pulls.append(additional_pull_request)

        pipe = await self.repository.installation.redis.stream.pipeline()
        for i, pull_number in enumerate(pulls):
            await refresher.send_pull_refresh(
                pipe,
                self.repository.repo,
                pull_request_number=pull_number,
                action="internal",
                source=source,
                priority=priority_first_pull_request
                if i == 0
                else worker_pusher.Priority.medium,
            )
        await pipe.execute()
