from collections import abc
import dataclasses
import datetime
import enum
import logging
import typing
import uuid

from ddtrace import tracer
import deepdiff
import first
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
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import worker_pusher
from mergify_engine import yaml
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models import github_user
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import checks as merge_train_checks
from mergify_engine.queue.merge_train import embarked_pull as ep_import
from mergify_engine.queue.merge_train import types as merge_train_types
from mergify_engine.queue.merge_train import utils as train_utils
from mergify_engine.rules import checks_status
from mergify_engine.rules import conditions
from mergify_engine.rules.config import partition_rules as partr_config


if typing.TYPE_CHECKING:
    from mergify_engine.queue.merge_train.train import Train
    from mergify_engine.queue.merge_train.train_car_state import TrainCarState
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config


@dataclasses.dataclass
class DraftPullRequestCreationTemporaryFailure(Exception):
    reason: str


CI_FAILED_MESSAGE = "The CI checks have failed."
CHECKS_TIMEOUT_MESSAGE = "The checks have timed out."

CHECK_ASSERTS: dict[merge_train_checks.CheckStateT | None, str] = {
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


@json.register_enum_type
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


class UnexpectedChange:
    pass


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


UNEXPECTED_CHANGE_COMPATIBILITY = {
    UnexpectedDraftPullRequestChange: TrainCarOutcome.DRAFT_PR_CHANGE,
    UnexpectedBaseBranchChange: TrainCarOutcome.BASE_BRANCH_CHANGE,
    UnexpectedUpdatedPullRequestChange: TrainCarOutcome.UPDATED_PR_CHANGE,
}


# NOTE(Syffe): type of the TrainCar's checks, can be created by rebasing a PR (inplace)
# or by creating a separate draft PR (draft)
# TODO "failed" needs to be refactored in another way since it is not relevant as a type anymore
@json.register_enum_type
@enum.unique
class TrainCarChecksType(enum.Enum):
    INPLACE = "inplace"
    DRAFT = "draft"
    FAILED = "failed"
    # Means that another TrainCar is already making checks
    # for the exact same PRs.
    # This happens only with partition rules.
    DRAFT_DELEGATED = "draft_delegated"


@dataclasses.dataclass
class TrainCarPullRequestCreationPostponed(Exception):
    car: "TrainCar"


@dataclasses.dataclass
class TrainCarPullRequestCreationFailure(Exception):
    car: "TrainCar"


@dataclasses.dataclass
class TrainCar:
    train: "Train" = dataclasses.field(repr=False)
    train_car_state: "TrainCarState" = dataclasses.field(repr=False)
    initial_embarked_pulls: list[ep_import.EmbarkedPull]
    still_queued_embarked_pulls: list[ep_import.EmbarkedPull]
    parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    initial_current_base_sha: github_types.SHAType
    queue_pull_request_number: None | (
        github_types.GitHubPullRequestNumber
    ) = dataclasses.field(default=None)
    failure_history: list["TrainCar"] = dataclasses.field(
        default_factory=list, repr=False
    )
    head_branch: str | None = None
    last_checks: list[merge_train_checks.QueueCheck] = dataclasses.field(
        default_factory=list
    )
    last_conditions_evaluation: conditions.QueueConditionEvaluationResult | None = None
    checks_ended_timestamp: datetime.datetime | None = None
    queue_branch_name: github_types.GitHubRefType | None = None
    delegating_train_cars_partition_names: list[
        partr_config.PartitionRuleName
    ] = dataclasses.field(default_factory=list)

    QUEUE_BRANCH_PREFIX: typing.ClassVar[str] = "tmp-"

    class Serialized(typing.TypedDict):
        train_car_state: "TrainCarState.Serialized"
        initial_embarked_pulls: list[ep_import.EmbarkedPull.Serialized]
        still_queued_embarked_pulls: list[ep_import.EmbarkedPull.Serialized]
        parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
        initial_current_base_sha: github_types.SHAType
        queue_pull_request_number: github_types.GitHubPullRequestNumber | None
        failure_history: list["TrainCar.Serialized"]
        head_branch: str | None
        last_checks: list[merge_train_checks.QueueCheck.Serialized]
        last_conditions_evaluation: conditions.QueueConditionEvaluationResult.Serialized | None
        checks_ended_timestamp: datetime.datetime | None
        queue_branch_name: github_types.GitHubRefType | None
        delegating_train_cars_partition_names: list[partr_config.PartitionRuleName]

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
                    merge_train_checks.QueueCheck.Serialized,
                    dataclasses.asdict(c),
                )
                for c in self.last_checks
            ],
            last_conditions_evaluation=last_conditions_evaluation,
            checks_ended_timestamp=self.checks_ended_timestamp,
            queue_branch_name=self.queue_branch_name,
            delegating_train_cars_partition_names=self.delegating_train_cars_partition_names,
        )

    @classmethod
    def deserialize(
        cls,
        train: "Train",
        data: "TrainCar.Serialized",
    ) -> "TrainCar":
        # Avoid circular import
        # from mergify_engine.rules import conditions

        if "initial_embarked_pulls" in data:
            initial_embarked_pulls = [
                ep_import.EmbarkedPull.deserialize(train, ep)
                for ep in data["initial_embarked_pulls"]
            ]
            still_queued_embarked_pulls = [
                ep_import.EmbarkedPull.deserialize(train, ep)
                for ep in data["still_queued_embarked_pulls"]
            ]

        else:
            # old format < 7.0
            initial_embarked_pulls = [  # type: ignore[unreachable]
                ep_import.EmbarkedPull(
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
            # backward compat <= 7.2.1
            failure_history = []  # type: ignore[unreachable]

        if "last_checks" in data:
            last_checks = [
                merge_train_checks.QueueCheck(**c) for c in data["last_checks"]
            ]
        else:
            # backward compat <= 7.2.1
            last_checks = []  # type: ignore[unreachable]

        if (
            checks_type == TrainCarChecksType.INPLACE
            and data["queue_pull_request_number"] is None
        ):
            data["queue_pull_request_number"] = still_queued_embarked_pulls[
                0
            ].user_pull_request_number

        if "head_branch" not in data:
            # backward compat <= 7.2.1
            if checks_type == TrainCarChecksType.DRAFT:  # type: ignore[unreachable]
                data["head_branch"] = cls._get_pulls_branch_ref(
                    initial_embarked_pulls,
                    data["parent_pull_request_numbers"],
                )
            else:
                data["head_branch"] = None

        # backward compat <= 7.2.1
        if "queue_branch_name" not in data:
            data["queue_branch_name"] = github_types.GitHubRefType(  # type: ignore[unreachable]
                f"{constants.MERGE_QUEUE_BRANCH_PREFIX}{train.ref}/{cls._get_pulls_branch_ref(initial_embarked_pulls)}"
            )

        # NOTE(Syffe): Backward compatibility for old TrainCar without TrainCarState attribute
        # (Released in version 6.0)
        train_car_state = cls._deserialize_train_car_state(
            train.convoy.repository, train.convoy.queue_rules, data, checks_type
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
            delegating_train_cars_partition_names=data.get(
                "delegating_train_cars_partition_names", []
            ),
        )

    @classmethod
    def _deserialize_train_car_state(
        cls,
        repository: context.Repository,
        queue_rules: "qr_config.QueueRules",
        data: "TrainCar.Serialized",
        checks_type: TrainCarChecksType | None,
    ) -> "TrainCarState":
        # Circular improt
        from mergify_engine.queue.merge_train import train_car_state

        if "train_car_state" in data:
            return train_car_state.TrainCarState.deserialize(
                repository, queue_rules, data["train_car_state"]
            )
        else:
            # backward compat < 6.0
            outcome = TrainCarOutcome.UNKNOWN  # type: ignore[unreachable]
            ci_state = merge_train_types.CiState.PENDING
            outcome_message = ""
            legacy_queue_conditions_conclusion = check_api.Conclusion.PENDING

            if "checks_conclusion" in data:
                legacy_queue_conditions_conclusion = data["checks_conclusion"]

            if "has_timed_out" in data and data["has_timed_out"]:
                outcome = TrainCarOutcome.CHECKS_TIMEOUT
                outcome_message = CHECKS_TIMEOUT_MESSAGE

            if "ci_has_passed" in data:
                if data["ci_has_passed"]:
                    ci_state = merge_train_types.CiState.SUCCESS
                elif legacy_queue_conditions_conclusion == check_api.Conclusion.FAILURE:
                    ci_state = merge_train_types.CiState.FAILED
                    if outcome == TrainCarOutcome.UNKNOWN:
                        outcome = TrainCarOutcome.CHECKS_FAILED
                        outcome_message = CI_FAILED_MESSAGE

            if (
                legacy_queue_conditions_conclusion == check_api.Conclusion.SUCCESS
                and ci_state == merge_train_types.CiState.SUCCESS
                and outcome == TrainCarOutcome.UNKNOWN
            ):
                outcome = TrainCarOutcome.MERGEABLE

            if "creation_date" in data:
                creation_date = data["creation_date"]
            else:
                creation_date = date.utcnow()

            return train_car_state.TrainCarState(
                outcome=outcome,
                ci_state=ci_state,
                outcome_message=outcome_message,
                checks_type=checks_type,
                ci_started_at=creation_date,
            )

    @property
    def repository(self) -> "context.Repository":
        return self.train.convoy.repository

    @property
    def ref(self) -> "github_types.GitHubRefType":
        return self.train.convoy.ref

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

    @property
    def has_unexpected_change(self) -> bool:
        return self.train_car_state.outcome in (
            TrainCarOutcome.DRAFT_PR_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
            TrainCarOutcome.BASE_BRANCH_CHANGE,
        )

    async def add_delegating_partition(
        self, partition_name: partr_config.PartitionRuleName
    ) -> None:
        if partition_name not in self.delegating_train_cars_partition_names:
            self.delegating_train_cars_partition_names.append(partition_name)

            await self.train.save()

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
        namespace_bytes = self.ref.encode()
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
        embarked_pulls: list[ep_import.EmbarkedPull] | None = None,
    ) -> str:
        if embarked_pulls is None:
            embarked_pulls = self.initial_embarked_pulls
        refs = [
            train_utils.build_pr_link(self.repository, ep.user_pull_request_number)
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
            refs = [f"Branch **{self.ref}** ({self.initial_current_base_sha[:7]})"]
        else:
            refs = [f"{self.ref} ({self.initial_current_base_sha[:7]})"]

        refs += [
            train_utils.build_pr_link(self.repository, p) if markdown else f"#{p}"
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
            TrainCarChecksType.DRAFT_DELEGATED,
        ):
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's spec checks type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            tmp_ctxt = await self.repository.get_pull_request_context(
                self.queue_pull_request_number
            )
            return [
                context.QueuePullRequest(
                    await self.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    ),
                    tmp_ctxt,
                )
                for ep in self.initial_embarked_pulls
            ]
        elif self.train_car_state.checks_type == TrainCarChecksType.FAILED:
            # Will be splitted or dropped soon
            return [
                (
                    await self.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    )
                ).pull_request
                for ep in self.initial_embarked_pulls
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
            return await self.repository.get_pull_request_context(
                self.queue_pull_request_number
            )
        else:
            return None

    def get_queue_name(self) -> "qr_config.QueueName":
        return self.initial_embarked_pulls[0].config["name"]

    async def can_be_checked_inplace(self) -> bool:
        queue_rule = self.get_queue_rule()
        if not queue_rule.config["allow_inplace_checks"]:
            return False

        # NOTE: don't check in-place if car is not the first
        if self != self.train._cars[0]:
            return False

        # NOTE: don't check in-place if there were other cars before
        if len(self.parent_pull_request_numbers) != 0:
            return False

        # NOTE: don't check in-place if it's a batch
        if len(self.initial_embarked_pulls) != 1:
            return False

        embarked_pull = self.initial_embarked_pulls[0]
        update_method = embarked_pull.config.get("update_method")
        bot_account = embarked_pull.config.get("update_bot_account")

        if update_method == "rebase" and bot_account is None:
            ctxt = await self.repository.get_pull_request_context(
                embarked_pull.user_pull_request_number
            )
            if not await ctxt.is_behind:
                # Already up to date, rebase will be a noop
                return True

            if ctxt.pull["user"]["type"] == "Bot":
                return False

        # Smart mode
        if (
            queue_rule.config["speculative_checks"] == 1
            and queue_rule.config["batch_size"] == 1
        ):
            return True

        ctxt = await self.repository.get_pull_request_context(
            embarked_pull.user_pull_request_number
        )
        # Already up to date, rebase will be a noop
        return not await ctxt.is_behind

    async def _start_checking_inplace_rebase(self, ctxt: context.Context) -> None:
        bot_account = self.still_queued_embarked_pulls[0].config.get(
            "update_bot_account"
        )

        if bot_account is None:
            if ctxt.pull["user"]["type"] == "Bot":
                raise RuntimeError(
                    f"_start_checking_inplace_rebase called with a PR from a bot: {ctxt.pull['user']['login']}"
                )
            bot_account = ctxt.pull["user"]["login"]

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                "update", bot_account
            )
        except action_utils.BotAccountNotFound as exc:
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.reason}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

        try:
            # FIXME(sileht): should we enabled autosquash here? MRGFY-279
            await branch_updater.rebase_with_git(ctxt, on_behalf, False)
        except branch_updater.BranchUpdateFailure as exc:
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.message}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

    async def _start_checking_inplace_merge(self, ctxt: context.Context) -> None:
        try:
            # FIXME(sileht): we should have passed on_behalf to update_with_api ...
            # MRGFY-1742
            await branch_updater.update_with_api(ctxt)
        except branch_updater.BranchUpdateFailure as exc:
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.message}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

    @tracer.wrap("TrainCar.start_inplace_checks", span_type="worker")
    async def start_checking_inplace(self) -> None:
        if len(self.still_queued_embarked_pulls) != 1:
            raise RuntimeError("multiple embarked_pulls but state==updated")

        embarked_pull = self.still_queued_embarked_pulls[0]

        ctxt = await self.repository.get_pull_request_context(
            embarked_pull.user_pull_request_number
        )

        if await ctxt.is_behind:
            # TODO(sileht): fallback to "merge" and None until all configs has
            # the new fields
            update_method = self.still_queued_embarked_pulls[0].config.get(
                "update_method", "merge"
            )
            if update_method == "merge":
                await self._start_checking_inplace_merge(ctxt)
            elif update_method == "rebase":
                await self._start_checking_inplace_rebase(ctxt)
            else:
                raise RuntimeError(f"Unexpected update_method: {update_method}")

            # NOTE(sileht): We must update head_sha of the pull request otherwise
            # next temporary pull request may be created on a vanished reference.
            await ctxt.update()

        else:
            # Already done, just refresh it to merge it
            await refresher.send_pull_refresh(
                self.repository.installation.redis.stream,
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
        embarked_pulls: list[ep_import.EmbarkedPull],
        parent_pr_numbers: None | (list[github_types.GitHubPullRequestNumber]) = None,
    ) -> str:
        pr_numbers = [ep.user_pull_request_number for ep in embarked_pulls]
        if parent_pr_numbers:
            pr_numbers += parent_pr_numbers

        pr_numbers_str = list(map(str, sorted(pr_numbers)))
        return "-".join(pr_numbers_str)

    async def _close_already_existing_draft_pr(
        self,
        branch_name: github_types.GitHubRefType,
        title: str,
        on_behalf: github_user.GitHubUser | None,
    ) -> None:
        head = f"{self.repository.installation.owner_login}:{branch_name}"
        closed_pulls = set()
        async for pull in typing.cast(
            abc.AsyncIterator[github_types.GitHubPullRequest],
            self.repository.installation.client.items(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/pulls",
                params={"head": head},
                resource_name="pulls",
                page_limit=20,
            ),
        ):
            closed_pulls.add(pull["number"])
            await self.train._close_pull_request(pull["number"])

        self.train.log.info(
            "failed to create a merge queue pull request, because the pull request already exists",
            head=branch_name,
            title=title,
            on_behalf=on_behalf.login if on_behalf else None,
            parent_pull_request_numbers=self.parent_pull_request_numbers,
            still_queued_embarked_pull_numbers=[
                ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
            ],
            exc_info=True,
            closed_pulls=closed_pulls,
            partition_name=self.train.partition_name,
        )

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
        on_behalf: github_user.GitHubUser | None,
    ) -> tuple[
        typing.Literal[
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
        ],
        github_types.GitHubPullRequestNumber,
    ]:
        try:
            title = f"merge queue: embarking {self._get_embarked_refs()} together"
            body = await self.generate_merge_queue_summary(for_queue_pull_request=True)
            response = await self.repository.installation.client.post(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/pulls",
                json={
                    "title": title,
                    "body": body,
                    "base": self.ref,
                    "head": branch_name,
                    "draft": True,
                },
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            )
        except http.HTTPClientSideError as e:
            if "A pull request already exists for" in e.message:
                # NOTE(sileht): Filtering pull requests on head is dangerous.
                # head must be organization:ref-name, if the left or the right side
                # of the : is empty then all pull requests are returned.
                # So it's better to double check.
                if not (self.repository.installation.owner_login and branch_name):
                    raise RuntimeError("Invalid merge queue head branch")

                if self.train.partition_name is not None:
                    # The same PR already exists in another TrainCar
                    existing_pr_number = await self.train.convoy.find_prs_in_train_cars_and_add_delegation(
                        [
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                        self.train.partition_name,
                    )
                    if existing_pr_number is not None:
                        return (TrainCarChecksType.DRAFT_DELEGATED, existing_pr_number)

                await self._close_already_existing_draft_pr(
                    branch_name, title, on_behalf
                )
                raise DraftPullRequestCreationTemporaryFailure(e.message)

            if "Draft pull requests are not supported" not in e.message:
                self.train.log.error(
                    "fail to create a merge queue pull request",
                    head=branch_name,
                    title=title,
                    on_behalf=on_behalf.login if on_behalf else None,
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
            return (
                TrainCarChecksType.DRAFT,
                typing.cast(
                    github_types.GitHubPullRequestNumber, response.json()["number"]
                ),
            )

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(tenacity.TryAgain),
        stop=tenacity.stop_after_attempt(2),
        reraise=True,
    )
    async def _prepare_draft_pr_branch(
        self,
        branch_name: github_types.GitHubRefType,
        base_sha: github_types.SHAType,
        on_behalf: github_user.GitHubUser | None,
    ) -> github_types.GitHubPullRequestNumber | None:
        """
        Return a pull request number only if the creation of the branch fails
        and another TrainCar already has an existing PR for the exact same
        PR that this TrainCar wants to check
        """
        try:
            await self.repository.installation.client.post(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/git/refs",
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                json={
                    "ref": f"refs/heads/{branch_name}",
                    "sha": base_sha,
                },
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 422 and "Reference already exists" in exc.message:
                # The same PR, with the same base_sha, already exists in another
                # TrainCar from another partition
                if self.train.partition_name is not None:
                    existing_pr_number = await self.train.convoy.find_prs_in_train_cars_and_add_delegation(
                        [
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                        self.train.partition_name,
                    )
                    if existing_pr_number is not None:
                        return existing_pr_number

                try:
                    await self._delete_branch()
                except http.HTTPClientSideError as exc_patch:
                    await self._set_creation_failure(exc_patch.message)
                    raise TrainCarPullRequestCreationFailure(self) from exc_patch

                raise tenacity.TryAgain
            else:
                await self._set_creation_failure(exc.message, report_as_error=True)
                raise TrainCarPullRequestCreationFailure(self) from exc

        return None

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(tenacity.TryAgain),
        stop=tenacity.stop_after_attempt(2),
        reraise=True,
    )
    async def _rename_branch(
        self,
        current_branch_name: github_types.GitHubRefType,
        new_branch_name: github_types.GitHubRefType,
        on_behalf: github_user.GitHubUser | None,
    ) -> None:
        try:
            await self.repository.installation.client.post(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/branches/{current_branch_name}/rename",
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
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
                ctxt = await self.repository.get_pull_request_context(
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

        on_behalf: github_user.GitHubUser | None = None
        if queue_rule.config["draft_bot_account"]:
            try:
                on_behalf = await action_utils.get_github_user_from_bot_account(
                    "prepare draft pull request", queue_rule.config["draft_bot_account"]
                )
            except action_utils.BotAccountNotFound as e:
                await self._set_creation_failure(f"{e.title}. {e.reason}")
                raise TrainCarPullRequestCreationFailure(self)

        base_sha, pulls_in_draft = await self._get_draft_pr_setup(
            queue_rule, previous_car
        )

        self.queue_branch_name = github_types.GitHubRefType(
            f"{self.QUEUE_BRANCH_PREFIX}{self.queue_branch_name}"
        )
        existing_pr_number = await self._prepare_draft_pr_branch(
            self.queue_branch_name, base_sha, on_behalf
        )
        if existing_pr_number is not None:
            await self._set_initial_state(
                TrainCarChecksType.DRAFT_DELEGATED, existing_pr_number
            )

        for pull_number in pulls_in_draft:
            try:
                await self.repository.installation.client.post(
                    f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/merges",
                    oauth_token=on_behalf.oauth_access_token if on_behalf else None,
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
        await self._rename_branch(self.queue_branch_name, new_branch_name, on_behalf)
        self.queue_branch_name = new_branch_name

        try:
            checks_type, tmp_pull_number = await self._create_draft_pull_request(
                self.queue_branch_name, on_behalf
            )
        except DraftPullRequestCreationTemporaryFailure as e:
            await self._delete_branch()
            raise TrainCarPullRequestCreationPostponed(self) from e
        await self._set_initial_state(checks_type, tmp_pull_number)

    async def _set_initial_state(
        self,
        checks_type: typing.Literal[
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
        ],
        pull_request_number: github_types.GitHubPullRequestNumber,
    ) -> None:
        self.train_car_state.ci_started_at = date.utcnow()
        self.train_car_state.checks_type = checks_type
        self.queue_pull_request_number = pull_request_number

        queue_rule = self.get_queue_rule()
        queue_pull_requests = await self.get_pull_requests_to_evaluate()
        evaluated_queue_rule = await queue_rule.get_evaluated_queue_rule(
            self.repository,
            self.ref,
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
                self.repository,
                ep.user_pull_request_number,
                "action.queue.checks_start",
                signals.EventQueueChecksStartMetadata(
                    {
                        "branch": self.ref,
                        "partition_name": self.train.partition_name,
                        "position": position,
                        "queued_at": ep.queued_at,
                        "queue_name": ep.config["name"],
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
        pull_rule: "prr_config.EvaluatedPullRequestRule | None" = None,
    ) -> str:
        description = ""
        if headline:
            description += f"**{headline}**\n\n"

        description += (
            f"{self._get_embarked_refs(markdown=True)} are embarked together for merge"
        )

        if self.train.partition_name is not None:
            if self.delegating_train_cars_partition_names:
                partition_names = sorted(
                    self.delegating_train_cars_partition_names
                    + [self.train.partition_name]
                )
                description += f" in partitions **{', '.join(partition_names)}**"
            else:
                description += f" in partition **{self.train.partition_name}**"

        description += "."

        if for_queue_pull_request:
            description += f"""

This pull request has been created by Mergify to speculatively check the mergeability of {self._get_user_refs()}.
You don't need to do anything. Mergify will close this pull request automatically when it is complete.
"""

        description += await self.train.generate_merge_queue_summary_footer(
            queue_rule_report=merge_train_types.QueueRuleReport(
                self.still_queued_embarked_pulls[0].config["name"],
                self.last_evaluated_conditions,
            ),
            pull_rule=pull_rule,
            # We don't want to show the queue if there are multiple partitions
            show_queue=show_queue and not self.delegating_train_cars_partition_names,
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

        if self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT_DELEGATED,
        ):
            return

        elif self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
            if reason is not None:
                await self._set_final_draft_pr_summary(reason, remaning_embarked_pulls)
            await self._delete_branch()

    async def _set_final_draft_pr_summary(
        self,
        reason: queue_utils.BaseUnqueueReason | None,
        reembarked_pulls: list[ep_import.EmbarkedPull],
    ) -> None:
        if (
            self.train_car_state.checks_type != TrainCarChecksType.DRAFT
            or self.queue_pull_request_number is None
        ):
            raise RuntimeError("can be called only on draft pr")
        tmp_pull_ctxt = await self.repository.get_pull_request_context(
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
                "abort_code": abort_code,
                "abort_reason": abort_reason_str,
                "abort_status": abort_status,
                "branch": self.ref,
                "queue_name": ep.config["name"],
                "partition_name": self.train.partition_name,
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
            self.repository,
            ep.user_pull_request_number,
            "action.queue.checks_end",
            metadata,
            "merge queue internal",
        )

    async def _delete_branch(self) -> None:
        if self.queue_pull_request_number is not None:
            await self.train._close_pull_request(self.queue_pull_request_number)

        if self.queue_branch_name is not None:
            await self.repository.delete_branch_if_exists(self.queue_branch_name)

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
            original_ctxt = await self.repository.get_pull_request_context(
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
                self.repository.installation.redis.stream,
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

        checked_ctxt = await self.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        mergify_bot = await github.GitHubAppInfo.get_bot(
            self.repository.installation.redis.cache
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

        checked_ctxt = await self.repository.get_pull_request_context(
            self.queue_pull_request_number
        )

        try:
            current_base_sha = await self.train.get_base_sha()
        except train_utils.BaseBranchVanished:
            checked_ctxt.log.warning(
                "target branch vanished, the merge queue will be deleted soon"
            )
            return None

        unexpected_changes: UnexpectedChange | None = None

        # TODO(MRGFY-2020): Add a way to store the merge_commit_sha of the merges we did
        # to be able to check if the current sha of the base branch is one of those
        # instead of ignoring this check if we are using partitions.
        if (
            self.train.partition_name is None
            and not await self.train.is_synced_with_the_base_branch(current_base_sha)
        ):
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
        original_pull_request_rule: "prr_config.EvaluatedPullRequestRule | None",
        original_pull_request_number: github_types.GitHubPullRequestNumber | None,
    ) -> None:
        if self.queue_pull_request_number is None:
            # Nothing to do the car has not been started yet
            return

        checked_ctxt = await self.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        queue_rule = self.get_queue_rule()
        unexpected_changes = await self.get_unexpected_change(queue_rule)
        if isinstance(unexpected_changes, UnexpectedBaseBranchChange):
            checked_ctxt.log.info(
                "train will be reset",
                gh_pull_queued=[
                    ep.user_pull_request_number for ep in self.initial_embarked_pulls
                ],
                unexpected_changes=unexpected_changes,
                partition_name=self.train.partition_name,
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

        saved_ci_state = self.train_car_state.ci_state
        saved_last_conditions_evaluation = self.last_conditions_evaluation
        saved_outcome = self.train_car_state.outcome

        check = await checked_ctxt.get_engine_check_run(
            constants.MERGE_QUEUE_SUMMARY_NAME
        )
        saved_queue_check_run_conclusion = check["conclusion"] if check else None
        saved_freeze_data = self.train_car_state.frozen_by

        if (
            origin == "original_pull_request"
            and self.train_car_state.checks_type
            in (TrainCarChecksType.DRAFT, TrainCarChecksType.DRAFT_DELEGATED)
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

        if self.train_car_state.checks_type == TrainCarChecksType.INPLACE and (
            await checked_ctxt.is_behind or await checked_ctxt.is_head_sha_outdated()
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
                self.repository, self.queue_pull_request_number, refresh_at
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
        if self.train_car_state.checks_type not in (
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
        ):
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
            self.repository.installation.redis.stream,
            self.repository.repo,
            pull_request_number=first_pull_request_in_car,
            action="internal",
            source="draft pull request state change",
            priority=worker_pusher.Priority.immediate,
        )

    def get_queue_rule(self) -> "qr_config.QueueRule":
        queue_name = self.initial_embarked_pulls[0].config["name"]
        try:
            return self.train.convoy.queue_rules[queue_name]
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
            self.train_car_state.ci_state = merge_train_types.CiState.SUCCESS
            self.train_car_state.ci_ended_at = date.utcnow()
        elif queue_conditions_conclusion == check_api.Conclusion.FAILURE:
            self.train_car_state.ci_state = merge_train_types.CiState.FAILED
            self.train_car_state.ci_ended_at = date.utcnow()
        elif queue_conditions_conclusion == check_api.Conclusion.PENDING:
            queue_pull_requests = await self.get_pull_requests_to_evaluate()
            conditions_with_only_checks = self._get_conditions_without_checks(
                evaluated_queue_rule
            )

            if await conditions_with_only_checks(queue_pull_requests):
                self.train_car_state.ci_state = merge_train_types.CiState.SUCCESS
                self.train_car_state.ci_ended_at = date.utcnow()
            else:
                self.train_car_state.ci_state = merge_train_types.CiState.PENDING
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
        qf = await self.train.convoy.is_queue_frozen(
            self.still_queued_embarked_pulls[0].config["name"],
        )
        if not qf:
            self.train_car_state.add_waiting_for_freeze_end_date()
        elif qf and self.train_car_state.ci_state == merge_train_types.CiState.SUCCESS:
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
                self.train_car_state.ci_state == merge_train_types.CiState.PENDING
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

        # Calculate next timeout refresh
        if (
            self.train_car_state.outcome == TrainCarOutcome.UNKNOWN
            and self.train_car_state.ci_state == merge_train_types.CiState.PENDING
            and timeout is not None
            and self.queue_pull_request_number is not None  # to please mypy
            and self.train_car_state.ci_started_at is not None  # to please mypy
            and not self.train_car_state.ci_has_timed_out(timeout)
        ):
            await delayed_refresh.plan_refresh_at_least_at(
                self.repository,
                self.queue_pull_request_number,
                self.train_car_state.ci_started_at + timeout,
            )

        self.train_car_state.frozen_by = (
            await self.train.convoy.get_current_queue_freeze(
                self.still_queued_embarked_pulls[0].config["name"]
            )
        )

        # Save the status of all check-runs/statuses for API/UI reporting
        if self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
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

        checked_ctxt = await self.repository.get_pull_request_context(
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
                merge_train_checks.QueueCheck(
                    name=f"{check['app_name']}/{check['name']}",
                    description=output_title,
                    avatar_url=check["app_avatar_url"],
                    url=check["html_url"],
                    state=check["conclusion"] or "pending",
                )
            )

        for status in await checked_ctxt.pull_statuses:
            self.last_checks.append(
                merge_train_checks.QueueCheck(
                    name=status["context"],
                    description=status["description"] or "",
                    avatar_url=status["avatar_url"] or "",
                    url=status["target_url"] or "",
                    state=status["state"],
                )
            )

    def get_summary_tmp_pull_title(self) -> str:
        refs = self._get_user_refs(create_link=False)
        if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            if len(self.initial_embarked_pulls) == 1:
                return f"The pull request {refs} is mergeable"
            else:
                return f"The pull requests {refs} are mergeable"
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            if len(self.initial_embarked_pulls) == 1:
                return f"The pull request {refs} is embarked for merge"
            else:
                return f"The pull requests {refs} are embarked for merge"
        elif self.has_unexpected_change:
            if len(self.initial_embarked_pulls) == 1:
                return f"The pull request {refs} cannot be merged, due to unexpected changes in this draft PR, and has been disembarked."
            else:
                return f"The pull requests {refs} cannot be merged, due to unexpected changes in this draft PR, and have been disembarked."
        elif self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_FAILED,
            TrainCarOutcome.CHECKS_TIMEOUT,
        ):
            if len(self.initial_embarked_pulls) == 1:
                return f"The pull request {refs} cannot be merged and has been disembarked."
            else:
                return f"The pull requests {refs} cannot be merged and will be split."
        elif (
            self.train_car_state.outcome
            == TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
        ):
            if len(self.initial_embarked_pulls) == 1:
                return f"The pull request {refs} cannot be merged, due to maximum batch failure resolution attempts reached, and has been disembarked."
            else:
                return f"The pull requests {refs} cannot be merged, due to maximum batch failure resolution attempts reached, and have been disembarked."
        else:
            raise RuntimeError(
                f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
            )

    def get_checks_copy_summary(
        self, checked_pull: github_types.GitHubPullRequestNumber
    ) -> str:
        if not self.last_checks:
            return ""

        checks_copy_summary = (
            "\n\nCheck-runs and statuses of the embarked "
            f"pull request #{checked_pull}:\n\n<table>"
        )
        for qcheck in merge_train_checks.get_check_list_ordered(self.last_checks):
            qcheck_icon_url = CHECK_ASSERTS.get(qcheck.state, CHECK_ASSERTS["neutral"])

            checks_copy_summary += (
                "<tr>"
                f'<td align="center" width="48" height="48"><img src="{qcheck_icon_url}" width="16" height="16" /></td>'
                f'<td align="center" width="48" height="48"><img src="{qcheck.avatar_url}&s=40" width="16" height="16" /></td>'
                f"<td><b>{qcheck.name}</b>{qcheck.description}</td>"
                f'<td><a href="{qcheck.url}">details</a></td>'
                "</tr>"
            )
        checks_copy_summary += "</table>\n"
        return checks_copy_summary

    def get_batch_failure_summary(self) -> str:
        if not self.failure_history:
            return ""

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
                speculative_checks = train_utils.build_pr_link(
                    self.repository, failure.queue_pull_request_number
                )
            else:
                speculative_checks = ""
        batch_failure_summary += f"| {self._get_user_refs()} | {self._get_embarked_refs(include_my_self=False, markdown=True)} | {speculative_checks} |"
        return batch_failure_summary

    def get_queue_summary(self) -> str:
        queue_summary = "\n\nRequired conditions for merge:\n\n"
        queue_summary += self.last_evaluated_conditions

        queue_rule = self.get_queue_rule()
        timeout = queue_rule.config["checks_timeout"]
        timeout_summary = ""
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

        return queue_summary

    def get_original_pull_request_title(self) -> str:
        if self.train_car_state.outcome == TrainCarOutcome.DRAFT_PR_CHANGE:
            return "The pull request has been removed from the queue"

        elif self.train_car_state.outcome in (
            TrainCarOutcome.BASE_BRANCH_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
        ):
            return "The pull request is going to be re-embarked soon"

        elif self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            return f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} is mergeable"
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            return f"The pull request is embarked with {self._get_embarked_refs(include_my_self=False)} for merge"

        elif self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_FAILED,
            TrainCarOutcome.CHECKS_TIMEOUT,
            TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS,
        ):
            return f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} cannot be merged and has been disembarked"

        raise RuntimeError(f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}")

    def get_original_pull_request_summary(
        self, checked_pull: github_types.GitHubPullRequestNumber
    ) -> str:
        # Circular import
        from mergify_engine.queue.merge_train.train_car_state import (
            TrainCarStateForSummary,
        )

        queue_freeze_summary = ""
        if self.train_car_state.frozen_by is not None:
            queue_freeze_summary = self.train_car_state.frozen_by.get_freeze_message()

        train_car_state_summary = TrainCarStateForSummary.from_train_car_state(
            self.train_car_state
        ).serialized()

        unexpected_change_summary = ""
        if self.has_unexpected_change:
            unexpected_change_summary = f"\n\n❌ Unexpected queue change: {self.train_car_state.outcome_message}. ❌"

        original_pull_summary = (
            train_car_state_summary
            + unexpected_change_summary
            + queue_freeze_summary
            + self.get_queue_summary()
            + self.get_checks_copy_summary(checked_pull)
            + self.get_batch_failure_summary()
        )
        return original_pull_summary

    async def update_summaries(self) -> None:
        tmp_pull_title = self.get_summary_tmp_pull_title()
        queue_summary = self.get_queue_summary()
        batch_failure_summary = self.get_batch_failure_summary()

        if self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
            summary = f"Embarking {self._get_embarked_refs(markdown=True)} together"
            summary += queue_summary + batch_failure_summary

            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    "car's spec checks type is draft, but queue_pull_request_number is None"
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
            elif self.has_unexpected_change:
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

            tmp_pull_ctxt = await self.repository.get_pull_request_context(
                self.queue_pull_request_number
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
        elif self.train_car_state.checks_type in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT_DELEGATED,
        ):
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's spec checks type is {self.train_car_state.checks_type},"
                    " but queue_pull_request_number is None"
                )
            checked_pull = self.queue_pull_request_number
        else:
            checked_pull = github_types.GitHubPullRequestNumber(0)

        self.train.log.info(
            "pull request train car status update",
            legacy_conclusion=self.get_queue_check_run_conclusion(),
            outcome=self.train_car_state.outcome,
            outcome_message=self.train_car_state.outcome_message,
            original_pull_title=self.get_original_pull_request_title(),
            gh_pull=checked_pull,
            queue_summary=queue_summary.strip(),
            batch_failure_summary=batch_failure_summary.strip(),
            checks_copy_summary={check.name: check.state for check in self.last_checks},
            still_embarked_pull_numbers=[
                ep.user_pull_request_number for ep in self.still_queued_embarked_pulls
            ],
            initial_embarked_pull_numbers=[
                ep.user_pull_request_number for ep in self.initial_embarked_pulls
            ],
            partition_name=self.train.partition_name,
        )

        for ep in self.still_queued_embarked_pulls:
            await self.train.convoy.update_user_pull_request_summary(
                ep.user_pull_request_number,
                checked_pull,
                self,
            )

        if (
            self.train_car_state.checks_type == TrainCarChecksType.DRAFT
            and self.get_queue_check_run_conclusion() != check_api.Conclusion.PENDING
            and not tmp_pull_ctxt.closed
        ):
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
                self.repository.installation.redis.stream,
                self.repository.repo,
                pull_request_number=ep.user_pull_request_number,
                action="internal",
                source="draft pull request state change",
                priority=worker_pusher.Priority.immediate
                if i == 0
                and self.train_car_state.checks_type == TrainCarChecksType.INPLACE
                else worker_pusher.Priority.medium,
            )

    def _get_previous_car(self) -> "TrainCar | None":
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
