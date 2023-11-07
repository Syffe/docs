from __future__ import annotations

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
from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import dashboard
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import pull_request_finder
from mergify_engine import refresher
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import worker_pusher
from mergify_engine import yaml
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models.github import user as github_user
from mergify_engine.queue import pause
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import checks as merge_train_checks
from mergify_engine.queue.merge_train import embarked_pull as ep_import
from mergify_engine.queue.merge_train import types as merge_train_types
from mergify_engine.queue.merge_train import utils as train_utils
from mergify_engine.rules import checks_status
from mergify_engine.rules import conditions
from mergify_engine.rules.config import partition_rules as partr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context
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
    "success": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/check-green-16.png",
    # red x
    "failure": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/x-red-16.png",
    "error": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/x-red-16.png",
    "action_required": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/x-red-16.png",
    "timed_out": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/x-red-16.png",
    # yellow dot
    "pending": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/dot-yellow-16.png",
    None: "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/dot-yellow-16.png",
    # grey square
    "neutral": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/square-grey-16.png",
    "stale": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/square-grey-16.png",
    # skipped stop sign
    "skipped": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/circle-skipped-grey-16.png",
    # cancelled stop sign
    "cancelled": "https://raw.githubusercontent.com/Mergifyio/mergify/master/assets/circle-cancelled-grey-16.png",
}


@json.register_enum_type
@enum.unique
class TrainCarOutcome(enum.Enum):
    MERGEABLE = "mergeable"
    CHECKS_TIMEOUT = "checks_timeout"
    CHECKS_FAILED = "checks_failed"
    BASE_BRANCH_CHANGE = "base_branch_change"
    DRAFT_PR_CHANGE = "draft_pr_change"
    UPDATED_PR_CHANGE = "updated_pr_change"
    UNKNOWN = "unknown"
    BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS = "batch_max_failure_resolution_attempts"
    PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE = (
        "pr_checks_stopped_because_merge_queue_pause"
    )
    CONFLICT_WITH_BASE_BRANCH = "conflict_with_base_branch"
    CONFLICT_WITH_PULL_AHEAD = "conflict_with_pull_ahead"
    BRANCH_UPDATE_FAILED = "branch_update_failed"


class UnexpectedChanges:
    pass


@dataclasses.dataclass
class UnexpectedDraftPullRequestChange(UnexpectedChanges):
    draft_pull_request_number: github_types.GitHubPullRequestNumber

    def __str__(self) -> str:
        return f"the draft pull request #{self.draft_pull_request_number} has sustained unexpected changes from external sources"


@dataclasses.dataclass
class UnexpectedUpdatedPullRequestChange(UnexpectedChanges):
    updated_pull_request_number: github_types.GitHubPullRequestNumber

    def __str__(self) -> str:
        return f"the updated pull request #{self.updated_pull_request_number} has been manually updated"


@dataclasses.dataclass
class UnexpectedBaseBranchChange(UnexpectedChanges):
    base_sha: github_types.SHAType

    def __str__(self) -> str:
        return f"an external action moved the target branch head to {self.base_sha}"


@dataclasses.dataclass
class MergifySupportReset(UnexpectedChanges):
    def __str__(self) -> str:
        return "Mergify support has resetted the merge-queue"


UNEXPECTED_CHANGES_COMPATIBILITY = {
    UnexpectedDraftPullRequestChange: TrainCarOutcome.DRAFT_PR_CHANGE,
    UnexpectedBaseBranchChange: TrainCarOutcome.BASE_BRANCH_CHANGE,
    UnexpectedUpdatedPullRequestChange: TrainCarOutcome.UPDATED_PR_CHANGE,
}


@dataclasses.dataclass
class BatchFailureSummary:
    title: str
    pull_request: str
    parent_pull_requests: str
    spec_checks: list[str]


@dataclasses.dataclass
class QueueSummary:
    title: str
    train_car_state: str
    unexpected_changes: str | None
    freeze: str | None
    pause: str | None
    checks_timeout: str | None
    merge_conditions: str
    check_runs: str | None
    batch_failure: BatchFailureSummary | None

    @property
    def body(self) -> str:
        body = self.train_car_state
        if self.unexpected_changes is not None:
            body += "\n\n" + self.unexpected_changes

        if self.freeze is not None:
            body += "\n\n" + self.freeze

        if self.pause is not None:
            body += "\n\n" + self.pause

        if self.checks_timeout is not None:
            body += "\n\n" + self.checks_timeout

        body += "\n\n" + self.merge_conditions

        if self.check_runs is not None:
            body += "\n\n" + self.check_runs

        if self.batch_failure is not None:
            body += "\n\n" + self.build_batch_failure_summary()

        return body

    def build_batch_failure_summary(self) -> str:
        if self.batch_failure is not None:
            batch_failure_summary = (
                self.batch_failure.title
                + "\n\n| Pull request | Parents pull requests | Speculative checks |\n"
                + "| ---: | :--- | :--- |\n"
            )
            for spec_check in self.batch_failure.spec_checks:
                batch_failure_summary += f"| {self.batch_failure.pull_request} | {self.batch_failure.parent_pull_requests} | {spec_check} |"

            return batch_failure_summary
        return ""

    def get_batch_failure_summary_title(self) -> str | None:
        if self.batch_failure is not None:
            return self.batch_failure.title
        return None


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
    car: TrainCar


@dataclasses.dataclass
class TrainCarPullRequestCreationFailure(Exception):
    car: TrainCar
    guilty_prs: list[github_types.GitHubPullRequestNumber] = dataclasses.field(
        default_factory=list
    )

    def __post_init__(self) -> None:
        if not self.guilty_prs:
            self.guilty_prs = [
                ep.user_pull_request_number
                for ep in self.car.still_queued_embarked_pulls
            ]


@dataclasses.dataclass
class TrainCar:
    train: Train = dataclasses.field(repr=False)
    train_car_state: TrainCarState = dataclasses.field(repr=False)
    initial_embarked_pulls: list[ep_import.EmbarkedPull]
    still_queued_embarked_pulls: list[ep_import.EmbarkedPull]
    parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
    initial_current_base_sha: github_types.SHAType
    queue_pull_request_number: None | (
        github_types.GitHubPullRequestNumber
    ) = dataclasses.field(default=None)
    failure_history: list[TrainCar] = dataclasses.field(
        default_factory=list, repr=False
    )
    head_branch: str | None = None
    last_checks: list[merge_train_checks.QueueCheck] = dataclasses.field(
        default_factory=list
    )
    last_merge_conditions_evaluation: conditions.QueueConditionEvaluationResult | None = None
    last_queue_conditions_evaluation: conditions.QueueConditionEvaluationResult | None = None
    checks_ended_timestamp: datetime.datetime | None = None
    queue_branch_name: github_types.GitHubRefType | None = None
    delegating_train_cars_partition_names: list[
        partr_config.PartitionRuleName
    ] = dataclasses.field(default_factory=list)

    QUEUE_BRANCH_PREFIX: typing.ClassVar[str] = "tmp-"

    class Serialized(typing.TypedDict):
        train_car_state: TrainCarState.Serialized
        initial_embarked_pulls: list[ep_import.EmbarkedPull.Serialized]
        still_queued_embarked_pulls: list[ep_import.EmbarkedPull.Serialized]
        parent_pull_request_numbers: list[github_types.GitHubPullRequestNumber]
        initial_current_base_sha: github_types.SHAType
        queue_pull_request_number: github_types.GitHubPullRequestNumber | None
        failure_history: list[TrainCar.Serialized]
        head_branch: str | None
        last_checks: list[merge_train_checks.QueueCheck.Serialized]
        # Backward compat, introduced in 7.6.0
        last_conditions_evaluation: conditions.QueueConditionEvaluationResult.Serialized | None
        last_merge_conditions_evaluation: conditions.QueueConditionEvaluationResult.Serialized | None
        last_queue_conditions_evaluation: conditions.QueueConditionEvaluationResult.Serialized | None
        checks_ended_timestamp: datetime.datetime | None
        queue_branch_name: github_types.GitHubRefType | None
        delegating_train_cars_partition_names: list[partr_config.PartitionRuleName]

    def serialized(self) -> TrainCar.Serialized:
        if self.last_merge_conditions_evaluation is not None:
            last_merge_conditions_evaluation = (
                self.last_merge_conditions_evaluation.serialized()
            )
        else:
            last_merge_conditions_evaluation = None

        if self.last_queue_conditions_evaluation is not None:
            last_queue_conditions_evaluation = (
                self.last_queue_conditions_evaluation.serialized()
            )
        else:
            last_queue_conditions_evaluation = None

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
            last_checks=[c.serialized() for c in self.last_checks],
            last_merge_conditions_evaluation=last_merge_conditions_evaluation,
            last_queue_conditions_evaluation=last_queue_conditions_evaluation,
            last_conditions_evaluation=None,
            checks_ended_timestamp=self.checks_ended_timestamp,
            queue_branch_name=self.queue_branch_name,
            delegating_train_cars_partition_names=self.delegating_train_cars_partition_names,
        )

    @classmethod
    def deserialize(
        cls,
        train: Train,
        data: TrainCar.Serialized,
    ) -> TrainCar:
        initial_embarked_pulls = [
            ep_import.EmbarkedPull.deserialize(train, ep)
            for ep in data["initial_embarked_pulls"]
        ]
        still_queued_embarked_pulls = [
            ep_import.EmbarkedPull.deserialize(train, ep)
            for ep in data["still_queued_embarked_pulls"]
        ]

        failure_history = [
            TrainCar.deserialize(train, fh) for fh in data["failure_history"]
        ]

        last_checks = [
            merge_train_checks.QueueCheck.deserialize(c) for c in data["last_checks"]
        ]

        from mergify_engine.queue.merge_train import (
            train_car_state as train_car_state_mod,
        )

        train_car_state = train_car_state_mod.TrainCarState.deserialize(
            train.convoy.repository, train.convoy.queue_rules, data["train_car_state"]
        )

        # Backward compat, introduced in 7.6.0
        if data.get("last_merge_conditions_evaluation") is not None:
            last_merge_conditions_evaluation = (
                conditions.QueueConditionEvaluationResult.deserialize(
                    typing.cast(
                        conditions.QueueConditionEvaluationResult.Serialized,
                        data["last_merge_conditions_evaluation"],
                    )
                )
            )
        elif data.get("last_conditions_evaluation") is not None:
            last_merge_conditions_evaluation = (
                conditions.QueueConditionEvaluationResult.deserialize(
                    typing.cast(
                        conditions.QueueConditionEvaluationResult.Serialized,
                        data["last_conditions_evaluation"],
                    )
                )
            )
        else:
            last_merge_conditions_evaluation = None

        # Backward compat, introduced in 7.6.0
        if data.get("last_queue_conditions_evaluation") is not None:
            last_queue_conditions_evaluation = (
                conditions.QueueConditionEvaluationResult.deserialize(
                    typing.cast(
                        conditions.QueueConditionEvaluationResult.Serialized,
                        data["last_queue_conditions_evaluation"],
                    )
                )
            )
        else:
            last_queue_conditions_evaluation = None

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
            last_merge_conditions_evaluation=last_merge_conditions_evaluation,
            last_queue_conditions_evaluation=last_queue_conditions_evaluation,
            checks_ended_timestamp=data.get("checks_ended_timestamp"),
            queue_branch_name=data["queue_branch_name"],
            delegating_train_cars_partition_names=data.get(
                "delegating_train_cars_partition_names", []
            ),
        )

    @property
    def repository(self) -> context.Repository:
        return self.train.convoy.repository

    @property
    def ref(self) -> github_types.GitHubRefType:
        return self.train.convoy.ref

    @property
    def is_batch_failure_resolved(self) -> bool:
        return self not in self.train._cars or (
            self.has_previous_car_status_succeeded()
            and len(self.initial_embarked_pulls) == 1
        )

    @property
    def last_evaluated_merge_conditions(self) -> str:
        if self.last_merge_conditions_evaluation is not None:
            return self.last_merge_conditions_evaluation.as_markdown()
        return ""

    @property
    def last_evaluated_queue_conditions(self) -> str:
        if self.last_queue_conditions_evaluation is not None:
            return self.last_queue_conditions_evaluation.as_markdown()
        return ""

    @property
    def has_unexpected_changes(self) -> bool:
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

        if self.train_car_state.outcome in (
            TrainCarOutcome.UNKNOWN,
            TrainCarOutcome.BASE_BRANCH_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
            TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE,
        ):
            return check_api.Conclusion.PENDING

        if self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_TIMEOUT,
            TrainCarOutcome.DRAFT_PR_CHANGE,
            TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS,
        ):
            return check_api.Conclusion.FAILURE

        if self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED:
            if self.is_batch_failure_resolved:
                return check_api.Conclusion.FAILURE
            return check_api.Conclusion.PENDING

        raise RuntimeError(f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}")

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
        if len(refs) == 1:
            return refs[-1]
        return f"{', '.join(refs[:-1])} and {refs[-1]}"

    async def get_pull_requests_to_evaluate(
        self,
    ) -> list[condition_value_querier.BasePullRequest]:
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
                condition_value_querier.QueuePullRequest(
                    await self.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    ),
                    tmp_ctxt,
                )
                for ep in self.initial_embarked_pulls
            ]

        if self.train_car_state.checks_type == TrainCarChecksType.FAILED:
            # Will be splitted or dropped soon
            return [
                condition_value_querier.PullRequest(
                    await self.repository.get_pull_request_context(
                        ep.user_pull_request_number
                    )
                )
                for ep in self.initial_embarked_pulls
            ]

        raise RuntimeError(
            f"Invalid spec checks type: {self.train_car_state.checks_type}"
        )

    async def get_context_to_evaluate(self) -> context.Context | None:
        if self.train_car_state.checks_type not in (
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
        ):
            return None

        if self.queue_pull_request_number is None:
            raise RuntimeError(
                f"car's spec check type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
            )

        return await self.repository.get_pull_request_context(
            self.queue_pull_request_number
        )

    def get_queue_name(self) -> qr_config.QueueName:
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
                self.repository, "update", bot_account, required_permissions=[]
            )
        except action_utils.BotAccountNotFound as exc:
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.reason}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

        autosquash = self.still_queued_embarked_pulls[0].config.get("autosquash", True)
        try:
            await branch_updater.rebase_with_git(ctxt, on_behalf, autosquash)
        except branch_updater.BranchUpdateFailure as exc:
            self.train_car_state.outcome = TrainCarOutcome.BRANCH_UPDATE_FAILED
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.message}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

    async def _start_checking_inplace_merge(self, ctxt: context.Context) -> None:
        # Do not use any oauth_token for this update, otherwise we won't be able
        # to detect when we did the merge or if the user did it.
        try:
            await branch_updater.update_with_api(ctxt)
        except branch_updater.BranchUpdateFailure as exc:
            self.train_car_state.outcome = TrainCarOutcome.BRANCH_UPDATE_FAILED
            await self._set_creation_failure(
                f"{exc.title}\n\n{exc.message}", operation="updated"
            )
            raise TrainCarPullRequestCreationFailure(self) from exc

    @tracer.wrap("TrainCar.start_inplace_checks")
    async def start_checking_inplace(self) -> None:
        if len(self.still_queued_embarked_pulls) != 1:
            raise RuntimeError("multiple embarked_pulls but state==updated")

        embarked_pull = self.still_queued_embarked_pulls[0]

        ctxt = await self.repository.get_pull_request_context(
            embarked_pull.user_pull_request_number
        )

        # TODO(sileht): fallback to "merge" and None until all configs has
        # the new fields
        update_method = self.still_queued_embarked_pulls[0].config.get(
            "update_method", "merge"
        )
        autosquash = self.still_queued_embarked_pulls[0].config.get("autosquash", True)

        if await ctxt.is_behind:
            if update_method == "merge":
                await self._start_checking_inplace_merge(ctxt)
            elif update_method == "rebase":
                await self._start_checking_inplace_rebase(ctxt)
            else:
                raise RuntimeError(f"Unexpected update_method: {update_method}")

            # NOTE(sileht): We must update head_sha of the pull request otherwise
            # next temporary pull request may be created on a vanished reference.
            await ctxt.update()
        elif (
            update_method == "rebase"
            and autosquash
            and await ctxt.has_squashable_commits()
        ):
            await self._start_checking_inplace_rebase(ctxt)

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

        await self._set_initial_state(TrainCarChecksType.INPLACE, ctxt.pull)

    @staticmethod
    def _get_pulls_branch_ref(
        embarked_pulls: list[ep_import.EmbarkedPull],
        parent_pr_numbers: None | (list[github_types.GitHubPullRequestNumber]) = None,
    ) -> str:
        pr_numbers = [ep.user_pull_request_number for ep in embarked_pulls]
        if parent_pr_numbers:
            pr_numbers += parent_pr_numbers

        return "-".join(map(str, sorted(pr_numbers)))

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

    @tracer.wrap("TrainCar._create_draft_pull_request")
    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            DraftPullRequestCreationTemporaryFailure
        ),
        stop=tenacity.stop_after_attempt(2),
        reraise=True,
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
        github_types.GitHubPullRequest,
    ]:
        try:
            title = f"merge queue: embarking {self._get_embarked_refs()} together"
            body = await self.build_draft_pr_summary(for_queue_pull_request=True)

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
                extensions={
                    "retry": lambda response: response.status_code == 422
                    and "Validation Failed" in http.extract_message(response),
                    "retry_log": lambda retry_state: self.train.log.info(
                        "retrying to create a merge queue pull request",
                        head=branch_name,
                        title=title,
                        ref=self.ref,
                        on_behalf=on_behalf.login if on_behalf else None,
                        parent_pull_request_numbers=self.parent_pull_request_numbers,
                        still_queued_embarked_pull_numbers=[
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                    ),
                },
            )

        except http.HTTPClientSideError as e:
            if "A pull request already exists for" in e.message:
                # NOTE(sileht): Filtering pull requests on head is dangerous.
                # head must be organization:ref-name, if the left or the right side
                # of the : is empty then all pull requests are returned.
                # So it's better to double check.
                if not (self.repository.installation.owner_login and branch_name):
                    raise RuntimeError("Invalid merge queue head branch")

                if self.train.partition_name != partr_config.DEFAULT_PARTITION_NAME:
                    # The same PR already exists in another TrainCar
                    existing_pr = await self.train.convoy.find_prs_in_train_cars_and_add_delegation(
                        [
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                        self.train.partition_name,
                    )
                    if existing_pr is not None:
                        return (TrainCarChecksType.DRAFT_DELEGATED, existing_pr)

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
                typing.cast(github_types.GitHubPullRequest, response.json()),
            )

    async def _prepare_draft_pr_branch(
        self,
        branch_name: github_types.GitHubRefType,
        base_sha: github_types.SHAType,
        on_behalf: github_user.GitHubUser | None,
    ) -> github_types.GitHubPullRequest | None:
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
                extensions={
                    "retry": lambda response: response.status_code == 422
                    and "Reference already exists" in http.extract_message(response),
                    "retry_log": lambda retry_state: self.train.log.info(
                        "retrying to create the merge-queue branch",
                        branch_name=branch_name,
                        sha=base_sha,
                    ),
                },
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 422 and "Reference already exists" in exc.message:
                # The same PR, with the same base_sha, already exists in another
                # TrainCar from another partition
                if self.train.partition_name != partr_config.DEFAULT_PARTITION_NAME:
                    existing_pr = await self.train.convoy.find_prs_in_train_cars_and_add_delegation(
                        [
                            ep.user_pull_request_number
                            for ep in self.still_queued_embarked_pulls
                        ],
                        self.train.partition_name,
                    )
                    if existing_pr is not None:
                        return existing_pr

                try:
                    await self._delete_branch()
                except http.HTTPClientSideError as exc_patch:
                    await self._set_creation_failure(exc_patch.message)
                    raise TrainCarPullRequestCreationFailure(self) from exc_patch

            await self._set_creation_failure(exc.message, report_as_error=True)
            raise TrainCarPullRequestCreationFailure(self) from exc

        return None

    async def _merge_pull_into_draft_pr_branch(
        self, pull_number: int, on_behalf: github_user.GitHubUser | None = None
    ) -> None:
        await self.repository.installation.client.post(
            f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/merges",
            oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            json={
                "base": self.queue_branch_name,
                "head": f"refs/pull/{pull_number}/head",
                "commit_message": f"Merge of #{pull_number}",
            },
            extensions={
                "retry": lambda response: response.status_code == 404
                and (
                    "Base does not exist" in http.extract_message(response)
                    or "Head does not exist" in http.extract_message(response)
                ),
                "retry_log": lambda retry_state: self.repository.log.info(
                    "fail to merge pull request into draft pr branch",
                    gh_pull=pull_number,
                    queue_branch_name=self.queue_branch_name,
                ),
            },
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
                extensions={
                    "retry": lambda response: response.status_code == 404,
                    "retry_log": lambda retry_state: self.train.log.info(
                        "retrying to rename merge-queue branch",
                        current_branch_name=current_branch_name,
                        new_branch_name=new_branch_name,
                    ),
                },
            )
        except http.HTTPClientSideError as exc:
            if exc.status_code == 422 and "New branch already exists" in exc.message:
                try:
                    await self._delete_branch()
                except http.HTTPClientSideError as exc_patch:
                    await self._set_creation_failure(exc_patch.message)
                    raise TrainCarPullRequestCreationFailure(self) from exc_patch

            elif (
                exc.status_code == 403
                and exc.message == "Resource not accessible by integration"
            ):
                # There are some branch protection rules that forbids us from renaming the branch.
                # Fallback to duplicating old branch ref with a different branch name
                await self.duplicate_old_branch_to_new_branch(
                    current_branch_name, new_branch_name, on_behalf
                )
            else:
                await self._set_creation_failure(exc.message, report_as_error=True)
                raise TrainCarPullRequestCreationFailure(self) from exc

    async def duplicate_old_branch_to_new_branch(
        self,
        current_branch_name: github_types.GitHubRefType,
        new_branch_name: github_types.GitHubRefType,
        on_behalf: github_user.GitHubUser | None,
    ) -> None:
        old_ref = await self.repository.installation.client.get(
            f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/git/refs/heads/{current_branch_name}",
            oauth_token=on_behalf.oauth_access_token if on_behalf else None,
        )

        try:
            await self.repository.installation.client.post(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/git/refs",
                json={
                    "ref": f"refs/heads/{new_branch_name}",
                    "sha": old_ref.json()["object"]["sha"],
                },
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            )

            await self.repository.installation.client.delete(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/git/refs/heads/{current_branch_name}",
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            )
        except http.HTTPClientSideError as exc:
            await self._set_creation_failure(exc.message, report_as_error=True)
            raise TrainCarPullRequestCreationFailure(self) from exc

    async def _get_draft_pr_setup(
        self,
        queue_rule: qr_config.QueueRule,
        previous_car: TrainCar | None,
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

    @tracer.wrap("TrainCar.start_checking_with_draft")
    async def start_checking_with_draft(
        self,
        previous_car: TrainCar | None,
    ) -> None:
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
                    self.repository,
                    "prepare draft pull request",
                    queue_rule.config["draft_bot_account"],
                    required_permissions=[],
                )
            except action_utils.BotAccountNotFound as e:
                await self._set_creation_failure(f"{e.title}. {e.reason}")
                raise TrainCarPullRequestCreationFailure(self)

        base_sha, pulls_in_draft = await self._get_draft_pr_setup(
            queue_rule, previous_car
        )
        if not pulls_in_draft:
            self.train.log.error(
                "We are going to create a train car with no pull requests",
                initial_embarked_pulls=[
                    ep.user_pull_request_number for ep in self.initial_embarked_pulls
                ],
                still_embarked_pull_numbers=[
                    ep.user_pull_request_number
                    for ep in self.still_queued_embarked_pulls
                ],
            )

        self.queue_branch_name = github_types.GitHubRefType(
            f"{self.QUEUE_BRANCH_PREFIX}{self.queue_branch_name}"
        )

        existing_pr = await self._prepare_draft_pr_branch(
            self.queue_branch_name, base_sha, on_behalf
        )
        if existing_pr is not None:
            await self._set_initial_state(
                TrainCarChecksType.DRAFT_DELEGATED, existing_pr
            )

        for pull_number in pulls_in_draft:
            try:
                await self._merge_pull_into_draft_pr_branch(pull_number, on_behalf)
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

                if "Merge conflict" in e.message:
                    pull_requests_ahead = self.parent_pull_request_numbers[:]
                    for ep in self.still_queued_embarked_pulls:
                        if ep.user_pull_request_number == pull_number:
                            break
                        pull_requests_ahead.append(ep.user_pull_request_number)

                    if not pull_requests_ahead:
                        self.train_car_state.outcome = (
                            TrainCarOutcome.CONFLICT_WITH_BASE_BRANCH
                        )
                        message = "The pull request conflicts with the base branch"
                    else:
                        self.train_car_state.outcome = (
                            TrainCarOutcome.CONFLICT_WITH_PULL_AHEAD
                        )
                        message = "The pull request conflicts with at least one pull request ahead in queue: "
                        message += ", ".join([f"#{p}" for p in pull_requests_ahead])

                    await self._set_creation_failure(
                        message, pull_requests_to_remove=[pull_number]
                    )
                    await self._delete_branch()
                    raise TrainCarPullRequestCreationFailure(self, [pull_number]) from e

                await self._set_creation_failure(
                    e.message,
                    pull_requests_to_remove=[pull_number],
                    report_as_error=True,
                )
                await self._delete_branch()
                raise TrainCarPullRequestCreationFailure(self, [pull_number]) from e

        new_branch_name = github_types.GitHubRefType(
            self.queue_branch_name.replace(self.QUEUE_BRANCH_PREFIX, "", 1)
        )
        await self._rename_branch(self.queue_branch_name, new_branch_name, on_behalf)
        self.queue_branch_name = new_branch_name

        try:
            checks_type, tmp_pull = await self._create_draft_pull_request(
                self.queue_branch_name, on_behalf
            )
        except DraftPullRequestCreationTemporaryFailure as e:
            await self._delete_branch()
            raise TrainCarPullRequestCreationPostponed(self) from e

        await self._set_initial_state(checks_type, tmp_pull)

    async def send_checks_start_signal(self) -> None:
        for ep in self.still_queued_embarked_pulls:
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    "expected the train car to be started to send the queue.checks_start signal"
                )

            position, ep_with_car = self.train.find_embarked_pull(
                ep.user_pull_request_number
            )
            if position is None or ep_with_car is None:
                raise RuntimeError("TrainCar with embarked_pull not in train...")

            checks_end_metadata = ep_with_car.embarked_pull.checks_end_metadata
            if checks_end_metadata.get("aborted") and (
                abort_message := checks_end_metadata["abort_reason"]
            ):
                start_reason = f"Previous attempt aborted with message: '{abort_message[0].lower() + abort_message[1:]}'"
            else:
                start_reason = (
                    f"First time checking pull request #{ep.user_pull_request_number}"
                )

            await signals.send(
                self.repository,
                ep.user_pull_request_number,
                self.ref,
                "action.queue.checks_start",
                metadata=signals.EventQueueChecksStartMetadata(
                    {
                        "branch": self.ref,
                        "partition_name": self.train.partition_name,
                        "position": position,
                        "queued_at": ep.queued_at,
                        "queue_name": ep.config["name"],
                        "start_reason": start_reason,
                        "speculative_check_pull_request": {
                            "number": self.queue_pull_request_number,
                            "in_place": self.train_car_state.checks_type
                            == TrainCarChecksType.INPLACE,
                            "checks_conclusion": self.get_queue_check_run_conclusion().value
                            or "pending",
                            "checks_timed_out": self.train_car_state.outcome
                            == TrainCarOutcome.CHECKS_TIMEOUT,
                            "checks_started_at": None,
                            "checks_ended_at": self.checks_ended_timestamp,
                            "unsuccessful_checks": [],
                        },
                    }
                ),
                trigger="merge queue internal",
            )

    async def _set_initial_state(
        self,
        checks_type: typing.Literal[
            TrainCarChecksType.INPLACE,
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
        ],
        pull_request: github_types.GitHubPullRequest,
    ) -> None:
        self.train_car_state.ci_started_at = date.utcnow()
        self.train_car_state.checks_type = checks_type
        self.queue_pull_request_number = pull_request["number"]

        # NOTE(Syffe): As we don't use set_summary_check() in the summary update
        # the function _save_cached_last_summary_head_sha() is not called anymore
        # thus not updating the cache
        await pull_request_finder.PullRequestFinder.sync(
            self.repository.installation.redis.cache, pull_request
        )
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

        # Needed for statistics_accuracy signal, otherwise the `action.queue.checks_start`
        # signal is sent without the train being saved, which will cause
        # the train car to be None and cause the ETA calculated to always be None
        await self.train.save()
        await self.send_checks_start_signal()

    async def build_draft_pr_summary(
        self,
        *,
        for_queue_pull_request: bool = False,
        show_queue: bool = True,
        headline: str | None = None,
        pull_rule: prr_config.EvaluatedPullRequestRule | None = None,
    ) -> str:
        description = ""
        if headline:
            description += f"**{headline}**\n\n"

        description += (
            f"{self._get_embarked_refs(markdown=True)} are embarked together for merge"
        )

        if self.train.partition_name != partr_config.DEFAULT_PARTITION_NAME:
            if self.delegating_train_cars_partition_names:
                partition_names = sorted(
                    (
                        *self.delegating_train_cars_partition_names,
                        self.train.partition_name,
                    )
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
                self.last_evaluated_merge_conditions,
            ),
            pull_rule=pull_rule,
            for_queue_pull_request=for_queue_pull_request,
            required_conditions_to_stay_in_queue=self.last_queue_conditions_evaluation.as_markdown()
            if self.last_queue_conditions_evaluation is not None
            else None,
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
            "previous_failed_batches": [
                {
                    "draft_pr_number": tc.queue_pull_request_number,
                    "checked_pull_requests": [
                        ep.user_pull_request_number for ep in tc.initial_embarked_pulls
                    ],
                }
                for tc in self.failure_history
            ],
        }
        description = "```yaml\n---\n"
        # TODO(sileht): use regular dumper, to use the C parser
        description += yaml.dump_with_indented_list(yaml_dict)
        description += "...\n\n```"

        return description

    async def end_checking(
        self,
        reason: queue_utils.BaseQueueCancelReason | None,
        not_reembarked_pull_requests: dict[
            github_types.GitHubPullRequestNumber, queue_utils.BaseQueueCancelReason
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

        if self.train_car_state.checks_type == TrainCarChecksType.DRAFT:
            if reason is not None:
                await self._set_final_draft_pr_summary(reason, remaning_embarked_pulls)
            await self._delete_branch()

    async def _set_final_draft_pr_summary(
        self,
        reason: queue_utils.BaseQueueCancelReason | None,
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

            body = await self.build_draft_pr_summary(
                for_queue_pull_request=True,
                headline=headline,
                show_queue=False,
            )

            if tmp_pull_ctxt.body != body:
                await tmp_pull_ctxt.client.patch(
                    f"{tmp_pull_ctxt.base_url}/pulls/{self.queue_pull_request_number}",
                    json={"body": body},
                )

            tmp_pull_ctxt.log.info("train car deleted", reason=reason)

    async def send_checks_end_signal(
        self,
        user_pull_request_number: github_types.GitHubPullRequestNumber,
        cancel_reason: queue_utils.BaseQueueCancelReason | None,
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
        if cancel_reason is None or isinstance(cancel_reason, queue_utils.PrMerged):
            aborted = False
            abort_reason_str = ""
            abort_code = None
        else:
            aborted = True
            abort_reason_str = str(cancel_reason)
            abort_code = typing.cast(queue_utils.AbortCodeT, cancel_reason.unqueue_code)

        raw_unsuccessful_checks = [
            check
            for check in self.last_checks
            if check.state not in ("success", "neutral")
        ]

        if (
            self.train_car_state.outcome
            in (
                TrainCarOutcome.CHECKS_TIMEOUT,
                TrainCarOutcome.CHECKS_FAILED,
            )
            and self.last_merge_conditions_evaluation
            and raw_unsuccessful_checks
        ):
            related_checks = self.last_merge_conditions_evaluation.get_related_checks()

            unsuccessful_checks = [
                check.serialized()
                for check in raw_unsuccessful_checks
                if check.name in related_checks
            ]

            if not unsuccessful_checks:
                self.train.log.error(
                    "unsuccessful_checks is unexpectedly empty",
                    last_checks=self.last_checks.copy(),
                    raw_unsuccessful_checks=raw_unsuccessful_checks,
                    related_checks=related_checks,
                    train_car_outcome=self.train_car_state.outcome,
                    last_evaluated_merge_conditions=self.last_merge_conditions_evaluation,
                )
        else:
            unsuccessful_checks = []

        metadata = signals.EventQueueChecksEndMetadata(
            {
                "aborted": aborted,
                "abort_code": abort_code,
                "abort_reason": abort_reason_str,
                "abort_status": abort_status,
                "unqueue_code": None,
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
                    "unsuccessful_checks": unsuccessful_checks,
                },
            }
        )

        if not (checks_end_metadata := ep.checks_end_metadata) or (
            checks_end_metadata
            and (
                (
                    checks_end_metadata["abort_status"] == "REEMBARKED"
                    and abort_status == "DEFINITIVE"
                )
                or (
                    checks_end_metadata["abort_status"] == "REEMBARKED"
                    and abort_status == "REEMBARKED"
                    and checks_end_metadata["abort_reason"] != abort_reason_str
                )
            )
        ):
            await signals.send(
                self.repository,
                ep.user_pull_request_number,
                self.ref,
                "action.queue.checks_end",
                metadata,
                "merge queue internal",
            )
        ep.associate_queue_checks_end_metadata(metadata)

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

        # Circular import
        from mergify_engine.queue.merge_train.train_car_state import (
            TrainCarStateForSummary,
        )

        train_car_state_summary = TrainCarStateForSummary.from_train_car_state(
            self.train_car_state
        ).serialized()

        # Append a `>` after a double newlines because otherwise
        # the quote breaks.
        details_as_markdown = details.replace("\n\n", "\n\n>")
        summary += f"""\nDetails:
> {details_as_markdown}

{train_car_state_summary}
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
                await original_ctxt.get_merge_queue_check_run_name(),
                check_api.Result(
                    check_api.Conclusion.ACTION_REQUIRED,
                    title=title,
                    summary=summary,
                ),
                details_url=await dashboard.get_queues_url_from_context(
                    original_ctxt, self.train.convoy
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

    def _get_merge_conditions_with_ignored_attributes(
        self,
        evaluated_queue_rule: qr_config.EvaluatedQueueRule,
        non_ignored_attributes: str | tuple[str, ...],
    ) -> conditions.QueueRuleMergeConditions:
        conditions_with_ignored_attributes = (
            evaluated_queue_rule.merge_conditions.copy()
        )
        for (
            condition_with_ignored_attributes
        ) in conditions_with_ignored_attributes.walk():
            attr = condition_with_ignored_attributes.get_attribute_name()
            if not attr.startswith(non_ignored_attributes):
                condition_with_ignored_attributes.make_always_true()

        return conditions_with_ignored_attributes

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

    async def get_unexpected_changes(
        self,
        queue_rule: qr_config.QueueRule,
    ) -> UnexpectedChanges | None:
        if self.queue_pull_request_number is None:
            raise RuntimeError(
                "get_unexpected_changes expected the train car to be started"
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

        if (
            self.train_car_state.checks_type == TrainCarChecksType.DRAFT
            and not queue_rule.config["allow_queue_branch_edit"]
            and await self._have_unexpected_draft_pull_request_changes()
        ):
            return UnexpectedDraftPullRequestChange(self.queue_pull_request_number)
        if (
            self.train_car_state.checks_type == TrainCarChecksType.INPLACE
            and await checked_ctxt.synchronized_by_user_at() is not None
        ):
            return UnexpectedUpdatedPullRequestChange(checked_ctxt.pull["number"])

        if (
            self.train_car_state.checks_type == TrainCarChecksType.INPLACE
            and checked_ctxt.pull["state"] == "closed"
        ):
            return None

        # NOTE(Syffe): If we enter this clause, it means there are no unexpected changes detected yet on the PR.
        # Thus, checking for UnexpectedBaseBranchChange on a closed pull request makes no sense, since
        # we are sure, if the PR is closed, that it is closed by us.
        train_synced_with_base_branch = await self.train.is_synced_with_the_base_branch(
            checked_ctxt,
            current_base_sha,
        )
        if train_synced_with_base_branch:
            return None

        fallback_partition_name = (
            self.train.convoy.partition_rules.get_fallback_partition_name()
        )
        if fallback_partition_name is None:
            # There is no fallback partition, and we don't know the change, we reset every partition
            return UnexpectedBaseBranchChange(current_base_sha)

        # NOTE(Syffe): In case a context with a fallback partition, if the train is out of sync with the base
        # branch we have to define wether the change comes from a Pull Request, or a force push directly on
        # the base branch.
        # * If the change comes from a PR, we need to evaluate it against each partition
        # rules, then there are two cases:
        #   1. The PR matches one or more partition --> We reset the concerned partitions
        #   2. The PR doesn't match any partition --> We reset the fallback partition only
        # * If the change doesn't come from a PR, it is a force push, we reset every partition

        # NOTE(Syffe): In order to define wether the change comes from a PR or not, we need to check if a PR
        # exists in the repository. To do this, we use the current_base_sha that is out of sync with the base
        # branch as a parameter.
        # * If the endpoint is empty, there is no existing PR, then we reset every partition
        #   see endpoint documentation:
        #   https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-pull-requests-associated-with-a-commit
        # * If the endpoint returns a result, we check that the pull request at the origin of
        #   the change is inside, and we then evaluate it against the current train partition rules in order to
        #   define if it needs to be reset

        is_api_call_result_empty = True
        async for pull in typing.cast(
            abc.AsyncIterator[github_types.GitHubPullRequest],
            self.repository.installation.client.items(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/commits/{current_base_sha}/pulls",
                resource_name="pulls",
                page_limit=20,
            ),
        ):
            is_api_call_result_empty = False
            if pull["merge_commit_sha"] == current_base_sha:
                pull_context = await self.repository.get_pull_request_context(
                    pull["number"]
                )
                return await self.train.get_unexpected_base_branch_change_after_manually_merged_pr_with_fallback_partition(
                    pull_context,
                    current_base_sha,
                    fallback_partition_name,
                )

        if is_api_call_result_empty:
            # In a context with a fallback partition, the change doesn't come from a PR, we reset every
            # partition
            return UnexpectedBaseBranchChange(current_base_sha)
        return None

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

        checked_ctxt = await self.repository.get_pull_request_context(
            self.queue_pull_request_number
        )
        queue_rule = self.get_queue_rule()
        unexpected_changes = await self.get_unexpected_changes(queue_rule)
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
        saved_last_merge_conditions_evaluation = self.last_merge_conditions_evaluation
        saved_outcome = self.train_car_state.outcome

        check = await checked_ctxt.get_merge_queue_check_run()
        saved_queue_check_run_conclusion = check["conclusion"] if check else None
        saved_freeze_data = self.train_car_state.frozen_by
        saved_pause_data = self.train_car_state.paused_by

        if (
            self.train_car_state.checks_type
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
            await checked_ctxt.is_head_sha_outdated()
            or await self.get_unexpected_changes(queue_rule) is not None
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
                evaluated_queue_rule.merge_conditions,
                wait_for_schedule_to_match=True,
            )

        await self.update_state(
            status,
            evaluated_queue_rule,
            unexpected_changes=unexpected_changes,
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
            saved_last_merge_conditions_evaluation,
            self.last_merge_conditions_evaluation,
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
            or saved_pause_data != self.train_car_state.paused_by
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

    def get_queue_rule(self) -> qr_config.QueueRule:
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
        evaluated_queue_rule: qr_config.EvaluatedQueueRule,
        unexpected_changes: UnexpectedChanges | None = None,
    ) -> None:
        self.last_merge_conditions_evaluation = (
            evaluated_queue_rule.merge_conditions.get_evaluation_result()
        )
        self.last_queue_conditions_evaluation = (
            evaluated_queue_rule.queue_conditions.get_evaluation_result(
                display_evaluations=True
            )
        )

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
            conditions_with_only_checks = (
                self._get_merge_conditions_with_ignored_attributes(
                    evaluated_queue_rule,
                    ("check-", "status-"),
                )
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

        outside_schedule = False
        has_failed_check_other_than_schedule = False

        # Register schedule period
        if queue_conditions_conclusion in (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.FAILURE,
        ):
            self.train_car_state.add_waiting_for_schedule_end_date()
        elif queue_conditions_conclusion == check_api.Conclusion.PENDING:
            for condition in evaluated_queue_rule.merge_conditions.walk(
                yield_only_failing_conditions=True
            ):
                if condition.get_attribute_name() == "schedule":
                    outside_schedule = True
                else:
                    has_failed_check_other_than_schedule = True

            if outside_schedule and not has_failed_check_other_than_schedule:
                self.train_car_state.add_waiting_for_schedule_start_date()
            else:
                self.train_car_state.add_waiting_for_schedule_end_date()

            queue_pull_requests = await self.get_pull_requests_to_evaluate()
            conditions_with_only_schedule = (
                self._get_merge_conditions_with_ignored_attributes(
                    evaluated_queue_rule, "schedule"
                )
            )
            await conditions_with_only_schedule(queue_pull_requests)

            for _ in conditions_with_only_schedule.walk(
                yield_only_failing_conditions=True
            ):
                self.train_car_state.add_time_spent_outside_schedule_start_date()
                break
            else:
                self.train_car_state.add_time_spent_outside_schedule_end_date()

        else:
            raise RuntimeError(
                f"Unhandled queue_conditions_conclusion: {queue_conditions_conclusion}"
            )

        # Register freeze period
        queue_freeze = await self.train.convoy.is_queue_frozen(
            self.still_queued_embarked_pulls[0].config["name"],
        )
        if not queue_freeze:
            self.train_car_state.add_waiting_for_freeze_end_date()
        elif (
            queue_freeze
            and self.train_car_state.ci_state == merge_train_types.CiState.SUCCESS
        ):
            # Add start date only if the reason this isnt getting merged is because of the freeze
            self.train_car_state.add_waiting_for_freeze_start_date()

        self.train_car_state.paused_by = await pause.QueuePause.get(self.repository)

        if (
            self.train_car_state.outcome == TrainCarOutcome.MERGEABLE
            and queue_conditions_conclusion != check_api.Conclusion.SUCCESS
        ):
            # NOTE(Kontrolix): If previous outcome was MERGEABLE but now
            # queue_conditions_conclusion is no longer SUCCESS, it needs to be reseted
            # to UNKNOWN. e.g. a PR is evaluated during a queue freeze as MERGEABLE
            # then the freeze is remove outside a schedule
            self.train_car_state.outcome = TrainCarOutcome.UNKNOWN

        if self.train_car_state.paused_by is not None:
            self.train_car_state.outcome = (
                TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE
            )
        # Update Outcome
        elif unexpected_changes is not None:
            # Unexpected change always override any outcome
            self.train_car_state.outcome = UNEXPECTED_CHANGES_COMPATIBILITY[
                type(unexpected_changes)
            ]
            self.train_car_state.outcome_message = str(unexpected_changes)
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
                self.train_car_state.outcome_message = CI_FAILED_MESSAGE
                if self._has_reached_batch_max_failure():
                    self.train_car_state.outcome = (
                        TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
                    )
                else:
                    self.train_car_state.outcome = TrainCarOutcome.CHECKS_FAILED

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

        if self.train_car_state.outcome in (
            TrainCarOutcome.UNKNOWN,
            TrainCarOutcome.MERGEABLE,
        ):
            # Everything look good for now, just wait.
            if (
                self.train_car_state.outcome == TrainCarOutcome.MERGEABLE
                and self.queue_pull_request_number is not None
            ):
                for ep in self.still_queued_embarked_pulls:
                    await self.send_checks_end_signal(
                        user_pull_request_number=ep.user_pull_request_number,
                        cancel_reason=None,
                        abort_status="DEFINITIVE",
                    )
            return

        if (
            self.train_car_state.outcome
            == TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE
        ):
            # We wait for _populate_cars() to reset the whole train
            return

        if (
            len(self.initial_embarked_pulls) != 1
            and self.train_car_state.outcome
            != TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
        ):
            # NOTE(sileht): We wait for _split_failed_batches() to find the culprit PR
            return

        if not self.has_previous_car_status_succeeded():
            # NOTE(sileht): We wait for the cars ahead in queue to success first in case of the failure
            # is due to them
            return

        # NOTE(sileht): This car failed and PRs inside are the culprit, report the failure and remove this car.
        await self.train.remove_failed_car(self)

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
            # NOTE(sileht): copy only post_check from our App, we don't care about Summary/Rule/Queue checks
            if check["app_id"] == settings.GITHUB_APP_ID and not check["name"].endswith(
                "(post_check)"
            ):
                continue

            output_title = ""
            if check["output"] and check["output"]["title"]:
                output_title = f" — {check['output']['title']}"

            self.last_checks.append(
                merge_train_checks.QueueCheck(
                    name=check["name"],
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

    def get_check_runs_summary(
        self, checked_pull: github_types.GitHubPullRequestNumber | None
    ) -> str | None:
        if not self.last_checks:
            return None

        if checked_pull is None:
            checked_pull = github_types.GitHubPullRequestNumber(0)

        check_runs_summary = (
            "Check-runs and statuses of the embarked "
            f"pull request #{checked_pull}:\n\n<table>"
        )
        for qcheck in merge_train_checks.get_check_list_ordered(self.last_checks):
            qcheck_icon_url = CHECK_ASSERTS.get(qcheck.state, CHECK_ASSERTS["neutral"])

            check_runs_summary += (
                "<tr>"
                f'<td align="center" width="48" height="48"><img src="{qcheck_icon_url}" width="16" height="16" /></td>'
                f'<td align="center" width="48" height="48"><img src="{qcheck.avatar_url}&s=40" width="16" height="16" /></td>'
                f"<td><b>{qcheck.name}</b>{qcheck.description}</td>"
                f'<td><a href="{qcheck.url}">details</a></td>'
                "</tr>"
            )
        check_runs_summary += "</table>\n"
        return check_runs_summary

    def get_batch_failure_summary(self) -> BatchFailureSummary | None:
        if not self.failure_history:
            return None

        spec_checks = []
        for failure in self.failure_history:
            if failure.train_car_state.checks_type == TrainCarChecksType.INPLACE:
                spec_checks.append("[in place]")
            elif failure.train_car_state.checks_type == TrainCarChecksType.DRAFT:
                if failure.queue_pull_request_number is None:
                    raise RuntimeError(
                        "car's spec checks type is draft, but queue_pull_request_number is None"
                    )
                spec_checks.append(
                    train_utils.build_pr_link(
                        self.repository, failure.queue_pull_request_number
                    )
                )
            else:
                spec_checks.append("")

        return BatchFailureSummary(
            title=f"❌ The pull request {self._get_user_refs()} is part of a speculative checks batch that previously failed ❌",
            pull_request=self._get_user_refs(),
            parent_pull_requests=self._get_embarked_refs(
                include_my_self=False, markdown=True
            ),
            spec_checks=spec_checks,
        )

    def get_checks_timeout_summary(self) -> str | None:
        queue_rule = self.get_queue_rule()
        timeout = queue_rule.config["checks_timeout"]

        if self.train_car_state.outcome == TrainCarOutcome.CHECKS_TIMEOUT:
            return "❌⏲️️  The checks have timed out ⏲❌"

        if (
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
            return f"⏲️  The checks have to pass before {expected_finish_pretty} ⏲"

        return None

    def get_train_car_state_for_summary(self) -> str:
        # Circular import
        from mergify_engine.queue.merge_train.train_car_state import (
            TrainCarStateForSummary,
        )

        return TrainCarStateForSummary.from_train_car_state(
            self.train_car_state
        ).serialized()

    def get_merge_conditions_summary(self) -> str:
        return (
            "Required conditions for merge:\n\n" + self.last_evaluated_merge_conditions
        )

    def get_unexpected_changes_summary(self) -> str | None:
        if not self.has_unexpected_changes:
            return None
        return f"❌ Unexpected queue change: {self.train_car_state.outcome_message}. ❌"

    def get_freeze_summary(self) -> str | None:
        if self.train_car_state.frozen_by is None:
            return None

        return self.train_car_state.frozen_by.get_freeze_message()

    def get_pause_summary(self) -> str | None:
        if self.train_car_state.paused_by is None:
            return None

        return self.train_car_state.paused_by.get_pause_message()

    def get_original_pr_summary_title(self) -> str:
        if self.train_car_state.outcome == TrainCarOutcome.DRAFT_PR_CHANGE:
            return "The pull request has been removed from the queue"

        if self.train_car_state.outcome in (
            TrainCarOutcome.BASE_BRANCH_CHANGE,
            TrainCarOutcome.UPDATED_PR_CHANGE,
        ):
            return "The pull request is going to be re-embarked soon"

        if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            return f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} is mergeable"

        if self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            return f"The pull request is embarked with {self._get_embarked_refs(include_my_self=False)} for merge"

        if self.train_car_state.outcome in (
            TrainCarOutcome.CHECKS_FAILED,
            TrainCarOutcome.CHECKS_TIMEOUT,
            TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS,
            TrainCarOutcome.CONFLICT_WITH_BASE_BRANCH,
            TrainCarOutcome.CONFLICT_WITH_PULL_AHEAD,
            TrainCarOutcome.BRANCH_UPDATE_FAILED,
        ):
            return f"The pull request embarked with {self._get_embarked_refs(include_my_self=False)} cannot be merged and has been disembarked"

        if (
            self.train_car_state.outcome
            == TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE
        ):
            return f"The pull request is embarked with {self._get_embarked_refs(include_my_self=False)} for merge, but the merge queue is paused"

        raise RuntimeError(f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}")

    def get_original_pr_summary(
        self,
        checked_pull: github_types.GitHubPullRequestNumber | None,
    ) -> QueueSummary:
        return QueueSummary(
            title=self.get_original_pr_summary_title(),
            train_car_state=self.get_train_car_state_for_summary(),
            merge_conditions=self.get_merge_conditions_summary(),
            unexpected_changes=self.get_unexpected_changes_summary(),
            freeze=self.get_freeze_summary(),
            pause=self.get_pause_summary(),
            checks_timeout=self.get_checks_timeout_summary(),
            check_runs=self.get_check_runs_summary(checked_pull),
            batch_failure=self.get_batch_failure_summary(),
        )

    def build_original_pr_summary_for_partition_report(
        self, checked_pull: github_types.GitHubPullRequestNumber | None
    ) -> str:
        original_pr_summary = self.get_original_pr_summary(checked_pull)
        return (
            original_pr_summary.train_car_state
            + f"\n\n**Partition {self.train.partition_name}**: {original_pr_summary.title}\n"
            + original_pr_summary.body
        )

    async def get_draft_pr_summary(self) -> str:
        headline: str | None = None
        show_queue = False
        if self.train_car_state.outcome == TrainCarOutcome.MERGEABLE:
            if len(self.initial_embarked_pulls) == 1:
                headline = "🎉 This pull request has been checked successfully and will be merged. 🎉"
            else:
                headline = "🎉 This combination of pull requests has been checked successfully and will be merged. 🎉"
        elif self.train_car_state.outcome == TrainCarOutcome.CHECKS_FAILED:
            if len(self.initial_embarked_pulls) == 1:
                headline = f"❌ This pull request has failed checks. {self._get_user_refs()} will be removed from the queue. ❌"
            elif self.has_previous_car_status_succeeded():
                headline = "🕵️ This combination of pull requests has failed checks. Mergify will split this batch to understand which pull request is responsible for the failure. 🕵️"
            else:
                headline = "🕵️ This combination of pull requests has failed checks. Mergify is waiting for other pull requests ahead in the queue to understand which one is responsible for the failure. 🕵️"
        elif self.train_car_state.outcome == TrainCarOutcome.CHECKS_TIMEOUT:
            if len(self.initial_embarked_pulls) == 1:
                headline = (
                    "❌ This pull request has timed out. "
                    f"{self._get_user_refs()} will be removed from the queue. ❌"
                )
            else:
                headline = (
                    "❌ This combination of pull requests has timed out. "
                    f"{self._get_user_refs()} will be removed from the queue. ❌"
                )

        elif self.has_unexpected_changes:
            show_queue = True
            if len(self.initial_embarked_pulls) == 1:
                headline = f"✨ Unexpected queue change: {self.train_car_state.outcome_message}. Due to an unexpected change in this draft PR, the pull request {self._get_user_refs()} cannot be merged and will be re-embarked soon. ✨"
            else:
                headline = f"✨ Unexpected queue change: {self.train_car_state.outcome_message}. Due to an unexpected change in this draft PR, the pull requests {self._get_user_refs()} cannot be merged and will be re-embarked soon. ✨"

        elif (
            self.train_car_state.outcome
            == TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
        ):
            if len(self.initial_embarked_pulls) == 1:
                headline = (
                    "❌ The pull request has failed checks and the maximum resolution attempts has been reached. "
                    f"{self._get_user_refs()} will be removed from the queue. ❌"
                )
            else:
                headline = (
                    "❌ This combination of pull requests has failed checks and the maximum resolution attempts has been reached. "
                    f"{self._get_user_refs()} will be removed from the queue. ❌"
                )
        elif self.train_car_state.outcome == TrainCarOutcome.UNKNOWN:
            if len(self.initial_embarked_pulls) == 1:
                headline = f"⏳ The pull request {self._get_user_refs()} is embarked for merge and currently being checked. ⏳"
            else:
                headline = f"⏳ The pull requests {self._get_user_refs()} are embarked for merge and currently being checked. ⏳"
            show_queue = True
        elif (
            self.train_car_state.outcome
            == TrainCarOutcome.PR_CHECKS_STOPPED_BECAUSE_MERGE_QUEUE_PAUSE
        ):
            headline = "⏸️ The checks have been interrupted because the merge queue is paused on the repository ⏸️"
        else:
            raise RuntimeError(
                f"Unhandled TrainCarOutcome: {self.train_car_state.outcome}"
            )

        return await self.build_draft_pr_summary(
            for_queue_pull_request=True,
            headline=headline,
            show_queue=show_queue,
        )

    def get_checked_pull(self) -> github_types.GitHubPullRequestNumber | None:
        if self.train_car_state.checks_type in (
            TrainCarChecksType.DRAFT,
            TrainCarChecksType.DRAFT_DELEGATED,
            TrainCarChecksType.INPLACE,
        ):
            if self.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's spec checks type is {self.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            return self.queue_pull_request_number
        return None

    async def update_summaries(self) -> None:
        checked_pull = self.get_checked_pull()

        # "Queue:" summary generation of the original PR
        queue_summary = self.get_original_pr_summary(checked_pull)

        self.train.log.info(
            "pull request train car status update",
            legacy_conclusion=self.get_queue_check_run_conclusion(),
            outcome=self.train_car_state.outcome,
            outcome_message=self.train_car_state.outcome_message,
            original_pull_title=queue_summary.title,
            gh_pull=checked_pull,
            merge_conditions_summary=queue_summary.merge_conditions.strip(),
            last_evaluated_queue_conditions_summary=self.last_evaluated_queue_conditions.strip(),
            batch_failure_summary=queue_summary.build_batch_failure_summary().strip(),
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

        # Draft PR summary and state update
        if (
            self.train_car_state.checks_type == TrainCarChecksType.DRAFT
            and checked_pull is not None
        ):
            tmp_pull_ctxt = await self.repository.get_pull_request_context(checked_pull)
            draft_summary = await self.get_draft_pr_summary()
            payload = {}
            if tmp_pull_ctxt.body != draft_summary:
                payload["body"] = draft_summary

            if (
                self.get_queue_check_run_conclusion() != check_api.Conclusion.PENDING
                and not tmp_pull_ctxt.closed
            ):
                payload["state"] = "closed"

            if payload:
                await tmp_pull_ctxt.client.patch(
                    f"{tmp_pull_ctxt.base_url}/pulls/{checked_pull}",
                    json=payload,
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

    def _get_previous_car(self) -> TrainCar | None:
        position = self.train._cars.index(self)
        if position == 0:
            return None
        return self.train._cars[position - 1]

    def has_previous_car_status_succeeded(self) -> bool:
        position = self.train._cars.index(self)
        if position == 0:
            return True
        return all(
            c.train_car_state.outcome == TrainCarOutcome.MERGEABLE
            for c in self.train._cars[:position]
        )
