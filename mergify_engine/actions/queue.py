# FIXME(sileht): we need to revist action.config and executor.config a lot of
# branches are not checked by mypy because it think the code is unreachable
# MRGFY-1994
# mypy: disable-error-code=unreachable
from __future__ import annotations

import dataclasses
import datetime
import functools
import typing

import first
import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import dashboard
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.actions import RawConfigT
from mergify_engine.actions import merge_base
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.queue import freeze
from mergify_engine.queue import merge_train
from mergify_engine.queue import pause
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import checks_status
from mergify_engine.rules import conditions
from mergify_engine.rules import types
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine import context
    from mergify_engine.queue.merge_train import types as merge_train_types

BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT = (
    "Require branches to be up to date before merging"
)


@dataclasses.dataclass
class InvalidQueueConfiguration(Exception):
    title: str
    message: str


@dataclasses.dataclass
class MergeBotAccountNotFoundWithBranchProtectionOff(InvalidQueueConfiguration):
    title: str = "There are no `merge_bot_account` being used"
    message: str = "Cannot use `branch_protection_injection_mode` set to `none` without using a `merge_bot_account` with the `queue` action"


@dataclasses.dataclass
class MismatchingQueueConditions(InvalidQueueConfiguration):
    summary: str | None = dataclasses.field(default=None)
    title: str = "There are no queue conditions matching"
    message: str = (
        "There are queue conditions defined in the configuration, but none matches. "
        "The pull request has not been embarked."
    )

    def __post_init__(self) -> None:
        self.message += f"\n\nDetails:\n{self.summary}"


@dataclasses.dataclass
class IncompatibleBranchProtection(InvalidQueueConfiguration):
    """Incompatibility between Mergify configuration and branch protection settings"""

    configuration: str
    branch_protection_setting: str
    required_additional_configuration: str | None = dataclasses.field(default=None)

    title: str = dataclasses.field(
        init=False,
        default="Configuration not compatible with a branch protection setting",
    )
    message: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.message = (
            "The branch protection setting "
            f"`{self.branch_protection_setting}` is not compatible with `{self.configuration}`"
        )

        if self.required_additional_configuration is None:
            self.message += " and must be unset."
        else:
            self.message += f" if `{self.required_additional_configuration}` isn't set."


class QueueCancellationQueryResult(typing.NamedTuple):
    should_be_cancelled: bool
    summary: str


QueueUpdateT = typing.Literal["merge", "rebase"]


class QueueExecutorConfig(typing.TypedDict):
    name: qr_config.QueueName
    merge_method: merge_base.MergeMethodT | None
    merge_bot_account: github_types.GitHubLogin | None
    update_bot_account: github_types.GitHubLogin | None
    update_method: QueueUpdateT
    commit_message_template: str | None
    allow_merging_configuration_change: bool
    autosquash: bool


@dataclasses.dataclass
class QueueExecutor(
    actions.ActionExecutor["QueueAction", "QueueExecutorConfig"],
    merge_base.MergeUtilsMixin,
):
    queue_rule: qr_config.QueueRule = dataclasses.field(init=False, repr=False)

    DEQUEUE_DOCUMENTATION = f"""
You can take a look at `{constants.MERGE_QUEUE_SUMMARY_NAME}` check runs for more details.

In case of a failure due to a flaky test, you should first retrigger the CI.
Then, re-embark the pull request into the merge queue by posting the comment
`@mergifyio refresh` on the pull request.
"""

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @classmethod
    async def create(
        cls,
        action: QueueAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> QueueExecutor:
        if (
            action.config["name"] is not None
            and ctxt.repository.mergify_config["queue_rules"].get(action.config["name"])
            is None
        ):
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                f"`{action.config['name']}` queue not found",
                "",
            )

        return cls(
            ctxt,
            rule,
            QueueExecutorConfig(
                {
                    "name": action.config["name"],
                    "merge_method": action.config["merge_method"],
                    "update_method": action.config["update_method"],
                    "commit_message_template": action.config["commit_message_template"],
                    "merge_bot_account": action.config["merge_bot_account"],
                    "update_bot_account": action.config["update_bot_account"],
                    "allow_merging_configuration_change": action.config[
                        "allow_merging_configuration_change"
                    ],
                    "autosquash": action.config["autosquash"],
                },
            ),
        )

    @property
    def queue_rules(self) -> qr_config.QueueRules:
        return self.ctxt.repository.mergify_config["queue_rules"]

    @property
    def partition_rules(self) -> partr_config.PartitionRules:
        return self.ctxt.repository.mergify_config["partition_rules"]

    async def _queue_branch_merge_pull_request(
        self,
        convoy: merge_train.Convoy,
        queue_freeze: freeze.QueueFreeze | None,
        queue_pause: pause.QueuePause | None,
        # "cars" is not used but needed because this func is used in 'merge' property
        # with another func that need this attribute.
        cars: list[merge_train.TrainCar],
    ) -> check_api.Result:
        if not cars:
            raise RuntimeError("Ready to merge PR without any car...")

        result = await self.common_merge(
            "queue",
            self.ctxt,
            self.config["merge_method"],
            self.config["merge_bot_account"],
            self.config["commit_message_template"],
            functools.partial(
                self.get_pending_queue_status,
                convoy=convoy,
                rule=self.rule,
                queue_rule=self.queue_rule,
                queue_freeze=queue_freeze,
                queue_pause=queue_pause,
            ),
            self.queue_rule.branch_protection_injection_mode,
        )
        if result.conclusion == check_api.Conclusion.SUCCESS:
            embarked_pulls = await convoy.find_embarked_pull(self.ctxt.pull["number"])
            if embarked_pulls is None:
                raise RuntimeError("Queue pull request with no embarked pulls")
            if self.ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")

            await convoy.remove_pull(
                self.ctxt.pull["number"],
                self.rule.get_signal_trigger(),
                queue_utils.PrMerged(
                    self.ctxt.pull["number"],
                    self.ctxt.pull["merge_commit_sha"],
                ),
            )

            partition_names = [ep.partition_name for ep in embarked_pulls]
            await self.send_merge_signal(
                self.ctxt.pull["number"],
                embarked_pulls[0].embarked_pull.queued_at,
                partition_names,
            )

        return result

    async def _queue_branch_merge_fastforward(
        self,
        convoy: merge_train.Convoy,
        queue_freeze: freeze.QueueFreeze | None,
        queue_pause: pause.QueuePause | None,
        cars: list[merge_train.TrainCar],
    ) -> check_api.Result:
        if not cars:
            raise RuntimeError("Ready to merge PR without any car...")

        if (
            len(cars) > 1
            or cars[0].train.partition_name != partr_config.DEFAULT_PARTITION_NAME
        ):
            raise RuntimeError(
                "Shouldn't be in queue_branch_merge_fastforward with partition rules in use",
            )

        if (
            self.config["merge_method"] is not None
            and self.config["merge_method"] != "merge"
        ):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                f"Cannot use merge_method={self.config['merge_method']} with queue_branch_merge_method=fast-forward",
                "Only `merge_method=merge` is supported with `queue_branch_merge_method=fast-forward`",
            )

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                self.ctxt.repository,
                "queue",
                self.config["merge_bot_account"],
                # NOTE(sileht): we don't allow admin, because if branch protection are
                # enabled, but not enforced on admins, we may bypass them
                required_permissions=[github_types.GitHubRepositoryPermission.WRITE],
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        car = cars[0]
        if car.train_car_state.checks_type == merge_train.TrainCarChecksType.INPLACE:
            newsha = self.ctxt.pull["head"]["sha"]
        elif car.train_car_state.checks_type in (
            merge_train.TrainCarChecksType.DRAFT,
            merge_train.TrainCarChecksType.DRAFT_DELEGATED,
        ):
            if car.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None",
                )
            tmp_ctxt = await convoy.repository.get_pull_request_context(
                car.queue_pull_request_number,
            )
            newsha = tmp_ctxt.pull["head"]["sha"]
        else:
            raise RuntimeError(
                f"Invalid car checks type: {car.train_car_state.checks_type}",
            )

        try:
            await self.ctxt.client.put(
                f"{self.ctxt.base_url}/git/refs/heads/{self.ctxt.pull['base']['ref']}",
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                json={"sha": newsha},
            )
        except http.HTTPUnauthorized:
            if on_behalf is None:
                raise
            return action_utils.get_invalid_credentials_report(on_behalf)
        except http.HTTPClientSideError as e:  # pragma: no cover
            return await self._handle_merge_error(
                e,
                self.ctxt,
                functools.partial(
                    self.get_pending_queue_status,
                    convoy=convoy,
                    rule=self.rule,
                    queue_rule=self.queue_rule,
                    queue_freeze=queue_freeze,
                    queue_pause=queue_pause,
                ),
            )

        for embarked_pull in cars[0].still_queued_embarked_pulls.copy():
            await self.create_recently_merged_tracker(
                self.ctxt.repository.installation.redis.cache,
                self.ctxt.repository.repo["id"],
                embarked_pull.user_pull_request_number,
            )
            await convoy.remove_pull(
                embarked_pull.user_pull_request_number,
                self.rule.get_signal_trigger(),
                queue_utils.PrMerged(embarked_pull.user_pull_request_number, newsha),
            )
            await self.send_merge_signal(
                embarked_pull.user_pull_request_number,
                embarked_pull.queued_at,
                [car.train.partition_name],
            )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "The pull request has been merged automatically",
            f"The pull request has been merged automatically at *{newsha}*",
        )

    @property
    def _merge(
        self,
    ) -> abc.Callable[
        [
            merge_train.Convoy,
            freeze.QueueFreeze | None,
            pause.QueuePause | None,
            list[merge_train.TrainCar],
        ],
        abc.Coroutine[typing.Any, typing.Any, check_api.Result],
    ]:
        queue_branch_merge_method = self.queue_rule.config["queue_branch_merge_method"]
        if queue_branch_merge_method is None:
            return self._queue_branch_merge_pull_request
        if queue_branch_merge_method == "fast-forward":
            return self._queue_branch_merge_fastforward
        raise RuntimeError(
            f"Unsupported queue_branch_merge_method: {queue_branch_merge_method}",
        )

    def _set_action_config_from_queue_rules(self) -> None:
        queue_rule_config_attributes_none_default: list[
            typing.Literal[
                "commit_message_template",
                "merge_bot_account",
                "update_bot_account",
                "update_method",
            ]
        ] = [
            "commit_message_template",
            "merge_bot_account",
            "update_bot_account",
            "update_method",
        ]

        for attr in queue_rule_config_attributes_none_default:
            if self.config[attr] is None:
                self.config[attr] = self.queue_rule.config[attr]

        # name in action and queue_rule are not the same so we need to treat it
        # in a different way
        if self.config["merge_method"] is None:
            self.config["merge_method"] = self.queue_rule.config["merge_method"]

        if self.config["update_method"] is None:
            self.config["update_method"] = (
                "rebase" if self.config["merge_method"] == "fast-forward" else "merge"
            )

    async def _set_action_queue_rule(self) -> None:
        mismatching_queue_conditions: list[qr_config.EvaluatedQueueRule] = []
        matching_queue_conditions: list[qr_config.EvaluatedQueueRule] = []
        if await self.queue_rules.queue_conditions_exists():
            for rule in await self.queue_rules.get_evaluated_queue_conditions(
                self.ctxt,
            ):
                (
                    matching_queue_conditions
                    if rule.queue_conditions.match
                    else mismatching_queue_conditions
                ).append(rule)

        if mismatching_queue_conditions and not matching_queue_conditions:
            summary = {
                q.name: q.queue_conditions.get_summary(display_evaluations=False)
                for q in mismatching_queue_conditions
            }

            self.ctxt.log.debug(
                "no queue conditions matching",
                summary=summary,
            )

            queue_conditions_summary_str = "\n".join(
                f"* Queue `{name}`: \n{queue_conditions_summary}"
                for name, queue_conditions_summary in summary.items()
            )
            raise MismatchingQueueConditions(summary=queue_conditions_summary_str)

        if matching_queue_conditions and self.config["name"] is None:
            self.config["name"] = matching_queue_conditions[0].name

        # default queue name
        if self.config["name"] is None:
            self.config["name"] = qr_config.QueueName("default")

        try:
            self.queue_rule = self.queue_rules[qr_config.QueueName(self.config["name"])]
        except KeyError:
            raise InvalidQueueConfiguration(
                "Invalid queue name",
                f"The queue `{self.config['name']}` does not exist",
            )

        if (
            self.queue_rule.config["queue_branch_merge_method"] == "fast-forward"
            and len(self.partition_rules) > 0
        ):
            raise InvalidQueueConfiguration(
                "Invalid merge method with partition rules in use",
                "Cannot use `fast-forward` merge method when using partition rules",
            )

        self._set_action_config_from_queue_rules()

    async def _can_be_reembarked_automatically(self) -> bool:
        # FIXME(sileht): relies on delete_reason and outcome instead of guessing
        check = await self.ctxt.get_merge_queue_check_run()
        if check is None or check_api.Conclusion(check["conclusion"]) in (
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.PENDING,
            check_api.Conclusion.NEUTRAL,
        ):
            return False

        reason = await action_utils.get_dequeue_reason(self.ctxt, False)
        if isinstance(reason, queue_utils.PrDequeued):
            # FIXME(sileht): we need dedicated BaseDequeueReason for all these special cases
            if reason.is_dequeue_command():
                return False

            # Automation match again, we can reembark
            return actions.CANCELLED_CHECK_REPORT.title in reason.details

        # NOTE(sileht): we allow auto reembark only for this configuration
        if not (
            self.queue_rule.config["speculative_checks"] == 1
            and self.queue_rule.config["batch_size"] == 1
            and self.queue_rule.config["allow_inplace_checks"]
        ):
            return False

        if not isinstance(
            reason,
            queue_utils.ChecksFailed
            | queue_utils.ChecksTimeout
            | queue_utils.QueueRuleMissing
            | queue_utils.DraftPullRequestChanged
            | queue_utils.PullRequestUpdated,
        ):
            return False

        # NOTE(sileht): All rules must match again (pull request_rules, queue_rules/queue_conditions and merge_conditions)
        # The two first are already checked because run() got call, we just need to check merge_conditions
        queue_rule_for_evaluator = await self.queue_rule.get_queue_rule_for_evaluator(
            self.ctxt.repository,
            self.ctxt.pull["base"]["ref"],
        )
        status = await checks_status.get_rule_checks_status(
            self.ctxt.log,
            self.ctxt.repository,
            [condition_value_querier.PullRequest(self.ctxt)],
            queue_rule_for_evaluator.merge_conditions,
            wait_for_schedule_to_match=False,
        )
        # Allow reembark if CIs states is not failing anymore
        return status != check_api.Conclusion.FAILURE

    async def run(self) -> check_api.Result:
        if self.ctxt.user_refresh_requested() or self.ctxt.admin_refresh_requested():
            await self.reembark_pull_request_if_possible()

        convoy = await merge_train.Convoy.from_context(self.ctxt)
        partition_names = (
            await self.partition_rules.get_evaluated_partition_names_from_context(
                self.ctxt,
            )
        )

        trains_and_car = convoy.get_trains_and_car_from_context_and_partition_names(
            self.ctxt,
            partition_names,
        )
        cars = [tac.train_car for tac in trains_and_car]

        result = await self.pre_queue_checks(convoy, cars)

        if result is not None:
            return result

        if await self._can_be_reembarked_automatically():
            await self.reembark_pull_request_if_possible()

        for car in cars[:]:
            try:
                await car.check_mergeability(
                    "original_pull_request",
                    original_pull_request_rule=self.rule,
                    original_pull_request_number=self.ctxt.pull["number"],
                )
            except merge_train.MergeQueueReset:
                cars.remove(car)

        dequeue_msg = (
            f"The pull request has been removed from the queue `{self.config['name']}`"
        )

        if str(self.config["name"]) == "None":
            self.ctxt.log.error(
                "queue name is unexpectedly `None`",
                is_string=isinstance(self.config["name"], str),
                logged_from="actions.queue.run",
            )

        # NOTE(sileht): the pull request gets checked and then changed
        # by user, we should dequeue and requeue it as the conditions still match.
        if await self.ctxt.synchronized_by_user_at():
            dequeue_reason_pr_updated = queue_utils.PullRequestUpdated(
                self.ctxt.pull["number"],
            )
            self.ctxt.log.info(
                "pull request has unexpected pull request update",
                synchronize_sources=[
                    source
                    for source in self.ctxt.sources
                    if source["event_type"] == "pull_request"
                    and typing.cast(
                        github_types.GitHubEventPullRequest,
                        source["data"],
                    )["action"]
                    == "synchronize"
                ],
            )

            await convoy.remove_pull(
                self.ctxt.pull["number"],
                self.rule.get_signal_trigger(),
                dequeue_reason_pr_updated,
            )
            if isinstance(self.rule, prr_config.CommandRule):
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    dequeue_msg,
                    f"{dequeue_reason_pr_updated!s}.\n{self.DEQUEUE_DOCUMENTATION}",
                )

        dequeue_reason: queue_utils.BaseDequeueReason

        if not await self._should_be_queued(self.ctxt):
            dequeue_reason = await action_utils.get_dequeue_reason(
                self.ctxt,
            )
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                dequeue_msg,
                f"{dequeue_reason!s}.\n{self.DEQUEUE_DOCUMENTATION}",
            )
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)
            return result

        pull_queue_config = queue.PullQueueConfig(
            {
                "update_method": self.config["update_method"],
                "effective_priority": await self.queue_rule.get_context_effective_priority(
                    self.ctxt,
                ),
                "bot_account": self.config["merge_bot_account"],
                "update_bot_account": self.config["update_bot_account"],
                "priority": await self.queue_rule.priority_rules.get_context_priority(
                    self.ctxt,
                ),
                "name": self.config["name"],
                "autosquash": self.config["autosquash"],
            },
        )

        await convoy.add_pull(
            self.ctxt,
            pull_queue_config,
            partition_names,
            self.rule.get_signal_trigger(),
        )

        try:
            queue_freeze = await convoy.get_current_queue_freeze(self.config["name"])
            queue_pause = await pause.QueuePause.get(self.ctxt.repository)
            if await self._should_be_merged(
                self.ctxt,
                convoy,
                queue_freeze,
                queue_pause,
                partition_names,
            ):
                result = await self._merge(convoy, queue_freeze, queue_pause, cars)
            else:
                result = await self.get_pending_queue_status(
                    self.ctxt,
                    convoy,
                    self.rule,
                    self.queue_rule,
                    queue_freeze,
                    queue_pause,
                )
        except Exception as e:
            if not exceptions.need_retry_in(e):
                await convoy.remove_pull(
                    self.ctxt.pull["number"],
                    self.rule.get_signal_trigger(),
                    queue_utils.PrUnexpectedlyFailedToMerge(),
                )
            raise

        if result.conclusion is not check_api.Conclusion.PENDING:
            dequeue_reason = await self.get_dequeue_reason_from_action_result(result)
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)
        else:
            # Reset MERGE_QUEUE_SUMMARY_NAME check_run to neutral only if it exists already
            merge_queue_check_run = await self.ctxt.get_merge_queue_check_run()
            if (
                merge_queue_check_run is not None
                and merge_queue_check_run["status"] != "in_progress"
            ):
                await check_api.set_check_run(
                    self.ctxt,
                    merge_queue_check_run["name"],
                    check_api.Result(
                        check_api.Conclusion.NEUTRAL,
                        f"The pull request #{self.ctxt.pull['number']} is queued",
                        (
                            f"The pull request #{self.ctxt.pull['number']} is queued,"
                            " this summary will be updated once the queue"
                            " checks have started."
                        ),
                        details_url=await dashboard.get_queues_url_from_context(
                            self.ctxt,
                            convoy,
                        ),
                    ),
                )

        result.details_url = await dashboard.get_queues_url_from_context(
            self.ctxt,
            convoy,
        )

        return result

    async def _dequeue_pull_request(
        self,
        convoy: merge_train.Convoy,
        cars: list[merge_train.TrainCar],
        dequeue_reason: queue_utils.BaseDequeueReason,
        result: check_api.Result,
    ) -> None:
        pull_request_rule_has_unmatched = (
            result.conclusion == check_api.Conclusion.CANCELLED
            and result.title == actions.CANCELLED_CHECK_REPORT.title
        )
        # NOTE(sileht): The PR has been checked successfully but the
        # final merge fail, we must erase the queue summary conclusion,
        # so the requeue can work.
        merge_has_been_cancelled = (
            cars
            and all(
                car.train_car_state.outcome == merge_train.TrainCarOutcome.MERGEABLE
                for car in cars
            )
            and result.conclusion is check_api.Conclusion.CANCELLED
        )
        if pull_request_rule_has_unmatched or merge_has_been_cancelled:
            if pull_request_rule_has_unmatched:
                # NOTE(sileht): we allow the action to rerun later
                conclusion = check_api.Conclusion.NEUTRAL
            elif merge_has_been_cancelled:
                # NOTE(sileht): we disallow the action to rerun later without requeue/refresh command
                conclusion = check_api.Conclusion.FAILURE
            else:
                raise RuntimeError("Impossible if branch")

            await check_api.set_check_run(
                self.ctxt,
                await self.ctxt.get_merge_queue_check_run_name(),
                check_api.Result(
                    conclusion,
                    f"The pull request {self.ctxt.pull['number']} cannot be merged and has been disembarked",
                    result.title + "\n" + result.summary,
                    details_url=await dashboard.get_queues_url_from_context(
                        self.ctxt,
                        convoy,
                    ),
                ),
            )

        await convoy.remove_pull(
            self.ctxt.pull["number"],
            self.rule.get_signal_trigger(),
            dequeue_reason,
        )

    async def cancel(self) -> check_api.Result:
        if self.ctxt.user_refresh_requested() or self.ctxt.admin_refresh_requested():
            await self.reembark_pull_request_if_possible()

        convoy = await merge_train.Convoy.from_context(self.ctxt)

        partition_names = (
            await self.partition_rules.get_evaluated_partition_names_from_context(
                self.ctxt,
            )
        )

        trains_and_car = convoy.get_trains_and_car_from_context_and_partition_names(
            self.ctxt,
            partition_names,
        )
        cars = [tac.train_car for tac in trains_and_car]

        result = await self.pre_queue_checks(convoy, cars)

        if result is not None:
            return result

        for car in cars:
            try:
                await car.check_mergeability(
                    "original_pull_request",
                    original_pull_request_rule=self.rule,
                    original_pull_request_number=self.ctxt.pull["number"],
                )
            except merge_train.MergeQueueReset:
                cars.remove(car)

        # We just rebase the pull request, don't cancel it yet if CIs are
        # running. The pull request will be merged if all rules match again.
        # if not we will delete it when we received all CIs termination
        cancel_query_result = await self._should_be_cancelled(
            self.ctxt,
            self.rule,
            trains_and_car,
        )
        if cancel_query_result.should_be_cancelled:
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                actions.CANCELLED_CHECK_REPORT.title,
                cancel_query_result.summary,
            )
            dequeue_reason = await self.get_dequeue_reason_from_action_result(result)
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)
            return result

        if not await self._should_be_queued(self.ctxt):
            dequeue_reason = await action_utils.get_dequeue_reason(
                self.ctxt,
            )
            dequeue_msg = f"The pull request has been removed from the queue `{self.config['name']}`"

            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                dequeue_msg,
                f"{dequeue_reason!s}.\n{self.DEQUEUE_DOCUMENTATION}",
            )
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)
            return result

        queue_freeze = await convoy.get_current_queue_freeze(self.config["name"])
        queue_pause = await pause.QueuePause.get(convoy.repository)
        result = await self.get_pending_queue_status(
            self.ctxt,
            convoy,
            self.rule,
            self.queue_rule,
            queue_freeze,
            queue_pause,
        )

        if result.conclusion is not check_api.Conclusion.PENDING:
            dequeue_reason = await self.get_dequeue_reason_from_action_result(result)
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)
        return result

    async def _render_bot_accounts(self) -> None:
        try:
            self.config["update_bot_account"] = await action_utils.render_bot_account(
                self.ctxt,
                self.config["update_bot_account"],
                bot_account_fallback=None,
                option_name="update_bot_account",
            )
            self.config["merge_bot_account"] = await action_utils.render_bot_account(
                self.ctxt,
                self.config["merge_bot_account"],
                bot_account_fallback=None,
                option_name="merge_bot_account",
            )
        except action_utils.RenderBotAccountFailure as e:
            raise InvalidQueueConfiguration(e.title, e.reason)

        try:
            await action_utils.get_github_user_from_bot_account(
                self.ctxt.repository,
                "update queued pull request",
                self.config["update_bot_account"],
                required_permissions=(
                    github_types.GitHubRepositoryPermission.permissions_above(
                        github_types.GitHubRepositoryPermission.WRITE,
                    )
                ),
            )
            await action_utils.get_github_user_from_bot_account(
                self.ctxt.repository,
                "merge queued pull request",
                self.config["merge_bot_account"],
                required_permissions=(
                    github_types.GitHubRepositoryPermission.permissions_above(
                        github_types.GitHubRepositoryPermission.WRITE,
                    )
                ),
                branch_protection_injection_mode=self.queue_rule.branch_protection_injection_mode,
            )
        except action_utils.BotAccountNotFound as e:
            raise InvalidQueueConfiguration(e.title, e.reason)

    async def _check_action_validity(
        self,
        cars: list[merge_train.TrainCar],
    ) -> check_api.Result | None:
        try:
            await self._set_action_queue_rule()
            await self._check_branch_protection_injection_mode_compatibility(
                branch_protection_injection_mode=self.queue_rule.branch_protection_injection_mode,
                queue_executor_config=self.config,
            )
            await self._check_config_compatibility_with_branch_protection(
                ctxt=self.ctxt,
                queue_rule_config=self.queue_rule.config,
                queue_executor_config=self.config,
            )
            self._check_method_fastforward_configuration(
                config=self.config,
                queue_rule=self.queue_rule,
            )
            await self._render_bot_accounts()
        except InvalidQueueConfiguration as e:
            if isinstance(e, MismatchingQueueConditions) and (
                still_embarked_pull := first.first(
                    still_embarked_pull
                    for car in cars
                    for still_embarked_pull in car.still_queued_embarked_pulls
                    if self.ctxt.pull["number"]
                    == still_embarked_pull.user_pull_request_number
                )
            ):
                # NOTE(lecrepont01): we don't want the rule to fail yet if PR was already embarked
                # this could be caused by queue conditions checks removed after a rebase
                self.queue_rule = self.queue_rules[
                    qr_config.QueueName(still_embarked_pull.config["name"])
                ]
                self.config["name"] = still_embarked_pull.config["name"]
                return None

            return check_api.Result(
                check_api.Conclusion.CANCELLED,
                e.title,
                e.message,
            )

        try:
            await self._check_subscription_status(
                config=self.config,
                queue_rule=self.queue_rule,
                queue_rules=self.queue_rules,
                partition_rules=self.partition_rules,
                ctxt=self.ctxt,
            )
        except InvalidQueueConfiguration as e:
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                e.title,
                e.message,
            )

        return None

    async def pre_queue_checks(
        self,
        convoy: merge_train.Convoy,
        cars: list[merge_train.TrainCar],
    ) -> check_api.Result | None:
        result = await self._check_action_validity(cars)

        if result is None:
            result = await self.pre_merge_checks(
                self.ctxt,
                self.config["merge_method"],
                self.config["merge_bot_account"],
            )

        # NOTE(sileht): The PR have maybe been merged fast-forward by
        # Mergify, but GitHub didn't yet update the "mergedXXX" attribute.
        # So we prefer don't do anything for 30 seconds to wait
        # pre_merge_checks to mark the pull request as merged as expected
        if (
            result is None
            and (
                self.queue_rule.config["queue_branch_merge_method"] == "fast-forward"
                or self.config["merge_method"] == "fast-forward"
            )
            and await self.has_been_recently_merged(
                self.ctxt.redis.cache,
                self.ctxt.repository.repo["id"],
                self.ctxt.pull["number"],
            )
        ):
            await delayed_refresh.plan_refresh_at_least_at(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                at=date.utcnow() + datetime.timedelta(seconds=30),
            )
            return check_api.Result(
                check_api.Conclusion.PENDING,
                "The pull request is going to be merged soon",
                summary="The pull request is going to be merged soon",
            )

        if result is not None and result.conclusion is not check_api.Conclusion.PENDING:
            dequeue_reason = await self.get_dequeue_reason_from_action_result(
                result,
            )
            await self._dequeue_pull_request(convoy, cars, dequeue_reason, result)

        return result

    async def get_dequeue_reason_from_action_result(
        self,
        result: check_api.Result,
    ) -> queue_utils.BaseDequeueReason:
        if result.conclusion is check_api.Conclusion.PENDING:
            raise RuntimeError(
                "get_dequeue_reason_from_action_result() on PENDING result",
            )
        if result.conclusion is check_api.Conclusion.SUCCESS:
            if self.ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")
            return queue_utils.PrMerged(
                self.ctxt.pull["number"],
                self.ctxt.pull["merge_commit_sha"],
            )
        details = f". {result.title}"
        if result.summary:
            details += f". {result.summary}"
        return queue_utils.PrDequeued(self.ctxt.pull["number"], details=details)

    @classmethod
    async def _should_be_queued(
        cls,
        ctxt: context.Context,
    ) -> bool:
        # FIXME(sileht): relies on delete_reason and outcome instead of guessing
        check = await ctxt.get_merge_queue_check_run()
        return not check or check_api.Conclusion(check["conclusion"]) in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.PENDING,
            check_api.Conclusion.NEUTRAL,
        ]

    @staticmethod
    async def _should_be_merged(
        ctxt: context.Context,
        convoy: merge_train.Convoy,
        queue_freeze: freeze.QueueFreeze | None,
        queue_pause: pause.QueuePause | None,
        partition_names: list[partr_config.PartitionRuleName],
    ) -> bool:
        if queue_freeze is not None:
            return False

        if queue_pause is not None:
            return False

        for train in convoy.iter_trains_from_partition_names(partition_names):
            if not await train.is_first_pull(ctxt):
                return False

            car = train.get_car(ctxt)
            if car is None:
                return False

            if car.train_car_state.outcome != merge_train.TrainCarOutcome.MERGEABLE:
                return False

        return True

    @staticmethod
    async def _should_be_cancelled(
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
        previous_cars: list[merge_train_types.TrainAndTrainCar],
    ) -> QueueCancellationQueryResult:
        # It's closed, it's not going to change
        if ctxt.closed:
            return QueueCancellationQueryResult(True, "The pull request is closed.")

        if await ctxt.synchronized_by_user_at() is not None:
            return QueueCancellationQueryResult(
                True,
                "The pull request has been synchronized by a user.",
            )

        should_be_cancelled = True

        for train, prev_car in previous_cars:
            car = train.get_car(ctxt)
            if car is None:
                # NOTE(sileht): train car has been deleted, wait for the queue status to be copied
                # to the action status
                return QueueCancellationQueryResult(
                    False,
                    "Wait for queue status to be copied.",
                )

            if (
                car
                and car == prev_car
                and car.train_car_state.checks_type
                == merge_train.TrainCarChecksType.INPLACE
            ):
                # NOTE(sileht) check first if PR should be removed from the queue
                pull_rule_checks_status = await checks_status.get_rule_checks_status(
                    ctxt.log,
                    ctxt.repository,
                    [condition_value_querier.PullRequest(ctxt)],
                    rule.conditions,
                )
                # NOTE(sileht): if the pull request rules are pending we wait their
                # match before checking queue rules states, in case of one
                # condition suddently dequeue the pull request.
                # TODO(sileht): we may want to make this behavior configurable as
                # people having slow/long CI running only on pull request rules, we
                # may want to merge it before it finishes.
                should_be_cancelled = pull_rule_checks_status not in (
                    check_api.Conclusion.PENDING,
                    check_api.Conclusion.SUCCESS,
                )

        if not rule.conditions.match:
            summary = f"""The following conditions don't match anymore:
{rule.conditions.get_unmatched_summary()}"""
        else:
            summary = actions.CANCELLED_CHECK_REPORT.summary

        return QueueCancellationQueryResult(should_be_cancelled, summary)

    @staticmethod
    async def get_pending_queue_status(
        ctxt: context.Context,
        convoy: merge_train.Convoy,
        rule: prr_config.EvaluatedPullRequestRule,
        queue_rule: qr_config.QueueRule,
        queue_freeze: freeze.QueueFreeze | None,
        queue_pause: pause.QueuePause | None,
    ) -> check_api.Result:
        if queue_pause is not None:
            title = queue_pause.get_pause_message()
        elif queue_freeze is not None:
            title = queue_freeze.get_freeze_message()
        else:
            embarked_pulls = await convoy.find_embarked_pull(ctxt.pull["number"])
            if not embarked_pulls:
                ctxt.log.error("expected queued pull request not found in queue")
                title = "The pull request is queued to be merged"
            elif len(embarked_pulls) == 1:
                _ord = utils.to_ordinal_numeric(embarked_pulls[0].position + 1)
                title = f"The pull request is the {_ord} in the "

                if (
                    embarked_pulls[0].partition_name
                    == partr_config.DEFAULT_PARTITION_NAME
                ):
                    title += "queue"
                else:
                    title += f"`{embarked_pulls[0].partition_name}` partition queue"

                title += " to be merged"
            else:
                positions = [
                    f"{utils.to_ordinal_numeric(ep.position + 1)} in {ep.partition_name}"
                    for ep in embarked_pulls
                ]
                title = (
                    "The pull request is queued in the following partitions to be merged: "
                    f"{', '.join(positions)}"
                )

        summary = await convoy.get_pull_summary(
            ctxt,
            queue_rule=queue_rule,
            pull_rule=rule,
        )

        return check_api.Result(check_api.Conclusion.PENDING, title, summary)

    async def send_merge_signal(
        self,
        pull_request: github_types.GitHubPullRequestNumber,
        embarked_pull_queued_at: datetime.datetime,
        partition_names: list[partr_config.PartitionRuleName],
    ) -> None:
        await signals.send(
            self.ctxt.repository,
            pull_request,
            self.ctxt.pull["base"]["ref"],
            "action.queue.merged",
            signals.EventQueueMergedMetadata(
                {
                    "branch": self.ctxt.pull["base"]["ref"],
                    "partition_names": partition_names,
                    "queue_name": self.config["name"],
                    "queued_at": embarked_pull_queued_at,
                },
            ),
            self.rule.get_signal_trigger(),
        )

    def _check_method_fastforward_configuration(
        self,
        config: QueueExecutorConfig,
        queue_rule: qr_config.QueueRule,
    ) -> None:
        if config["merge_method"] != "fast-forward":
            return

        if config["update_method"] != "rebase":
            raise InvalidQueueConfiguration(
                f"`update_method: {config['update_method']}` is not compatible with fast-forward merge method",
                "`update_method` must be set to `rebase`.",
            )

        if config["commit_message_template"] is not None:
            raise InvalidQueueConfiguration(
                "Commit message can't be changed with fast-forward merge method",
                "`commit_message_template` must not be set if `merge_method: fast-forward` is set.",
            )
        if queue_rule.config["batch_size"] > 1:
            raise InvalidQueueConfiguration(
                "batch_size > 1 is not compatible with fast-forward merge method",
                "The `merge_method` or the queue configuration must be updated.",
            )
        if queue_rule.config["speculative_checks"] > 1:
            raise InvalidQueueConfiguration(
                "speculative_checks > 1 is not compatible with fast-forward merge method",
                "The `merge_method` or the queue configuration must be updated.",
            )
        if not queue_rule.config["allow_inplace_checks"]:
            raise InvalidQueueConfiguration(
                "allow_inplace_checks=False is not compatible with fast-forward merge method",
                "The `merge_method` or the queue configuration must be updated.",
            )

        if (
            self.ctxt.pull["user"]["type"] == "Bot"
            and config["update_bot_account"] is None
        ):
            raise InvalidQueueConfiguration(
                "Pull requests created by GitHub App can't be impersonated for rebasing.",
                "A `update_bot_account` must be set to be able to rebase and merge them with fast-forward merge method.",
            )

    @staticmethod
    async def _check_subscription_status(
        config: QueueExecutorConfig,
        queue_rule: qr_config.QueueRule,
        queue_rules: qr_config.QueueRules,
        partition_rules: partr_config.PartitionRules,
        ctxt: context.Context,
    ) -> None:
        if not ctxt.subscription.has_feature(subscription.Features.MERGE_QUEUE):
            raise InvalidQueueConfiguration(
                "Cannot use the merge queue.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

        if len(queue_rules) > 1 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION,
        ):
            raise InvalidQueueConfiguration(
                "Cannot use multiple queues.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

        if queue_rule.config[
            "speculative_checks"
        ] > 1 and not ctxt.subscription.has_feature(subscription.Features.QUEUE_ACTION):
            raise InvalidQueueConfiguration(
                "Cannot use `speculative_checks` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

        if queue_rule.config["batch_size"] > 1 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION,
        ):
            raise InvalidQueueConfiguration(
                "Cannot use `batch_size` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

        if len(partition_rules) > 0 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION,
        ):
            raise InvalidQueueConfiguration(
                "Cannot use `partition_rules` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

    @staticmethod
    async def _check_branch_protection_injection_mode_compatibility(
        branch_protection_injection_mode: queue.BranchProtectionInjectionModeT,
        queue_executor_config: QueueExecutorConfig,
    ) -> None:
        if (
            branch_protection_injection_mode == "none"
            and queue_executor_config["merge_bot_account"] is None
        ):
            raise MergeBotAccountNotFoundWithBranchProtectionOff

    @staticmethod
    async def _check_config_compatibility_with_branch_protection(
        ctxt: context.Context,
        queue_rule_config: qr_config.QueueConfig,
        queue_executor_config: QueueExecutorConfig,
    ) -> None:
        if queue_rule_config["queue_branch_merge_method"] == "fast-forward":
            # Note(charly): `queue_branch_merge_method=fast-forward` make the
            # use of batches compatible with branch protection
            # `required_status_checks=strict`
            return

        protection = await ctxt.repository.get_branch_protection(
            ctxt.pull["base"]["ref"],
        )

        _has_required_status_checks_strict = (
            protection is not None
            and "required_status_checks" in protection
            and protection["required_status_checks"].get("strict", False)
        )

        if _has_required_status_checks_strict:
            if queue_rule_config["batch_size"] > 1:
                raise IncompatibleBranchProtection(
                    "batch_size>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            if queue_rule_config["speculative_checks"] > 1:
                raise IncompatibleBranchProtection(
                    "speculative_checks>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            if queue_rule_config["allow_inplace_checks"] is False:
                raise IncompatibleBranchProtection(
                    "allow_inplace_checks=false",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )

            if (
                ctxt.pull["user"]["type"] == "Bot"
                and await ctxt.is_behind
                and queue_executor_config["update_method"] == "rebase"
                and not queue_executor_config["update_bot_account"]
            ):
                raise IncompatibleBranchProtection(
                    "update_method=rebase",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                    "update_bot_account",
                )

    async def reembark_pull_request_if_possible(self) -> None:
        # NOTE(sileht): user ask a refresh, we just remove the previous state of this
        # check and the method _should_be_queued will become true again :)
        # FIXME(sileht): relies on delete_reason and outcome instead of guessing
        check = await self.ctxt.get_merge_queue_check_run()
        if check and check_api.Conclusion(check["conclusion"]) not in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.PENDING,
            check_api.Conclusion.NEUTRAL,
        ]:
            await check_api.set_check_run(
                self.ctxt,
                check["name"],
                check_api.Result(
                    check_api.Conclusion.PENDING,
                    "The pull request has been refreshed and is going to be re-embarked soon",
                    "",
                    details_url=await dashboard.get_queues_url_from_context(self.ctxt),
                ),
            )


class QueueAction(actions.Action):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.SUCCESS_IS_FINAL_STATE
        | actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
        | actions.ActionFlag.SAME_COMMAND_WITH_DIFFERENT_ARGS_CANCEL_PENDING_STATUS
        # FIXME(sileht): MRGFY-562
        # | actions.ActionFlag.ALWAYS_RUN
    )

    merge_method_exclusive_msg = "Cannot have both `method` and `merge_method` options in `queue` action, use `merge_method` only (`method` is deprecated)"
    merge_method_value_validator = voluptuous.Any(
        None,  # fallback to queue_rule
        "rebase",
        "merge",
        "squash",
        "fast-forward",
    )

    @property
    def validator(self) -> actions.ValidatorT:
        config = {
            voluptuous.Required("name", default=None): voluptuous.Any(
                str,
                None,
            ),
            voluptuous.Required(
                "merge_method",
                default=None,
            ): self.merge_method_value_validator,
            "method": self.merge_method_value_validator,  # for retro-compatibility
            voluptuous.Required(
                "merge_bot_account",
                default=None,
            ): types.Jinja2WithNone,
            voluptuous.Required(
                "update_bot_account",
                default=None,
            ): types.Jinja2WithNone,
            voluptuous.Required("update_method", default=None): voluptuous.Any(
                "rebase",
                "merge",
                None,
            ),
            voluptuous.Required(
                "commit_message_template",
                default=None,
            ): types.Jinja2WithNone,
            voluptuous.Required(
                "allow_merging_configuration_change",
                default=False,
            ): bool,
            voluptuous.Required("autosquash", default=True): bool,
        }

        return typing.cast(
            actions.ValidatorT,
            voluptuous.All(
                qr_config._has_only_one_of(
                    "method",
                    "merge_method",
                    msg=self.merge_method_exclusive_msg,
                ),
                config,
            ),
        )

    required_feature_for_command = subscription.Features.MERGE_QUEUE

    def __post_init__(self, raw_config_: RawConfigT | None) -> None:
        super().__post_init__(raw_config_)
        # NOTE(lecrepont01): `method` is deprecated but still accepted see validator
        if "method" in self.config:
            self.config["merge_method"] = self.config.pop("method")

    @staticmethod
    def command_to_config(command_arguments: str) -> dict[str, typing.Any]:
        name: str | None = None
        config = {"name": name}
        args = [
            arg_stripped
            for arg in command_arguments.split(" ")
            if (arg_stripped := arg.strip())
        ]
        if args and args[0]:
            config["name"] = args[0]
        return config

    async def get_conditions_requirements(
        self,
        ctxt: context.Context,
    ) -> list[conditions.RuleConditionNode]:
        conditions_requirements = []

        conditions_requirements.append(
            conditions.get_mergify_configuration_change_conditions(
                "queue",
                self.config["allow_merging_configuration_change"],
            ),
        )
        conditions_requirements.extend(await conditions.get_depends_on_conditions(ctxt))

        merge_after_condition = conditions.get_merge_after_condition(ctxt)
        if merge_after_condition is not None:
            conditions_requirements.append(merge_after_condition)

        if queue_conditions := await conditions.get_queue_conditions(
            ctxt,
            self.config["name"],
        ):
            conditions_requirements.append(queue_conditions)

        conditions_requirements.append(
            conditions.RuleCondition.from_tree(
                {"=": ("draft", False)},
                description=":pushpin: queue requirement",
            ),
        )
        return conditions_requirements

    executor_class = QueueExecutor
