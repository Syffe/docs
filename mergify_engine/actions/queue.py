from __future__ import annotations

from collections import abc
import dataclasses
import functools
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.actions import merge_base
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.queue import freeze
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import checks_status
from mergify_engine.rules import conditions
from mergify_engine.rules import types
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT = (
    "Require branches to be up to date before merging"
)


DEPRECATED_MESSAGE_REBASE_FALLBACK_QUEUE_ACTION = """The configuration uses the deprecated `rebase_fallback` attribute of the queue action.
A brownout is planned on February 13th, 2023.
This option will be removed on March 13th, 2023.
For more information: https://docs.mergify.com/actions/queue/

`%s` is invalid"""

DEPRECATED_MESSAGE_PRIORITY_ATTRIBUTE_QUEUE_ACTION = """The configuration uses the deprecated `priority` attribute of the queue action and must be replaced by `priority_rules`.
A brownout is planned on April 3rd, 2023.
This option will be removed on May 9th, 2023.
For more information: https://docs.mergify.com/actions/queue/#priority-rules

`%s` is invalid"""


@dataclasses.dataclass
class InvalidQueueConfiguration(Exception):
    title: str
    message: str


@dataclasses.dataclass
class IncompatibleBranchProtection(InvalidQueueConfiguration):
    """Incompatibility between Mergify configuration and branch protection settings"""

    configuration: str
    branch_protection_setting: str

    title: str = dataclasses.field(
        init=False,
        default="Configuration not compatible with a branch protection setting",
    )
    message: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.message = (
            "The branch protection setting "
            f"`{self.branch_protection_setting}` is not compatible with `{self.configuration}` "
            "and must be unset."
        )


QueueUpdateT = typing.Literal["merge", "rebase"]


class QueueExecutorConfig(typing.TypedDict):
    name: qr_config.QueueName
    method: merge_base.MergeMethodT
    merge_bot_account: github_types.GitHubLogin | None
    update_bot_account: github_types.GitHubLogin | None
    update_method: QueueUpdateT
    commit_message_template: str | None
    priority: int
    require_branch_protection: bool
    allow_merging_configuration_change: bool

    # deprecated
    rebase_fallback: merge_base.RebaseFallbackT


@dataclasses.dataclass
class QueueExecutor(
    actions.ActionExecutor["QueueAction", "QueueExecutorConfig"],
    merge_base.MergeUtilsMixin,
):
    config_hidden_from_simulator = ("priority",)
    queue_rule: qr_config.QueueRule
    queue_rules: qr_config.QueueRules

    UNQUEUE_DOCUMENTATION = f"""
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
        action: "QueueAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "QueueExecutor":
        return cls(
            ctxt,
            rule,
            QueueExecutorConfig(
                {
                    "name": action.config["name"],
                    "method": action.config["method"],
                    "update_method": action.config["update_method"],
                    "rebase_fallback": action.config["rebase_fallback"],
                    "commit_message_template": action.config["commit_message_template"],
                    "merge_bot_account": action.config["merge_bot_account"],
                    "update_bot_account": action.config["update_bot_account"],
                    "priority": action.config["priority"],
                    "require_branch_protection": action.config[
                        "require_branch_protection"
                    ],
                    "allow_merging_configuration_change": action.config[
                        "allow_merging_configuration_change"
                    ],
                }
            ),
            action.queue_rule,
            action.queue_rules,
        )

    async def _queue_branch_merge_pull_request(
        self,
        queue: merge_train.Train,
        queue_freeze: freeze.QueueFreeze | None,
        # "car" is not used but needed because this func is used in 'merge' property
        # with other func that need this attribute.
        car: merge_train.TrainCar | None,
    ) -> check_api.Result:
        result = await self.common_merge(
            self.ctxt,
            self.rule,
            self.config["method"],
            self.config["rebase_fallback"],
            self.config["merge_bot_account"],
            self.config["commit_message_template"],
            functools.partial(
                self.get_pending_queue_status,
                queue=queue,
                queue_rule=self.queue_rule,
                queue_freeze=queue_freeze,
            ),
        )
        if result.conclusion == check_api.Conclusion.SUCCESS:
            _, embarked_pull = queue.find_embarked_pull(self.ctxt.pull["number"])
            if embarked_pull is None:
                raise RuntimeError("Queue pull request with no embarked_pull")
            if self.ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")
            await queue.remove_pull(
                self.ctxt,
                self.rule.get_signal_trigger(),
                queue_utils.PrMerged(
                    self.ctxt.pull["number"], self.ctxt.pull["merge_commit_sha"]
                ),
            )
            await self.send_merge_signal(embarked_pull)
        return result

    async def _queue_branch_merge_fastforward(
        self,
        queue: merge_train.Train,
        queue_freeze: freeze.QueueFreeze | None,
        car: merge_train.TrainCar | None,
    ) -> check_api.Result:
        if car is None:
            raise RuntimeError("ready to merge PR without car....")

        if self.config["method"] != "merge":
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                f"Cannot use method={self.config['method']} with queue_branch_merge_method=fast-forward",
                "Only `method=merge` is supported with `queue_branch_merge_method=fast-forward`",
            )

        # FIXME(sileht): move this to create() to report the error early
        github_user: user_tokens.UserTokensUser | None = None
        if self.config["merge_bot_account"]:
            tokens = await self.ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(self.config["merge_bot_account"])
            if not github_user:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    f"Unable to rebase: user `{self.config['merge_bot_account']}` is unknown. ",
                    f"Please make sure `{self.config['merge_bot_account']}` has logged in Mergify dashboard.",
                )

        if car.train_car_state.checks_type == merge_train.TrainCarChecksType.INPLACE:
            newsha = self.ctxt.pull["head"]["sha"]
        elif car.train_car_state.checks_type == merge_train.TrainCarChecksType.DRAFT:
            if car.queue_pull_request_number is None:
                raise RuntimeError(
                    f"car's checks type is {car.train_car_state.checks_type}, but queue_pull_request_number is None"
                )
            tmp_ctxt = await queue.repository.get_pull_request_context(
                car.queue_pull_request_number
            )
            newsha = tmp_ctxt.pull["head"]["sha"]
        else:
            raise RuntimeError(
                f"Invalid car checks type: {car.train_car_state.checks_type}"
            )

        try:
            await self.ctxt.client.put(
                f"{self.ctxt.base_url}/git/refs/heads/{self.ctxt.pull['base']['ref']}",
                oauth_token=github_user["oauth_access_token"] if github_user else None,
                json={"sha": newsha},
            )
        except http.HTTPClientSideError as e:  # pragma: no cover
            return await self._handle_merge_error(
                e,
                self.ctxt,
                self.rule,
                functools.partial(
                    self.get_pending_queue_status,
                    queue=queue,
                    queue_rule=self.queue_rule,
                    queue_freeze=queue_freeze,
                ),
            )

        for embarked_pull in car.still_queued_embarked_pulls.copy():
            other_ctxt = await queue.repository.get_pull_request_context(
                embarked_pull.user_pull_request_number
            )
            await queue.remove_pull(
                other_ctxt,
                self.rule.get_signal_trigger(),
                queue_utils.PrMerged(self.ctxt.pull["number"], newsha),
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
            merge_train.Train,
            freeze.QueueFreeze | None,
            merge_train.TrainCar | None,
        ],
        abc.Coroutine[typing.Any, typing.Any, check_api.Result],
    ]:
        queue_branch_merge_method = self.queue_rule.config["queue_branch_merge_method"]
        if queue_branch_merge_method is None:
            return self._queue_branch_merge_pull_request
        elif queue_branch_merge_method == "fast-forward":
            return self._queue_branch_merge_fastforward
        else:
            raise RuntimeError(
                f"Unsupported queue_branch_merge_method: {queue_branch_merge_method}"
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
        if self.config["method"] is None:
            self.config["method"] = self.queue_rule.config["merge_method"]

        if self.config["update_method"] is None:
            self.config["update_method"] = (
                "rebase" if self.config["method"] == "fast-forward" else "merge"
            )

    async def _set_action_queue_rule(self) -> None:
        if (
            await self.queue_rules.routing_conditions_exists()
            and not await self.queue_rules.are_routing_conditions_matching(self.ctxt)
        ):
            raise InvalidQueueConfiguration(
                "There are no queue routing conditions matching",
                "There are routing conditions defined in the configuration, but none matches; the pull request has not been embarked",
            )

        if (
            self.config["name"] is None
            and await self.queue_rules.routing_conditions_exists()
        ):
            evaluated_routing_conditions = (
                await self.queue_rules.get_matching_evaluated_routing_conditions(
                    self.ctxt
                )
            )
            if evaluated_routing_conditions:
                self.config["name"] = evaluated_routing_conditions[0].name

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

        self._set_action_config_from_queue_rules()

    async def run(self) -> check_api.Result:
        if self.ctxt.user_refresh_requested() or self.ctxt.admin_refresh_requested():
            # NOTE(sileht): user ask a refresh, we just remove the previous state of this
            # check and the method _should_be_queued will become true again :)
            check = await self.ctxt.get_engine_check_run(
                constants.MERGE_QUEUE_SUMMARY_NAME
            )
            if check and check_api.Conclusion(check["conclusion"]) not in [
                check_api.Conclusion.SUCCESS,
                check_api.Conclusion.PENDING,
                check_api.Conclusion.NEUTRAL,
            ]:
                self.ctxt.log.info(
                    "a refresh marks the pull request as re-embarkable",
                    check=check,
                    user_refresh=self.ctxt.user_refresh_requested(),
                    admin_refresh=self.ctxt.admin_refresh_requested(),
                )
                await check_api.set_check_run(
                    self.ctxt,
                    constants.MERGE_QUEUE_SUMMARY_NAME,
                    check_api.Result(
                        check_api.Conclusion.PENDING,
                        "The pull request has been refreshed and is going to be re-embarked soon",
                        "",
                    ),
                )

        q = await merge_train.Train.from_context(self.ctxt, self.queue_rules)
        car = q.get_car(self.ctxt)

        result = await self.pre_queue_checks(
            q,
            car,
        )

        if result is not None:
            return result

        if car is not None:
            await car.check_mergeability(
                "original_pull_request",
                original_pull_request_rule=self.rule,
                original_pull_request_number=self.ctxt.pull["number"],
            )

        # NOTE(sileht): the pull request gets checked and then changed
        # by user, we should unqueue and requeue it as the conditions still match.
        if await self.ctxt.synchronized_by_user_at():
            unexpected_change = queue_utils.UnexpectedQueueChange(
                str(
                    merge_train.UnexpectedUpdatedPullRequestChange(
                        self.ctxt.pull["number"]
                    )
                )
            )
            self.ctxt.log.info(
                "pull request has unexpected pull request update",
                synchronize_sources=[
                    source
                    for source in self.ctxt.sources
                    if source["event_type"] == "pull_request"
                    and typing.cast(
                        github_types.GitHubEventPullRequest, source["data"]
                    )["action"]
                    == "synchronize"
                ],
            )
            await q.remove_pull(
                self.ctxt,
                self.rule.get_signal_trigger(),
                unexpected_change,
            )
            if isinstance(self.rule, prr_config.CommandRule):
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    "The pull request has been removed from the queue",
                    f"{unexpected_change!s}.\n{self.UNQUEUE_DOCUMENTATION}",
                )

        if not await self._should_be_queued(self.ctxt):
            unqueue_reason = await self.get_unqueue_reason_from_outcome(self.ctxt)
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                "The pull request has been removed from the queue",
                f"{unqueue_reason!s}.\n{self.UNQUEUE_DOCUMENTATION}",
            )
            await self._unqueue_pull_request(q, car, unqueue_reason, result)
            return result

        pull_queue_config = queue.PullQueueConfig(
            {
                "update_method": self.config["update_method"],
                "effective_priority": await self.queue_rule.get_effective_priority(
                    self.ctxt, self.config["priority"]
                ),
                "bot_account": self.config["merge_bot_account"],
                "update_bot_account": self.config["update_bot_account"],
                "priority": self.config["priority"],
                "name": self.config["name"],
            }
        )
        await q.add_pull(self.ctxt, pull_queue_config, self.rule.get_signal_trigger())

        try:
            qf = await q.get_current_queue_freeze(self.config["name"])
            if await self._should_be_merged(self.ctxt, q, qf):
                result = await self._merge(q, qf, car)
            else:
                result = await self.get_pending_queue_status(
                    self.ctxt, self.rule, q, self.queue_rule, qf
                )
        except Exception as e:
            if not exceptions.need_retry(e):
                await q.remove_pull(
                    self.ctxt,
                    self.rule.get_signal_trigger(),
                    queue_utils.PrUnexpectedlyFailedToMerge(),
                )
            raise

        if result.conclusion is not check_api.Conclusion.PENDING:
            unqueue_reason = await self.get_unqueue_reason_from_action_result(result)
            await self._unqueue_pull_request(q, car, unqueue_reason, result)

        return result

    async def _unqueue_pull_request(
        self,
        q: merge_train.Train,
        car: merge_train.TrainCar | None,
        unqueue_reason: queue_utils.BaseUnqueueReason,
        result: check_api.Result,
    ) -> None:
        pull_request_rule_has_unmatched = result is actions.CANCELLED_CHECK_REPORT
        # NOTE(sileht): The PR has been checked successfully but the
        # final merge fail, we must erase the queue summary conclusion,
        # so the requeue can works.
        merge_has_been_cancelled = (
            car
            and car.train_car_state.outcome == merge_train.TrainCarOutcome.MERGEABLE
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
                raise RuntimeError("impossible if branch")
            await check_api.set_check_run(
                self.ctxt,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                check_api.Result(
                    conclusion,
                    f"The pull request {self.ctxt.pull['number']} cannot be merged and has been disembarked",
                    result.title + "\n" + result.summary,
                ),
            )

        await q.remove_pull(self.ctxt, self.rule.get_signal_trigger(), unqueue_reason)

    async def cancel(self) -> check_api.Result:
        q = await merge_train.Train.from_context(self.ctxt, self.queue_rules)
        car = q.get_car(self.ctxt)

        result = await self.pre_queue_checks(
            q,
            car,
        )

        if result is not None:
            return result

        if car is not None:
            await car.check_mergeability(
                "original_pull_request",
                original_pull_request_rule=self.rule,
                original_pull_request_number=self.ctxt.pull["number"],
            )

        # We just rebase the pull request, don't cancel it yet if CIs are
        # running. The pull request will be merged if all rules match again.
        # if not we will delete it when we received all CIs termination
        if await self._should_be_cancel(self.ctxt, self.rule, q, car):
            result = actions.CANCELLED_CHECK_REPORT
            unqueue_reason = await self.get_unqueue_reason_from_action_result(result)
            await self._unqueue_pull_request(q, car, unqueue_reason, result)
            return result

        if not await self._should_be_queued(self.ctxt):
            unqueue_reason = await self.get_unqueue_reason_from_outcome(self.ctxt)
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                "The pull request has been removed from the queue",
                f"{unqueue_reason!s}.\n{self.UNQUEUE_DOCUMENTATION}",
            )
            await self._unqueue_pull_request(q, car, unqueue_reason, result)
            return result

        qf = await q.get_current_queue_freeze(self.config["name"])
        result = await self.get_pending_queue_status(
            self.ctxt, self.rule, q, self.queue_rule, qf
        )

        if result.conclusion is not check_api.Conclusion.PENDING:
            unqueue_reason = await self.get_unqueue_reason_from_action_result(result)
            await self._unqueue_pull_request(q, car, unqueue_reason, result)
        return result

    async def _render_bot_account(self) -> None:
        try:
            self.config["update_bot_account"] = await action_utils.render_bot_account(
                self.ctxt,
                self.config["update_bot_account"],
                bot_account_fallback=None,
                option_name="update_bot_account",
                required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
                missing_feature_message="Cannot use `update_bot_account` with queue action",
            )
        except action_utils.RenderBotAccountFailure as e:
            raise InvalidQueueConfiguration(e.title, e.reason)

        try:
            self.config["merge_bot_account"] = await action_utils.render_bot_account(
                self.ctxt,
                self.config["merge_bot_account"],
                bot_account_fallback=None,
                option_name="merge_bot_account",
                required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
                missing_feature_message="Cannot use `merge_bot_account` with queue action",
                # NOTE(sileht): we don't allow admin, because if branch protection are
                # enabled, but not enforced on admins, we may bypass them
                required_permissions=[github_types.GitHubRepositoryPermission.WRITE],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise InvalidQueueConfiguration(e.title, e.reason)

    async def _check_action_validity(self) -> check_api.Result | None:
        try:
            await self._set_action_queue_rule()
            await self._check_config_compatibility_with_branch_protection(
                queue_rule=self.queue_rule, ctxt=self.ctxt
            )
            self._check_method_fastforward_configuration(
                config=self.config,
                queue_rule=self.queue_rule,
            )
            await self._render_bot_account()
        except InvalidQueueConfiguration as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                e.title,
                e.message,
            )

        try:
            await self._check_subscription_status(
                config=self.config,
                queue_rule=self.queue_rule,
                queue_rules=self.queue_rules,
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
        queue: merge_train.Train,
        car: merge_train.TrainCar | None,
    ) -> check_api.Result | None:
        result = await self._check_action_validity()

        if result is None:
            result = await self.pre_merge_checks(
                self.ctxt,
                self.config["method"],
                self.config["rebase_fallback"],
                self.config["merge_bot_account"],
            )

        if result is not None:
            if result.conclusion is not check_api.Conclusion.PENDING:
                unqueue_reason = await self.get_unqueue_reason_from_action_result(
                    result
                )
                await self._unqueue_pull_request(queue, car, unqueue_reason, result)

        return result

    async def get_unqueue_reason_from_action_result(
        self, result: check_api.Result
    ) -> queue_utils.BaseUnqueueReason:
        if result.conclusion is check_api.Conclusion.PENDING:
            raise RuntimeError(
                "get_unqueue_reason_from_action_result() on PENDING result"
            )
        elif result.conclusion is check_api.Conclusion.SUCCESS:
            if self.ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")
            return queue_utils.PrMerged(
                self.ctxt.pull["number"], self.ctxt.pull["merge_commit_sha"]
            )
        else:
            return queue_utils.PrDequeued(
                self.ctxt.pull["number"], details=f". {result.title}"
            )

    @staticmethod
    async def get_unqueue_reason_from_outcome(
        ctxt: context.Context,
    ) -> queue_utils.BaseUnqueueReason:
        check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        if check is None:
            raise RuntimeError(
                "get_unqueue_reason_from_outcome() called by check is not there"
            )

        elif check["conclusion"] == "cancelled":
            # NOTE(sileht): should not be possible as unqueue command already
            # remove the pull request from the queue
            return queue_utils.PrDequeued(
                ctxt.pull["number"], " by an `unqueue` command"
            )

        train_car_state = merge_train.TrainCarStateForSummary.deserialize_from_summary(
            check
        )
        if train_car_state is None:
            # NOTE(sileht): No details but we can't do much at this point
            return queue_utils.PrDequeued(
                ctxt.pull["number"], " due to failing checks or checks timeout"
            )

        if (
            train_car_state.outcome
            in (
                merge_train.TrainCarOutcome.DRAFT_PR_CHANGE,
                merge_train.TrainCarOutcome.BASE_BRANCH_CHANGE,
                merge_train.TrainCarOutcome.UPDATED_PR_CHANGE,
            )
            and train_car_state.outcome_message is not None
        ):
            return queue_utils.UnexpectedQueueChange(
                change=train_car_state.outcome_message
            )
        elif train_car_state.outcome == merge_train.TrainCarOutcome.CHECKS_FAILED:
            return queue_utils.ChecksFailed()
        elif train_car_state.outcome == merge_train.TrainCarOutcome.CHECKS_TIMEOUT:
            return queue_utils.ChecksTimeout()
        elif (
            train_car_state.outcome
            == merge_train.TrainCarOutcome.BATCH_MAX_FAILURE_RESOLUTION_ATTEMPTS
        ):
            return queue_utils.MaximumBatchFailureResolutionAttemptsReached()
        else:
            raise RuntimeError(
                f"TrainCarState.outcome `{train_car_state.outcome.value}` can't be mapped to an AbortReason"
            )

    @staticmethod
    async def _should_be_queued(ctxt: context.Context) -> bool:
        # TODO(sileht): load outcome from summary, so we known why it shouldn't
        # be queued
        check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        return not check or check_api.Conclusion(check["conclusion"]) in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.PENDING,
            check_api.Conclusion.NEUTRAL,
        ]

    @staticmethod
    async def _should_be_merged(
        ctxt: context.Context,
        queue: merge_train.Train,
        queue_freeze: freeze.QueueFreeze | None,
    ) -> bool:
        if queue_freeze is not None:
            return False

        if not await queue.is_first_pull(ctxt):
            return False

        car = queue.get_car(ctxt)
        if car is None:
            return False

        return car.train_car_state.outcome == merge_train.TrainCarOutcome.MERGEABLE

    @staticmethod
    async def _should_be_cancel(
        ctxt: context.Context,
        rule: "prr_config.EvaluatedPullRequestRule",
        q: merge_train.Train,
        previous_car: merge_train.TrainCar | None,
    ) -> bool:
        # It's closed, it's not going to change
        if ctxt.closed:
            return True

        if await ctxt.synchronized_by_user_at() is not None:
            return True

        position, _ = q.find_embarked_pull(ctxt.pull["number"])
        if position is None:
            return True

        car = q.get_car(ctxt)
        if car is None:
            # NOTE(sileht): in case the car just got deleted but the PR is
            # still first in queue This means the train has been resetted for
            # some reason. If we was waiting for CIs to complete, we must not
            # cancel the action as the train car will be recreated very soon
            # (by Train.refresh()).
            # To keep it queued, we check the state against the previous car
            # version one last time.
            car = previous_car

        if (
            car
            and car.train_car_state.checks_type
            == merge_train.TrainCarChecksType.INPLACE
        ):
            # NOTE(sileht) check first if PR should be removed from the queue
            pull_rule_checks_status = await checks_status.get_rule_checks_status(
                ctxt.log, ctxt.repository, [ctxt.pull_request], rule
            )
            # NOTE(sileht): if the pull request rules are pending we wait their
            # match before checking queue rules states, in case of one
            # condition suddently unqueue the pull request.
            # TODO(sileht): we may want to make this behavior configurable as
            # people having slow/long CI running only on pull request rules, we
            # may want to merge it before it finishes.
            return pull_rule_checks_status not in (
                check_api.Conclusion.PENDING,
                check_api.Conclusion.SUCCESS,
            )

        return True

    @staticmethod
    async def get_pending_queue_status(
        ctxt: context.Context,
        rule: "prr_config.EvaluatedPullRequestRule",
        queue: merge_train.Train,
        queue_rule: qr_config.QueueRule,
        queue_freeze: freeze.QueueFreeze | None,
    ) -> check_api.Result:
        if queue_freeze is not None:
            title = queue_freeze.get_freeze_message()
        else:
            position, _ = queue.find_embarked_pull(ctxt.pull["number"])
            if position is None:
                ctxt.log.error("expected queued pull request not found in queue")
                title = "The pull request is queued to be merged"
            else:
                _ord = utils.to_ordinal_numeric(position + 1)
                title = f"The pull request is the {_ord} in the queue to be merged"

        summary = await queue.get_pull_summary(
            ctxt, queue_rule=queue_rule, pull_rule=rule
        )

        return check_api.Result(check_api.Conclusion.PENDING, title, summary)

    async def send_merge_signal(
        self, embarked_pull: merge_train.EmbarkedPullWithCar
    ) -> None:
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.queue.merged",
            signals.EventQueueMergedMetadata(
                {
                    "queue_name": self.config["name"],
                    "branch": self.ctxt.pull["base"]["ref"],
                    "queued_at": embarked_pull.embarked_pull.queued_at,
                }
            ),
            self.rule.get_signal_trigger(),
        )

    @staticmethod
    def _check_method_fastforward_configuration(
        config: QueueExecutorConfig,
        queue_rule: qr_config.QueueRule,
    ) -> None:
        if config["method"] != "fast-forward":
            return

        if config["update_method"] != "rebase":
            raise InvalidQueueConfiguration(
                f"`update_method: {config['update_method']}` is not compatible with fast-forward merge method",
                "`update_method` must be set to `rebase`.",
            )
        elif config["commit_message_template"] is not None:
            raise InvalidQueueConfiguration(
                "Commit message can't be changed with fast-forward merge method",
                "`commit_message_template` must not be set if `method: fast-forward` is set.",
            )
        elif queue_rule.config["batch_size"] > 1:
            raise InvalidQueueConfiguration(
                "batch_size > 1 is not compatible with fast-forward merge method",
                "The merge `method` or the queue configuration must be updated.",
            )
        elif queue_rule.config["speculative_checks"] > 1:
            raise InvalidQueueConfiguration(
                "speculative_checks > 1 is not compatible with fast-forward merge method",
                "The merge `method` or the queue configuration must be updated.",
            )
        elif not queue_rule.config["allow_inplace_checks"]:
            raise InvalidQueueConfiguration(
                "allow_inplace_checks=False is not compatible with fast-forward merge method",
                "The merge `method` or the queue configuration must be updated.",
            )

    @staticmethod
    async def _check_subscription_status(
        config: QueueExecutorConfig,
        queue_rule: qr_config.QueueRule,
        queue_rules: qr_config.QueueRules,
        ctxt: context.Context,
    ) -> None:
        if len(queue_rules) > 1 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION
        ):
            raise InvalidQueueConfiguration(
                "Cannot use multiple queues.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        elif queue_rule.config[
            "speculative_checks"
        ] > 1 and not ctxt.subscription.has_feature(subscription.Features.QUEUE_ACTION):
            raise InvalidQueueConfiguration(
                "Cannot use `speculative_checks` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        elif queue_rule.config["batch_size"] > 1 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION
        ):
            raise InvalidQueueConfiguration(
                "Cannot use `batch_size` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )
        elif config[
            "priority"
        ] != queue.PriorityAliases.medium.value and not ctxt.subscription.has_feature(
            subscription.Features.PRIORITY_QUEUES
        ):
            raise InvalidQueueConfiguration(
                "Cannot use `priority` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

    @staticmethod
    async def _check_config_compatibility_with_branch_protection(
        queue_rule: qr_config.QueueRule, ctxt: context.Context
    ) -> None:
        if queue_rule.config["queue_branch_merge_method"] == "fast-forward":
            # Note(charly): `queue_branch_merge_method=fast-forward` make the
            # use of batches compatible with branch protection
            # `required_status_checks=strict`
            return None

        protection = await ctxt.repository.get_branch_protection(
            ctxt.pull["base"]["ref"]
        )

        _has_required_status_checks_strict = (
            protection is not None
            and "required_status_checks" in protection
            and "strict" in protection["required_status_checks"]
            and protection["required_status_checks"]["strict"]
        )

        if _has_required_status_checks_strict:
            if queue_rule.config["batch_size"] > 1:
                raise IncompatibleBranchProtection(
                    "batch_size>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            elif queue_rule.config["speculative_checks"] > 1:
                raise IncompatibleBranchProtection(
                    "speculative_checks>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            elif queue_rule.config["allow_inplace_checks"] is False:
                raise IncompatibleBranchProtection(
                    "allow_inplace_checks=false",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )


class QueueAction(actions.Action):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.SUCCESS_IS_FINAL_STATE
        | actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
        # FIXME(sileht): MRGFY-562
        # | actions.ActionFlag.ALWAYS_RUN
    )

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    # NOTE(sileht): set by validate_config()
    queue_rules: qr_config.QueueRules = dataclasses.field(init=False, repr=False)
    queue_rule: qr_config.QueueRule = dataclasses.field(init=False, repr=False)

    @property
    def validator(self) -> dict[typing.Any, typing.Any]:
        validator = {
            voluptuous.Required("name", default=None): voluptuous.Any(
                str,
                None,
            ),
            voluptuous.Required("method", default=None): voluptuous.Any(
                None,  # fallback to queue_rule
                "rebase",
                "merge",
                "squash",
                "fast-forward",
            ),
            voluptuous.Required(
                "merge_bot_account", default=None
            ): types.Jinja2WithNone,
            voluptuous.Required(
                "update_bot_account", default=None
            ): types.Jinja2WithNone,
            voluptuous.Required("update_method", default=None): voluptuous.Any(
                "rebase", "merge", None
            ),
            voluptuous.Required(
                "commit_message_template", default=None
            ): types.Jinja2WithNone,
            voluptuous.Required("require_branch_protection", default=True): bool,
            voluptuous.Required(
                "allow_merging_configuration_change", default=False
            ): bool,
        }

        # NOTE(Syffe): In deprecation process
        if config.ALLOW_REBASE_FALLBACK_ATTRIBUTE:
            validator[
                voluptuous.Required("rebase_fallback", default="none")
            ] = voluptuous.Any("merge", "squash", "none", None)
        else:
            validator[
                voluptuous.Required("rebase_fallback", default=utils.UnsetMarker)
            ] = utils.DeprecatedOption(
                DEPRECATED_MESSAGE_REBASE_FALLBACK_QUEUE_ACTION, "none"
            )

        if config.ALLOW_QUEUE_PRIORITY_ATTRIBUTE:
            validator[
                voluptuous.Required(
                    "priority", default=queue.PriorityAliases.medium.value
                )
            ] = queue.PrioritySchema
        else:
            validator[
                voluptuous.Required("priority", default=utils.UnsetMarker)
            ] = utils.DeprecatedOption(
                DEPRECATED_MESSAGE_PRIORITY_ATTRIBUTE_QUEUE_ACTION,
                queue.PriorityAliases.medium.value,
            )

        return validator

    def validate_config(self, mergify_config: "mergify_conf.MergifyConfig") -> None:
        self.queue_rules = mergify_config["queue_rules"]
        if (
            self.config["name"] is not None
            and mergify_config["queue_rules"].get(self.config["name"]) is None
        ):
            raise voluptuous.error.Invalid(f"`{self.config['name']}` queue not found")

    @staticmethod
    def command_to_config(command_arguments: str) -> dict[str, typing.Any]:
        # NOTE(sileht): requiring branch_protection before putting in queue
        # doesn't play well with command subsystem, and it's not intuitive as
        # the user tell us to put the PR in queue and by default we don't. So
        # we disable this for commands.
        name: str | None = None
        config = {"name": name, "require_branch_protection": False}
        args = [
            arg_stripped
            for arg in command_arguments.split(" ")
            if (arg_stripped := arg.strip())
        ]
        if args and args[0]:
            config["name"] = args[0]
        return config

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        conditions_requirements = []
        if self.config["require_branch_protection"]:
            conditions_requirements.extend(
                await conditions.get_branch_protection_conditions(
                    ctxt.repository, ctxt.pull["base"]["ref"], strict=False
                )
            )

        conditions_requirements.append(
            conditions.get_mergify_configuration_change_conditions(
                "queue", self.config["allow_merging_configuration_change"]
            )
        )
        conditions_requirements.extend(await conditions.get_depends_on_conditions(ctxt))
        conditions_requirements.append(
            conditions.RuleCondition.from_tree(
                {"=": ("draft", False)}, description=":pushpin: queue requirement"
            )
        )
        return conditions_requirements

    executor_class = QueueExecutor
