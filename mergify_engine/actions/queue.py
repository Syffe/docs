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
from mergify_engine import rules
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


BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT = (
    "Require branches to be up to date before merging"
)


DEPRECATED_MESSAGE_REBASE_FALLBACK_QUEUE_ACTION = """The configuration uses the deprecated `rebase_fallback` attribute of the queue action.
A brownout is planned on February 13th, 2023.
This option will be removed on March 13th, 2023.
For more information: https://docs.mergify.com/actions/queue/

`%s` is invalid"""


@dataclasses.dataclass
class IncompatibleBranchProtection(Exception):
    """Incompatibility between Mergify configuration and branch protection settings"""

    configuration: str
    branch_protection_setting: str
    message = "Configuration not compatible with a branch protection setting"


class QueueAction(actions.BackwardCompatAction, merge_base.MergeUtilsMixin):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.SUCCESS_IS_FINAL_STATE
        | actions.ActionFlag.ALLOW_AS_PENDING_COMMAND
        # FIXME(sileht): MRGFY-562
        # | actions.ActionFlag.ALWAYS_RUN
    )

    UNQUEUE_DOCUMENTATION = f"""
You can take a look at `{constants.MERGE_QUEUE_SUMMARY_NAME}` check runs for more details.

In case of a failure due to a flaky test, you should first retrigger the CI.
Then, re-embark the pull request into the merge queue by posting the comment
`@mergifyio refresh` on the pull request.
"""

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        "sender-permission>=write"
    ]

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @property
    def validator(self) -> dict[typing.Any, typing.Any]:
        validator = {
            voluptuous.Required("name", default="default"): str,
            voluptuous.Required("method", default="merge"): voluptuous.Any(
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
            voluptuous.Required(
                "priority", default=queue.PriorityAliases.medium.value
            ): queue.PrioritySchema,
            voluptuous.Required("require_branch_protection", default=True): bool,
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
                DEPRECATED_MESSAGE_REBASE_FALLBACK_QUEUE_ACTION,
                voluptuous.Any("merge", "squash", "none", None),
            )

        return validator

    @staticmethod
    def command_to_config(command_arguments: str) -> dict[str, typing.Any]:
        # NOTE(sileht): requiring branch_protection before putting in queue
        # doesn't play well with command subsystem, and it's not intuitive as
        # the user tell us to put the PR in queue and by default we don't. So
        # we disable this for commands.
        config = {"name": "default", "require_branch_protection": False}
        args = [
            arg_stripped
            for arg in command_arguments.split(" ")
            if (arg_stripped := arg.strip())
        ]
        if args and args[0]:
            config["name"] = args[0]
        return config

    async def _queue_branch_merge_pull_request(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        queue: merge_train.Train,
        queue_freeze: freeze.QueueFreeze | None,
        car: merge_train.TrainCar | None,
        merge_bot_account: github_types.GitHubLogin | None,
    ) -> check_api.Result:
        result = await self.common_merge(
            ctxt,
            rule,
            self.config["method"],
            self.config["rebase_fallback"],
            merge_bot_account,
            self.config["commit_message_template"],
            functools.partial(
                self.get_pending_queue_status,
                queue=queue,
                queue_freeze=queue_freeze,
            ),
        )
        if result.conclusion == check_api.Conclusion.SUCCESS:
            _, embarked_pull = queue.find_embarked_pull(ctxt.pull["number"])
            if embarked_pull is None:
                raise RuntimeError("Queue pull request with no embarked_pull")
            if ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")
            await queue.remove_pull(
                ctxt,
                rule.get_signal_trigger(),
                queue_utils.PrMerged(
                    ctxt.pull["number"], ctxt.pull["merge_commit_sha"]
                ),
            )
            await self.send_merge_signal(ctxt, rule, embarked_pull)
        return result

    async def _queue_branch_merge_fastforward(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        queue: merge_train.Train,
        queue_freeze: freeze.QueueFreeze | None,
        car: merge_train.TrainCar | None,
        merge_bot_account: github_types.GitHubLogin | None,
    ) -> check_api.Result:

        if car is None:
            raise RuntimeError("ready to merge PR without car....")

        if self.config["method"] != "merge":
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                f"Cannot use method={self.config['method']} with queue_branch_merge_method=fast-forward",
                "Only `method=merge` is supported with `queue_branch_merge_method=fast-forward`",
            )

        github_user: user_tokens.UserTokensUser | None = None
        if merge_bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(merge_bot_account)
            if not github_user:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    f"Unable to rebase: user `{merge_bot_account}` is unknown. ",
                    f"Please make sure `{merge_bot_account}` has logged in Mergify dashboard.",
                )

        if car.train_car_state.checks_type == merge_train.TrainCarChecksType.INPLACE:
            newsha = ctxt.pull["head"]["sha"]
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
            await ctxt.client.put(
                f"{ctxt.base_url}/git/refs/heads/{ctxt.pull['base']['ref']}",
                oauth_token=github_user["oauth_access_token"] if github_user else None,
                json={"sha": newsha},
            )
        except http.HTTPClientSideError as e:  # pragma: no cover
            return await self._handle_merge_error(
                e,
                ctxt,
                rule,
                functools.partial(
                    self.get_pending_queue_status,
                    queue=queue,
                    queue_freeze=queue_freeze,
                ),
            )

        for embarked_pull in car.still_queued_embarked_pulls.copy():
            other_ctxt = await queue.repository.get_pull_request_context(
                embarked_pull.user_pull_request_number
            )
            await queue.remove_pull(
                other_ctxt,
                rule.get_signal_trigger(),
                queue_utils.PrMerged(ctxt.pull["number"], newsha),
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
            context.Context,
            "rules.EvaluatedPullRequestRule",
            merge_train.Train,
            freeze.QueueFreeze | None,
            merge_train.TrainCar | None,
            github_types.GitHubLogin | None,
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

    async def _subscription_status(
        self, ctxt: context.Context
    ) -> check_api.Result | None:
        if len(self.queue_rules) > 1 and not ctxt.subscription.has_feature(
            subscription.Features.QUEUE_ACTION
        ):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Cannot use multiple queues.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        elif self.queue_rule.config[
            "speculative_checks"
        ] > 1 and not ctxt.subscription.has_feature(subscription.Features.QUEUE_ACTION):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Cannot use `speculative_checks` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        elif self.queue_rule.config[
            "batch_size"
        ] > 1 and not ctxt.subscription.has_feature(subscription.Features.QUEUE_ACTION):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Cannot use `batch_size` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )
        elif self.config[
            "priority"
        ] != queue.PriorityAliases.medium.value and not ctxt.subscription.has_feature(
            subscription.Features.PRIORITY_QUEUES
        ):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Cannot use `priority` with queue action.",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        return None

    async def _render_bot_account(
        self, ctxt: context.Context
    ) -> github_types.GitHubLogin | None:
        return await action_utils.render_bot_account(
            ctxt,
            self.config["merge_bot_account"],
            option_name="merge_bot_account",
            required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
            missing_feature_message="Cannot use `merge_bot_account` with queue action",
            # NOTE(sileht): we don't allow admin, because if branch protection are
            # enabled, but not enforced on admins, we may bypass them
            required_permissions=[github_types.GitHubRepositoryPermission.WRITE],
        )

    async def run(
        self, ctxt: context.Context, rule: "rules.EvaluatedPullRequestRule"
    ) -> check_api.Result:
        subscription_status = await self._subscription_status(ctxt)
        if subscription_status:
            return subscription_status

        if self.config["method"] == "fast-forward":
            if self.config["update_method"] != "rebase":
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    f"`update_method: {self.config['update_method']}` is not compatible with fast-forward merge method",
                    "`update_method` must be set to `rebase`.",
                )
            elif self.config["commit_message_template"] is not None:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "Commit message can't be changed with fast-forward merge method",
                    "`commit_message_template` must not be set if `method: fast-forward` is set.",
                )
            elif self.queue_rule.config["batch_size"] > 1:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "batch_size > 1 is not compatible with fast-forward merge method",
                    "The merge `method` or the queue configuration must be updated.",
                )
            elif self.queue_rule.config["speculative_checks"] > 1:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "speculative_checks > 1 is not compatible with fast-forward merge method",
                    "The merge `method` or the queue configuration must be updated.",
                )
            elif not self.queue_rule.config["allow_inplace_checks"]:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "allow_inplace_checks=False is not compatible with fast-forward merge method",
                    "The merge `method` or the queue configuration must be updated.",
                )

        try:
            await self._check_config_compatibility_with_branch_protection(ctxt)
        except IncompatibleBranchProtection as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                e.message,
                "The branch protection setting "
                f"`{e.branch_protection_setting}` is not compatible with `{e.configuration}` "
                "and must be unset.",
            )

        try:
            update_bot_account = await action_utils.render_bot_account(
                ctxt,
                self.config["update_bot_account"],
                option_name="update_bot_account",
                required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
                missing_feature_message="Cannot use `update_bot_account` with queue action",
            )
        except action_utils.RenderBotAccountFailure as e:
            return check_api.Result(e.status, e.title, e.reason)

        try:
            merge_bot_account = await self._render_bot_account(ctxt)
        except action_utils.RenderBotAccountFailure as e:
            return check_api.Result(e.status, e.title, e.reason)

        if ctxt.user_refresh_requested() or ctxt.admin_refresh_requested():
            # NOTE(sileht): user ask a refresh, we just remove the previous state of this
            # check and the method _should_be_queued will become true again :)
            check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
            if check and check_api.Conclusion(check["conclusion"]) not in [
                check_api.Conclusion.SUCCESS,
                check_api.Conclusion.PENDING,
                check_api.Conclusion.NEUTRAL,
            ]:
                ctxt.log.info(
                    "a refresh marks the pull request as re-embarkable",
                    check=check,
                    user_refresh=ctxt.user_refresh_requested(),
                    admin_refresh=ctxt.admin_refresh_requested(),
                )
                await check_api.set_check_run(
                    ctxt,
                    constants.MERGE_QUEUE_SUMMARY_NAME,
                    check_api.Result(
                        check_api.Conclusion.PENDING,
                        "The pull request has been refreshed and is going to be re-embarked soon",
                        "",
                    ),
                )

        q = await merge_train.Train.from_context(ctxt)
        car = q.get_car(ctxt)
        result = await self.pre_merge_checks(
            ctxt,
            self.config["method"],
            self.config["rebase_fallback"],
            merge_bot_account,
        )
        if result is not None:
            if result.conclusion is not check_api.Conclusion.PENDING:
                unqueue_reason = await self.get_unqueue_reason_from_action_result(
                    ctxt, result
                )
                await self._unqueue_pull_request(
                    ctxt, rule, q, car, unqueue_reason, result
                )
            return result

        if car is not None:
            await car.check_mergeability(
                self.queue_rules,
                "original_pull_request",
                original_pull_request_rule=rule,
                original_pull_request_number=ctxt.pull["number"],
            )

        # NOTE(sileht): the pull request gets checked and then changed
        # by user, we should unqueue and requeue it as the conditions still match.
        if await ctxt.has_been_synchronized_by_user():
            unexpected_change = queue_utils.UnexpectedQueueChange(
                str(merge_train.UnexpectedUpdatedPullRequestChange(ctxt.pull["number"]))
            )
            await q.remove_pull(
                ctxt,
                rule.get_signal_trigger(),
                unexpected_change,
            )
            if isinstance(rule, rules.CommandRule):
                return check_api.Result(
                    check_api.Conclusion.CANCELLED,
                    "The pull request has been removed from the queue",
                    f"{unexpected_change!s}.\n{self.UNQUEUE_DOCUMENTATION}",
                )

        if not await self._should_be_queued(ctxt, q):
            unqueue_reason = await self.get_unqueue_reason_from_outcome(ctxt, q)
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                "The pull request has been removed from the queue",
                f"{unqueue_reason!s}.\n{self.UNQUEUE_DOCUMENTATION}",
            )
            await self._unqueue_pull_request(ctxt, rule, q, car, unqueue_reason, result)
            return result

        pull_queue_config = queue.PullQueueConfig(
            {
                "update_method": self.config["update_method"],
                "effective_priority": await self.queue_rule.get_effective_priority(
                    ctxt, self.config["priority"]
                ),
                "bot_account": merge_bot_account,
                "update_bot_account": update_bot_account,
                "priority": self.config["priority"],
                "name": self.config["name"],
            }
        )
        await q.add_pull(
            self.queue_rules,
            ctxt,
            pull_queue_config,
            rule.get_signal_trigger(),
        )

        try:
            qf = await q.get_current_queue_freeze(self.queue_rules, self.config["name"])
            if await self._should_be_merged(ctxt, q, qf):
                result = await self._merge(ctxt, rule, q, qf, car, merge_bot_account)
            else:
                result = await self.get_pending_queue_status(ctxt, rule, q, qf)
        except Exception as e:
            if not exceptions.need_retry(e):
                await q.remove_pull(
                    ctxt,
                    rule.get_signal_trigger(),
                    queue_utils.PrUnexpectedlyFailedToMerge(),
                )
            raise

        if result.conclusion is not check_api.Conclusion.PENDING:
            unqueue_reason = await self.get_unqueue_reason_from_action_result(
                ctxt, result
            )
            await self._unqueue_pull_request(ctxt, rule, q, car, unqueue_reason, result)

        return result

    @staticmethod
    async def _unqueue_pull_request(
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        q: merge_train.Train,
        car: merge_train.TrainCar | None,
        unqueue_reason: queue_utils.BaseUnqueueReason,
        result: check_api.Result,
    ) -> None:
        # NOTE(sileht): The PR has been checked successfully but the
        # final merge fail, we must erase the queue summary conclusion,
        # so the requeue can works.
        if (
            car
            and car.train_car_state.outcome == merge_train.TrainCarOutcome.MERGEABLE
            and result.conclusion is check_api.Conclusion.CANCELLED
        ):
            await check_api.set_check_run(
                ctxt,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                check_api.Result(
                    # FIXME(sileht): CANCELLED mean unqueue with command, we should have put something else here, FAILURE?
                    check_api.Conclusion.CANCELLED,
                    f"The pull request {ctxt.pull['number']} cannot be merged and has been disembarked",
                    result.title + "\n" + result.summary,
                ),
            )

        await q.remove_pull(ctxt, rule.get_signal_trigger(), unqueue_reason)

    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedPullRequestRule"
    ) -> check_api.Result:
        try:
            await action_utils.render_bot_account(
                ctxt,
                self.config["update_bot_account"],
                option_name="update_bot_account",
                required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
                missing_feature_message="Cannot use `update_bot_account` with queue action",
            )
        except action_utils.RenderBotAccountFailure as e:
            return check_api.Result(e.status, e.title, e.reason)

        try:
            merge_bot_account = await self._render_bot_account(ctxt)
        except action_utils.RenderBotAccountFailure as e:
            return check_api.Result(e.status, e.title, e.reason)

        q = await merge_train.Train.from_context(ctxt)
        car = q.get_car(ctxt)

        result = await self.pre_merge_checks(
            ctxt,
            self.config["method"],
            self.config["rebase_fallback"],
            merge_bot_account,
        )
        if result is not None:
            if result.conclusion is not check_api.Conclusion.PENDING:
                unqueue_reason = await self.get_unqueue_reason_from_action_result(
                    ctxt, result
                )
                await self._unqueue_pull_request(
                    ctxt, rule, q, car, unqueue_reason, result
                )
            return result

        if car is not None:
            await car.check_mergeability(
                self.queue_rules,
                "original_pull_request",
                original_pull_request_rule=rule,
                original_pull_request_number=ctxt.pull["number"],
            )

        # We just rebase the pull request, don't cancel it yet if CIs are
        # running. The pull request will be merged if all rules match again.
        # if not we will delete it when we received all CIs termination
        if await self._should_be_cancel(ctxt, rule, q, car):
            result = actions.CANCELLED_CHECK_REPORT
            unqueue_reason = await self.get_unqueue_reason_from_action_result(
                ctxt, result
            )
            await self._unqueue_pull_request(ctxt, rule, q, car, unqueue_reason, result)
            return result

        if not await self._should_be_queued(ctxt, q):
            unqueue_reason = await self.get_unqueue_reason_from_outcome(ctxt, q)
            result = check_api.Result(
                check_api.Conclusion.CANCELLED,
                "The pull request has been removed from the queue",
                f"{unqueue_reason!s}.\n{self.UNQUEUE_DOCUMENTATION}",
            )
            await self._unqueue_pull_request(ctxt, rule, q, car, unqueue_reason, result)
            return result

        qf = await q.get_current_queue_freeze(self.queue_rules, self.config["name"])
        result = await self.get_pending_queue_status(ctxt, rule, q, qf)

        if result.conclusion is not check_api.Conclusion.PENDING:
            unqueue_reason = await self.get_unqueue_reason_from_action_result(
                ctxt, result
            )
            await self._unqueue_pull_request(ctxt, rule, q, car, unqueue_reason, result)
        return result

    def validate_config(self, mergify_config: "rules.MergifyConfig") -> None:
        if self.config["update_method"] is None:
            self.config["update_method"] = (
                "rebase" if self.config["method"] == "fast-forward" else "merge"
            )

        self.queue_rules = mergify_config["queue_rules"]
        try:
            self.queue_rule = mergify_config["queue_rules"][self.config["name"]]
        except KeyError:
            raise voluptuous.error.Invalid(f"`{self.config['name']}` queue not found")

    async def get_unqueue_reason_from_action_result(
        self, ctxt: context.Context, result: check_api.Result
    ) -> queue_utils.BaseUnqueueReason:
        if result.conclusion is check_api.Conclusion.PENDING:
            raise RuntimeError(
                "get_unqueue_reason_from_action_result() on PENDING result"
            )
        elif result.conclusion is check_api.Conclusion.SUCCESS:
            if ctxt.pull["merge_commit_sha"] is None:
                raise RuntimeError("pull request merged but merge_commit_sha is null")
            return queue_utils.PrMerged(
                ctxt.pull["number"], ctxt.pull["merge_commit_sha"]
            )
        else:
            return queue_utils.PrDequeued(
                ctxt.pull["number"], details=f". {result.title}"
            )

    async def get_unqueue_reason_from_outcome(
        self, ctxt: context.Context, q: merge_train.Train
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

        train_car_state = merge_train.TrainCarState.decode_train_car_state_from_summary(
            ctxt.repository, check
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

    async def _should_be_queued(
        self, ctxt: context.Context, q: merge_train.Train
    ) -> bool:
        # TODO(sileht): load outcome from summary, so we known why it shouldn't
        # be queued
        check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        return not check or check_api.Conclusion(check["conclusion"]) in [
            check_api.Conclusion.SUCCESS,
            check_api.Conclusion.PENDING,
            check_api.Conclusion.NEUTRAL,
        ]

    async def _should_be_merged(
        self,
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

    async def _should_be_cancel(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        q: merge_train.Train,
        previous_car: merge_train.TrainCar | None,
    ) -> bool:
        # It's closed, it's not going to change
        if ctxt.closed:
            return True

        if await ctxt.has_been_synchronized_by_user():
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

    async def get_pending_queue_status(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        queue: merge_train.Train,
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

        summary = await queue.get_pull_summary(ctxt, self.queue_rule, pull_rule=rule)

        return check_api.Result(check_api.Conclusion.PENDING, title, summary)

    async def send_merge_signal(
        self,
        ctxt: context.Context,
        rule: "rules.EvaluatedPullRequestRule",
        embarked_pull: merge_train.EmbarkedPullWithCar,
    ) -> None:
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.queue.merged",
            signals.EventQueueMergedMetadata(
                {
                    "queue_name": self.config["name"],
                    "branch": ctxt.pull["base"]["ref"],
                    "queued_at": embarked_pull.embarked_pull.queued_at,
                }
            ),
            rule.get_signal_trigger(),
        )

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        branch_protection_conditions = []
        if self.config["require_branch_protection"]:
            branch_protection_conditions = (
                await conditions.get_branch_protection_conditions(
                    ctxt.repository, ctxt.pull["base"]["ref"], strict=False
                )
            )
        depends_on_conditions = await conditions.get_depends_on_conditions(ctxt)
        return (
            branch_protection_conditions
            + depends_on_conditions
            + [
                conditions.RuleCondition(
                    "-draft", description=":pushpin: queue requirement"
                )
            ]
        )

    async def _check_config_compatibility_with_branch_protection(
        self, ctxt: context.Context
    ) -> None:
        if self.queue_rule.config["queue_branch_merge_method"] == "fast-forward":
            # Note(charly): `queue_branch_merge_method=fast-forward` make the
            # use of batches compatible with branch protection
            # `required_status_checks=strict`
            return None

        protection = await ctxt.repository.get_branch_protection(
            ctxt.pull["base"]["ref"]
        )
        if self._has_required_status_checks_strict(protection):
            if self.queue_rule.config["batch_size"] > 1:
                raise IncompatibleBranchProtection(
                    "batch_size>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            elif self.queue_rule.config["speculative_checks"] > 1:
                raise IncompatibleBranchProtection(
                    "speculative_checks>1",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )
            elif self.queue_rule.config["allow_inplace_checks"] is False:
                raise IncompatibleBranchProtection(
                    "allow_inplace_checks=false",
                    BRANCH_PROTECTION_REQUIRED_STATUS_CHECKS_STRICT,
                )

    @staticmethod
    def _has_required_status_checks_strict(
        protection: github_types.GitHubBranchProtection | None,
    ) -> bool:
        return (
            protection is not None
            and "required_status_checks" in protection
            and "strict" in protection["required_status_checks"]
            and protection["required_status_checks"]["strict"]
        )
