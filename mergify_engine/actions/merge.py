import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.actions import merge_base
from mergify_engine.actions import utils as action_utils
from mergify_engine.dashboard import subscription
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions
from mergify_engine.rules import types


DEPRECATED_MESSAGE_PRIORITY_ATTRIBUTE_MERGE_ACTION = """The configuration uses the deprecated `priority` attribute of the merge action.
A brownout is planned on December 28th, 2022.
This option will be removed on January 17th, 2023.
For more information: https://docs.mergify.com/actions/merge/

`%s` is invalid"""

DEPRECATED_MESSAGE_REBASE_FALLBACK_MERGE_ACTION = """The configuration uses the deprecated `rebase_fallback` attribute of the merge action.
A brownout is planned on February 13th, 2023.
This option will be removed on March 13th, 2023.
For more information: https://docs.mergify.com/actions/merge/

`%s` is invalid"""


class MergeExecutorConfig(typing.TypedDict):
    method: merge_base.MergeMethodT
    rebase_fallback: merge_base.RebaseFallbackT
    commit_message_template: str | None
    merge_bot_account: github_types.GitHubLogin | None
    priority: int


class MergeExecutor(
    actions.ActionExecutor["MergeAction", "MergeExecutorConfig"],
    merge_base.MergeUtilsMixin,
):
    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        return ()

    @classmethod
    async def create(
        cls,
        action: "MergeAction",
        ctxt: "context.Context",
        rule: "rules.EvaluatedPullRequestRule",
    ) -> "MergeExecutor":
        try:
            merge_bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["merge_bot_account"],
                option_name="merge_bot_account",
                required_feature=subscription.Features.MERGE_BOT_ACCOUNT,
                missing_feature_message="Cannot use `merge_bot_account` with merge action",
                # NOTE(sileht): we don't allow admin, because if branch protection are
                # enabled, but not enforced on admins, we may bypass them
                required_permissions=[github_types.GitHubRepositoryPermission.WRITE],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise rules.InvalidPullRequestRule(e.title, e.reason)

        if action.config["method"] == "fast-forward":
            if action.config["commit_message_template"] is not None:
                raise rules.InvalidPullRequestRule(
                    "Commit message can't be changed with fast-forward merge method",
                    "`commit_message_template` must not be set if `method: fast-forward` is set.",
                )

        return cls(
            ctxt,
            rule,
            MergeExecutorConfig(
                {
                    "method": action.config["method"],
                    "rebase_fallback": action.config["rebase_fallback"],
                    "commit_message_template": action.config["commit_message_template"],
                    "merge_bot_account": merge_bot_account,
                    "priority": action.config["priority"],
                }
            ),
        )

    async def run(self) -> check_api.Result:
        report = await self.pre_merge_checks(
            self.ctxt,
            self.config["method"],
            self.config["rebase_fallback"],
            self.config["merge_bot_account"],
        )
        if report is None:
            report = await self.common_merge(
                self.ctxt,
                self.rule,
                self.config["method"],
                self.config["rebase_fallback"],
                self.config["merge_bot_account"],
                self.config["commit_message_template"],
                self.get_pending_merge_status,
            )
            if report.conclusion == check_api.Conclusion.SUCCESS:
                queue = await merge_train.Train.from_context(self.ctxt)
                if queue.is_queued(self.ctxt.pull["number"]):
                    await queue.remove_pull(
                        self.ctxt,
                        self.rule.get_signal_trigger(),
                        queue_utils.PrDequeued(
                            self.ctxt.pull["number"],
                            ". Pull request automatically merged by a `merge` action",
                        ),
                    )
                await signals.send(
                    self.ctxt.repository,
                    self.ctxt.pull["number"],
                    "action.merge",
                    signals.EventMergeMetadata(
                        {"branch": self.ctxt.pull["base"]["ref"]}
                    ),
                    self.rule.get_signal_trigger(),
                )
        return report

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT

    async def get_pending_merge_status(
        self, ctxt: context.Context, rule: "rules.EvaluatedPullRequestRule"
    ) -> check_api.Result:
        return check_api.Result(
            check_api.Conclusion.PENDING, "The pull request will be merged soon", ""
        )


class MergeAction(actions.Action):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.SUCCESS_IS_FINAL_STATE
        # FIXME(sileht): MRGFY-562
        # enforce -merged/-closed in conditions requirements
        # | actions.ActionFlag.ALWAYS_RUN
    )

    @property
    def validator(self) -> dict[typing.Any, typing.Any]:
        validator = {
            voluptuous.Required("method", default="merge"): voluptuous.Any(
                *typing.get_args(merge_base.MergeMethodT)
            ),
            voluptuous.Required(
                "merge_bot_account", default=None
            ): types.Jinja2WithNone,
            voluptuous.Required(
                "commit_message_template", default=None
            ): types.Jinja2WithNone,
        }

        if config.ALLOW_REBASE_FALLBACK_ATTRIBUTE:
            # NOTE(sileht): None is supported for legacy reason
            # in deprecation process
            validator[
                voluptuous.Required("rebase_fallback", default="none")
            ] = voluptuous.Any(*typing.get_args(merge_base.RebaseFallbackT))
        else:
            validator[
                voluptuous.Required("rebase_fallback", default=utils.UnsetMarker)
            ] = utils.DeprecatedOption(
                DEPRECATED_MESSAGE_REBASE_FALLBACK_MERGE_ACTION,
                voluptuous.Any(*typing.get_args(merge_base.RebaseFallbackT)),
            )

        if config.ALLOW_MERGE_PRIORITY_ATTRIBUTE:
            validator[
                voluptuous.Required(
                    "priority", default=queue.PriorityAliases.medium.value
                )
            ] = queue.PrioritySchema
        else:
            validator[
                voluptuous.Required("priority", default=utils.UnsetMarker)
            ] = utils.DeprecatedOption(
                DEPRECATED_MESSAGE_PRIORITY_ATTRIBUTE_MERGE_ACTION,
                queue.PriorityAliases.medium.value,
            )

        return validator

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        conditions_requirements: list[conditions.RuleConditionNode] = []
        if self.config["method"] == "fast-forward":
            conditions_requirements.append(
                conditions.RuleCondition(
                    "#commits-behind=0",
                    description=":pushpin: fast-forward merge requirement",
                )
            )

        conditions_requirements.append(
            conditions.RuleCondition(
                "-draft", description=":pushpin: merge requirement"
            )
        )
        conditions_requirements.extend(
            await conditions.get_branch_protection_conditions(
                ctxt.repository, ctxt.pull["base"]["ref"], strict=True
            )
        )
        conditions_requirements.extend(await conditions.get_depends_on_conditions(ctxt))
        return conditions_requirements

    executor_class = MergeExecutor
