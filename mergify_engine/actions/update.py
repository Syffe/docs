import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import branch_updater
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.dashboard import subscription
from mergify_engine.rules import conditions
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


class UpdateExecutorConfig(typing.TypedDict):
    bot_account: github_types.GitHubLogin | None


class UpdateExecutor(actions.ActionExecutor["UpdateAction", "UpdateExecutorConfig"]):
    @classmethod
    async def create(
        cls,
        action: "UpdateAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "UpdateExecutor":
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=None,
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Update with `bot_account` set are disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, e.title, e.reason
            )

        return cls(ctxt, rule, UpdateExecutorConfig({"bot_account": bot_account}))

    async def run(self) -> check_api.Result:
        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                "update", self.config["bot_account"]
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        try:
            await branch_updater.update_with_api(self.ctxt, on_behalf)
        except branch_updater.BranchUpdateFailure as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                e.title,
                e.message,
            )
        else:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                "action.update",
                signals.EventNoMetadata(),
                self.rule.get_signal_trigger(),
            )

            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Branch has been successfully updated",
                "",
            )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class UpdateAction(actions.Action):
    flags = (
        actions.ActionFlag.ALWAYS_RUN | actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
    )
    validator: typing.ClassVar[dict[typing.Any, typing.Any]] = {
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
    }

    executor_class = UpdateExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        {"or": ["sender-permission>=write", "sender={{author}}"]}
    ]

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        description = ":pushpin: update requirement"
        return [
            conditions.RuleCondition.from_tree(
                {"=": ("closed", False)},
                description=description,
            ),
            conditions.RuleCondition.from_tree(
                {">": ("#commits-behind", 0)},
                description=description,
            ),
        ]
