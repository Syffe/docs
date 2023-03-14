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


class RebaseExecutorConfig(typing.TypedDict):
    autosquash: bool
    bot_account: github_types.GitHubLogin


class RebaseExecutor(actions.ActionExecutor["RebaseAction", RebaseExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "RebaseAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "RebaseExecutor":
        if isinstance(rule, prr_config.CommandRule):
            bot_account_fallback = rule.sender["login"]
        else:
            bot_account_fallback = ctxt.pull["user"]["login"]

        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=bot_account_fallback,
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Comments with `bot_account` set are disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, e.title, e.reason
            )

        return cls(
            ctxt,
            rule,
            RebaseExecutorConfig(
                {"bot_account": bot_account, "autosquash": action.config["autosquash"]}
            ),
        )

    async def run(self) -> check_api.Result:
        if (
            self.config["autosquash"]
            and await self.ctxt.commits_behind_count == 0
            and await self.ctxt.has_linear_history()
            and not await self.ctxt.has_squashable_commits()
        ):
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Nothing to do for rebase action", ""
            )

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                "rebase", self.config["bot_account"]
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        try:
            await branch_updater.rebase_with_git(
                self.ctxt,
                on_behalf,
                self.config["autosquash"],
            )
        except branch_updater.BranchUpdateFailure as e:
            return check_api.Result(check_api.Conclusion.FAILURE, e.title, e.message)

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.rebase",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "Branch has been successfully rebased",
            "",
        )

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT


class RebaseAction(actions.Action):
    flags = (
        actions.ActionFlag.ALWAYS_RUN | actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
    )
    validator = {
        voluptuous.Required("bot_account", default=None): voluptuous.Any(
            None, types.Jinja2
        ),
        voluptuous.Required("autosquash", default=True): bool,
    }
    executor_class = RebaseExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        {"or": ["sender-permission>=write", "sender={{author}}"]}
    ]

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        description = ":pushpin: rebase requirement"
        conds: list[conditions.RuleConditionNode] = [
            conditions.RuleCondition.from_tree(
                {"=": ("closed", False)},
                description=description,
            ),
        ]
        if not self.config["autosquash"]:
            conds.append(
                conditions.RuleConditionCombination(
                    {
                        "or": [
                            conditions.RuleCondition.from_tree(
                                {">": ("#commits-behind", 0)},
                                description=description,
                            ),
                            conditions.RuleCondition.from_tree(
                                {"=": ("linear-history", False)},
                                description=description,
                            ),
                        ],
                    }
                )
            )

        return conds
