import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import branch_updater
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import conditions
from mergify_engine.rules import types


class RebaseExecutorConfig(typing.TypedDict):
    autosquash: bool
    bot_account: user_tokens.UserTokensUser | None


class RebaseExecutor(actions.ActionExecutor["RebaseAction", RebaseExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "RebaseAction",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "RebaseExecutor":
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Comments with `bot_account` set are disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise rules.InvalidPullRequestRule(e.title, e.reason)

        github_user: user_tokens.UserTokensUser | None = None
        if bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                raise rules.InvalidPullRequestRule(
                    f"Unable to rebase: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )

        return cls(
            ctxt,
            rule,
            RebaseExecutorConfig(
                {"bot_account": github_user, "autosquash": action.config["autosquash"]}
            ),
        )

    async def run(self) -> check_api.Result:
        if self.config[
            "bot_account"
        ] is not None and self.ctxt.subscription.has_feature(
            subscription.Features.BOT_ACCOUNT
        ):
            users = [self.config["bot_account"]]
            committer = self.config["bot_account"]
        else:
            tokens = await self.ctxt.repository.installation.get_user_tokens()
            users = tokens.users
            committer = None

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
            await branch_updater.rebase_with_git(
                self.ctxt,
                users,
                committer,
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
        actions.ActionFlag.ALWAYS_RUN
        | actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        | actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
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
            conditions.RuleCondition(
                "-closed",
                description=description,
            ),
        ]
        if not self.config["autosquash"]:
            conds.append(
                conditions.RuleConditionCombination(
                    {
                        "or": [
                            conditions.RuleCondition(
                                "#commits-behind>0",
                                description=description,
                            ),
                            conditions.RuleCondition(
                                "-linear-history",
                                description=description,
                            ),
                        ],
                    }
                )
            )

        return conds
