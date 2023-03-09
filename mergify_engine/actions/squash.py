import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import squash_pull
from mergify_engine.actions import utils as action_utils
from mergify_engine.dashboard import subscription
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


class SquashExecutorConfig(typing.TypedDict):
    commit_message: typing.Literal["all-commits", "first-commit", "title+body"]
    bot_account: github_types.GitHubLogin


class SquashExecutor(actions.ActionExecutor["SquashAction", SquashExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "SquashAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "SquashExecutor":
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
                missing_feature_message="Squash with `bot_account` set is disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise prr_config.InvalidPullRequestRule(e.title, e.reason)

        return cls(
            ctxt,
            rule,
            SquashExecutorConfig(
                {
                    "commit_message": action.config["commit_message"],
                    "bot_account": bot_account,
                }
            ),
        )

    async def run(self) -> check_api.Result:
        if self.ctxt.pull["commits"] <= 1:
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Pull request is already one-commit long",
                "",
            )

        try:
            commit_title_and_message = await self.ctxt.pull_request.get_commit_message()
        except context.RenderTemplateFailure as rmf:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Invalid commit message",
                str(rmf),
            )

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                "squash", self.config["bot_account"]
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        if commit_title_and_message is not None:
            title, message = commit_title_and_message
            message = f"{title}\n\n{message}"

        elif self.config["commit_message"] == "all-commits":
            message = f"{(await self.ctxt.pull_request.title)} (#{(await self.ctxt.pull_request.number)})\n"
            message += "\n\n* ".join(
                [commit.commit_message for commit in await self.ctxt.commits]
            )

        elif self.config["commit_message"] == "first-commit":
            message = (await self.ctxt.commits)[0].commit_message

        elif self.config["commit_message"] == "title+body":
            message = f"{(await self.ctxt.pull_request.title)} (#{(await self.ctxt.pull_request.number)})"
            message += f"\n\n{await self.ctxt.pull_request.body}"

        else:
            raise RuntimeError("Unsupported commit_message option")

        try:
            await squash_pull.squash(self.ctxt, message, on_behalf=on_behalf)
        except squash_pull.SquashFailure as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE, "Pull request squash failed", e.reason
            )
        else:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                "action.squash",
                signals.EventNoMetadata(),
                self.rule.get_signal_trigger(),
            )
        return check_api.Result(
            check_api.Conclusion.SUCCESS, "Pull request squashed successfully", ""
        )

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT


class SquashAction(actions.Action):
    flags = (
        actions.ActionFlag.ALWAYS_RUN | actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
    )
    validator = {
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
        voluptuous.Required("commit_message", default="all-commits"): voluptuous.Any(
            "all-commits", "first-commit", "title+body"
        ),
    }
    executor_class = SquashExecutor

    default_restrictions: typing.ClassVar[list[typing.Any]] = [
        {"or": ["sender-permission>=write", "sender={{author}}"]}
    ]

    @staticmethod
    def command_to_config(string: str) -> dict[str, typing.Any]:
        if string:
            return {"commit_message": string.strip()}
        else:
            return {}
