import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import signals
from mergify_engine import squash_pull
from mergify_engine.actions import utils as action_utils
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


class SquashExecutorConfig(typing.TypedDict):
    commit_message: typing.Literal["all-commits", "first-commit", "title+body"]
    bot_account: user_tokens.UserTokensUser | None


class SquashExecutor(actions.ActionExecutor["SquashAction", SquashExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "SquashAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "SquashExecutor":
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Squash with `bot_account` set is disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise prr_config.InvalidPullRequestRule(e.title, e.reason)

        github_user: user_tokens.UserTokensUser | None = None

        if bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                raise prr_config.InvalidPullRequestRule(
                    f"Unable to edit: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )
        else:
            github_user = None

        return cls(
            ctxt,
            rule,
            SquashExecutorConfig(
                {
                    "commit_message": action.config["commit_message"],
                    "bot_account": github_user,
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
            await squash_pull.squash(
                self.ctxt,
                message,
                on_behalf=self.config["bot_account"],
            )
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
        voluptuous.Required("bot_account", default=None): voluptuous.Any(
            None, types.Jinja2
        ),
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
