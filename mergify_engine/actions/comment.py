import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


class CommentExecutorConfig(typing.TypedDict):
    message: str | None
    bot_account: user_tokens.UserTokensUser | None


class CommentExecutor(actions.ActionExecutor["CommentAction", "CommentExecutorConfig"]):
    @classmethod
    async def create(
        cls,
        action: "CommentAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "CommentExecutor":
        if action.config["message"] is None:
            # Happens when the config for a comment action is just `None` and
            # there is no "defaults" for the comment action.
            raise prr_config.InvalidPullRequestRule(
                "Cannot have `comment` action with no `message`",
                str(action.config),
            )

        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=None,
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Comments with `bot_account` set are disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise prr_config.InvalidPullRequestRule(e.title, e.reason)

        try:
            message = await ctxt.pull_request.render_template(action.config["message"])
        except context.RenderTemplateFailure as rmf:
            raise prr_config.InvalidPullRequestRule(
                "Invalid comment message",
                str(rmf),
            )

        github_user: user_tokens.UserTokensUser | None = None
        if bot_account:
            tokens = await ctxt.repository.installation.get_user_tokens()
            github_user = tokens.get_token_for(bot_account)
            if not github_user:
                raise prr_config.InvalidPullRequestRule(
                    f"Unable to comment: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )
        return cls(
            ctxt,
            rule,
            CommentExecutorConfig({"message": message, "bot_account": github_user}),
        )

    async def run(self) -> check_api.Result:
        if self.config["message"] is None:
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Message is empty", ""
            )

        try:
            await self.ctxt.client.post(
                f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/comments",
                oauth_token=self.config["bot_account"]["oauth_access_token"]
                if self.config["bot_account"]
                else None,
                json={"body": self.config["message"]},
            )
        except http.HTTPClientSideError as e:  # pragma: no cover
            return check_api.Result(
                check_api.Conclusion.PENDING,
                "Unable to post comment",
                f"GitHub error: [{e.status_code}] `{e.message}`",
            )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.comment",
            signals.EventCommentMetadata(message=self.config["message"]),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS, "Comment posted", self.config["message"]
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class CommentAction(actions.Action):
    flags = actions.ActionFlag.NONE
    validator = {
        # FIXME(sileht): we shouldn't allow None here, we should raise an error
        # or set a default message.
        voluptuous.Required("message", default=None): types.Jinja2WithNone,
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
    }
    executor_class = CommentExecutor
