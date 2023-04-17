import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import github
from mergify_engine.dashboard import subscription
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


class EditExecutorConfig(typing.TypedDict):
    draft: bool | None
    bot_account: github_types.GitHubLogin | None


class EditExecutor(actions.ActionExecutor["EditAction", EditExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: "EditAction",
        ctxt: "context.Context",
        rule: "prr_config.EvaluatedPullRequestRule",
    ) -> "EditExecutor":
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=None,
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Edit with `bot_account` set is disabled",
                required_permissions=[],
            )
        except action_utils.RenderBotAccountFailure as e:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, e.title, e.reason
            )

        return cls(
            ctxt,
            rule,
            EditExecutorConfig(
                {"draft": action.config["draft"], "bot_account": bot_account}
            ),
        )

    async def run(self) -> check_api.Result:
        if self.config["draft"] is None:
            return check_api.Result(check_api.Conclusion.SUCCESS, "Nothing to do.", "")

        if self.ctxt.closed:
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Nothing to do, the pull request is closed.",
                "",
            )

        if self.config["draft"]:
            expected_state = True
            current_state = "draft"
            mutation = "convertPullRequestToDraft"
        else:
            expected_state = False
            current_state = "ready for review"
            mutation = "markPullRequestReadyForReview"

        if self.ctxt.pull["draft"] == expected_state:
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                f"Pull request is already {current_state}.",
                "",
            )

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                "edit", self.config["bot_account"]
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        mutation = f"""
            mutation {{
                {mutation}(input:{{pullRequestId: "{self.ctxt.pull['node_id']}"}}) {{
                    pullRequest {{
                        isDraft
                    }}
                }}
            }}
        """
        try:
            await self.ctxt.client.graphql_post(
                mutation,
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
            )
        except github.GraphqlError as e:
            if "Field 'convertPullRequestToDraft' doesn't exist" in e.message:
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "Converting pull request to draft requires GHES >= 3.2",
                    "",
                )
            self.ctxt.log.error(
                "GraphQL API call failed, unable to convert PR.",
                current_state=current_state,
                response=e.message,
            )
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                f"GraphQL API call failed, pull request wasn't converted to {current_state}.",
                "",
            )
        self.ctxt.pull["draft"] = expected_state

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.edit",
            signals.EventEditMetadata({"draft": expected_state}),
            self.rule.get_signal_trigger(),
        )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            f"Pull request successfully converted to {current_state}",
            "",
        )

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT


class EditAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN

    validator = {
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
        voluptuous.Required("draft", default=None): voluptuous.Any(None, bool),
    }

    executor_class = EditExecutor
