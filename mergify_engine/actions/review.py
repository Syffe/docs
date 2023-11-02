from __future__ import annotations

import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context

EVENT_STATE_MAP = {
    "APPROVE": "APPROVED",
    "REQUEST_CHANGES": "CHANGES_REQUESTED",
    "COMMENT": "COMMENTED",
}


class ReviewExecutorConfig(typing.TypedDict):
    type: github_types.GitHubReviewStateChangeType
    message: str | None
    bot_account: github_types.GitHubLogin | None


class ReviewExecutor(actions.ActionExecutor["ReviewAction", ReviewExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: ReviewAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> ReviewExecutor:
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=None,
            )
        except action_utils.RenderBotAccountFailure as e:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, e.title, e.reason
            )

        if action.config["message"]:
            pull_attrs = condition_value_querier.PullRequest(ctxt)
            try:
                message = await pull_attrs.render_template(action.config["message"])
            except condition_value_querier.RenderTemplateFailure as rmf:
                raise actions.InvalidDynamicActionConfiguration(
                    rule, action, "Invalid review message", str(rmf)
                )
        else:
            message = None

        return cls(
            ctxt,
            rule,
            ReviewExecutorConfig(
                {
                    "message": message,
                    "type": action.config["type"],
                    "bot_account": bot_account,
                }
            ),
        )

    async def run(self) -> check_api.Result:
        payload = github_types.GitHubReviewPost({"event": self.config["type"]})

        try:
            on_behalf = await action_utils.get_github_user_from_bot_account(
                self.ctxt.repository,
                "review",
                self.config["bot_account"],
                required_permissions=[],
            )
        except action_utils.BotAccountNotFound as e:
            return check_api.Result(e.status, e.title, e.reason)

        if self.ctxt.pull["merged"] and self.config["type"] != "COMMENT":
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Pull request has been merged, APPROVE and REQUEST_CHANGES are ignored.",
                "",
            )

        if on_behalf is None:
            mergify_bot = await github.GitHubAppInfo.get_bot(
                self.ctxt.repository.installation.redis.cache
            )
            review_user_id = mergify_bot["id"]
        else:
            review_user_id = on_behalf.id

        reviews = reversed(
            list(
                filter(
                    lambda r: r["user"] is not None
                    and r["user"]["id"] == review_user_id,
                    await self.ctxt.reviews,
                )
            )
        )

        if self.config["message"]:
            payload["body"] = self.config["message"]
        elif self.config["type"] != "APPROVE":
            payload[
                "body"
            ] = f"Pull request automatically reviewed by Mergify: {self.config['type']}"

        for review in reviews:
            if (
                review["body"] == payload.get("body", "")
                and review["state"] == EVENT_STATE_MAP[self.config["type"]]
            ):
                # Already posted
                return check_api.Result(
                    check_api.Conclusion.SUCCESS, "Review already posted", ""
                )

            if (
                self.config["type"] == "REQUEST_CHANGES"
                and review["state"] == "APPROVED"
            ):
                break

            if (
                self.config["type"] == "APPROVE"
                and review["state"] == "CHANGES_REQUESTED"
            ):
                break

        try:
            response = await self.ctxt.client.post(
                f"{self.ctxt.base_url}/pulls/{self.ctxt.pull['number']}/reviews",
                oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                json=payload,
            )
        except http.HTTPUnauthorized:
            if on_behalf is None:
                raise
            return action_utils.get_invalid_credentials_report(on_behalf)
        except http.HTTPClientSideError as e:
            if e.status_code == 422 and "errors" in e.response.json():
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "Review failed",
                    "GitHub returned an unexpected error:\n\n * "
                    + "\n * ".join(f"`{s}`" for s in e.response.json()["errors"]),
                )

            if e.status_code == 404 and on_behalf is not None:
                # NOTE(sileht): If the oauth token is valid but the user is not
                # allowed access this repository GitHub returns 404 for private
                # repository instead of 403.
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "Review failed",
                    f"GitHub account `{on_behalf.login}` is not "
                    "allowed to review pull requests of this repository",
                )

            raise

        review = typing.cast(github_types.GitHubReview, response.json())

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.review",
            signals.EventReviewMetadata(
                {
                    "review_type": self.config["type"],
                    "reviewer": (review["user"] and review["user"]["login"]) or None,
                    "message": payload.get("body"),
                }
            ),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(check_api.Conclusion.SUCCESS, "Review posted", "")

    async def cancel(self) -> check_api.Result:
        return actions.CANCELLED_CHECK_REPORT


class ReviewAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN
    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("type", default="APPROVE"): voluptuous.Any(
            *github_types.GitHubReviewStateChangeType.__args__  # type: ignore[attr-defined]
        ),
        voluptuous.Required("message", default=None): types.Jinja2WithNone,
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
    }
    executor_class = ReviewExecutor
