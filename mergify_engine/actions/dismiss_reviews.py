from __future__ import annotations

import logging
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import pull_request_getter
from mergify_engine import signals
from mergify_engine.clients import http
from mergify_engine.rules import types


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config

DismissReviewWhenT = typing.Literal["synchronize", "always"]
WHEN_SYNCHRONIZE: DismissReviewWhenT = "synchronize"
WHEN_ALWAYS: DismissReviewWhenT = "always"

DismissReviewTypeT = (
    typing.Literal[True, False, "from_requested_reviewers"]
    | list[github_types.GitHubLogin]
)

FROM_REQUESTED_REVIEWERS: DismissReviewTypeT = "from_requested_reviewers"

DEFAULT_MESSAGE = {
    WHEN_SYNCHRONIZE: "Pull request has been modified.",
    WHEN_ALWAYS: "Automatic dismiss reviews requested",
}


class DismissReviewsExecutorConfig(typing.TypedDict):
    approved: DismissReviewTypeT
    changes_requested: DismissReviewTypeT
    message: str | None
    when: DismissReviewWhenT


class DismissReviewsExecutor(
    actions.ActionExecutor["DismissReviewsAction", DismissReviewsExecutorConfig],
):
    @classmethod
    async def create(
        cls,
        action: DismissReviewsAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> DismissReviewsExecutor:
        if action.config["message"] is None:
            message_raw = DEFAULT_MESSAGE[action.config["when"]]
        else:
            message_raw = typing.cast(str, action.config["message"])

        pull_attrs = condition_value_querier.PullRequest(ctxt)
        try:
            message = await pull_attrs.render_template(message_raw)
        except condition_value_querier.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                "Invalid dismiss reviews message",
                str(rmf),
            )

        return cls(
            ctxt,
            rule,
            DismissReviewsExecutorConfig(
                {
                    "message": message,
                    "approved": action.config["approved"],
                    "changes_requested": action.config["changes_requested"],
                    "when": action.config["when"],
                },
            ),
        )

    async def run(self) -> check_api.Result:
        if (
            self.config["when"] == WHEN_SYNCHRONIZE
            and not self.ctxt.has_been_synchronized()
        ):
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Nothing to do, pull request has not been synchronized",
                "",
            )

        # FIXME(sileht): Currently sender id is not the bot by the admin
        # user that enroll the repo in Mergify, because branch_updater uses
        # his access_token instead of the Mergify installation token.
        # As workaround we track in redis merge commit id
        # This is only true for method="rebase"
        if self.config["when"] == WHEN_SYNCHRONIZE:
            last_user_sync = await self.ctxt.synchronized_by_user_at()
            if last_user_sync is None:
                return check_api.Result(
                    check_api.Conclusion.SUCCESS,
                    "Updated by Mergify, ignoring",
                    "",
                )
        else:
            last_user_sync = None

        requested_reviewers_login = {
            rr["login"] for rr in self.ctxt.pull["requested_reviewers"]
        }

        to_dismiss = set()
        to_dismiss_users = set()
        to_dismiss_user_from_requested_reviewers = set()
        for review in (await self.ctxt.consolidated_reviews())[1]:
            if self.config["when"] == WHEN_SYNCHRONIZE and last_user_sync is not None:
                submitted_at = date.fromisoformat(review["submitted_at"])
                if submitted_at > last_user_sync:
                    # NOTE(sileht): we ignore review done after the sync
                    continue

            conf = self.config.get(review["state"].lower(), False)
            if conf is True:
                to_dismiss.add(review["id"])
                if review["user"] is not None:
                    to_dismiss_users.add(review["user"]["login"])
            elif conf == FROM_REQUESTED_REVIEWERS:
                if (
                    review["user"] is not None
                    and review["user"]["login"] in requested_reviewers_login
                ):
                    to_dismiss.add(review["id"])
                    to_dismiss_users.add(review["user"]["login"])
                    to_dismiss_user_from_requested_reviewers.add(
                        review["user"]["login"],
                    )
            elif isinstance(conf, list):
                if review["user"] is not None and review["user"]["login"] in conf:
                    to_dismiss_users.add(review["user"]["login"])
                    to_dismiss.add(review["id"])

        if not to_dismiss:
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Nothing to dismiss",
                "",
            )

        if (
            self.config.get("approved") == FROM_REQUESTED_REVIEWERS
            and to_dismiss_user_from_requested_reviewers
        ):
            updated_pull = await pull_request_getter.get_pull_request(
                self.ctxt.client,
                self.ctxt.pull["number"],
                repo_owner=self.ctxt.repo_owner_login,
                repo_name=self.ctxt.repo_name,
            )
            updated_requested_reviewers_login = {
                rr["login"] for rr in updated_pull["requested_reviewers"]
            }
            if updated_requested_reviewers_login != requested_reviewers_login:
                # Query the PR from GitHub to make sure that the problem also appears
                # with the latest version of the PR.
                updated_pull = await pull_request_getter.get_pull_request(
                    self.ctxt.client,
                    self.ctxt.pull["number"],
                    repo_owner=self.ctxt.repo_owner_login,
                    repo_name=self.ctxt.repo_name,
                    force_new=True,
                )
                updated_requested_reviewers_login = {
                    rr["login"] for rr in updated_pull["requested_reviewers"]
                }

            level = (
                logging.ERROR
                if updated_requested_reviewers_login != requested_reviewers_login
                else logging.INFO
            )
            self.ctxt.log.log(
                level,
                "about to dismiss approval reviews from requested_reviewers",
                requested_reviewers_login=requested_reviewers_login,
                updated_requested_reviewers_login=updated_requested_reviewers_login,
                to_dismiss_user_from_requested_reviewers=to_dismiss_user_from_requested_reviewers,
            )

        errors = set()
        for review_id in to_dismiss:
            try:
                await self.ctxt.client.put(
                    f"{self.ctxt.base_url}/pulls/{self.ctxt.pull['number']}/reviews/{review_id}/dismissals",
                    json={"message": self.config["message"]},
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                errors.add(f"GitHub error: [{e.status_code}] `{e.message}`")

        if errors:
            return check_api.Result(
                check_api.Conclusion.PENDING,
                "Unable to dismiss review",
                "\n".join(errors),
            )

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.ctxt.pull["base"]["ref"],
            "action.dismiss_reviews",
            signals.EventDismissReviewsMetadata({"users": list(to_dismiss_users)}),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(check_api.Conclusion.SUCCESS, "Review dismissed", "")

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class DismissReviewsAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN

    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("approved", default=True): voluptuous.Any(
            True,
            False,
            [types.GitHubLogin],
            FROM_REQUESTED_REVIEWERS,
        ),
        voluptuous.Required("changes_requested", default=True): voluptuous.Any(
            True,
            False,
            [types.GitHubLogin],
            FROM_REQUESTED_REVIEWERS,
        ),
        voluptuous.Required("message", default=None): types.Jinja2WithNone,
        voluptuous.Required("when", default=WHEN_SYNCHRONIZE): voluptuous.Any(
            WHEN_SYNCHRONIZE,
            WHEN_ALWAYS,
        ),
    }

    executor_class = DismissReviewsExecutor
