from __future__ import annotations

import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.rules import types


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config

ReviewEntityWithWeightT = dict[types.GitHubLogin, int] | dict[types.GitHubTeam, int]
ReviewEntityT = list[types.GitHubLogin] | list[types.GitHubTeam]


def _ensure_weight(entities: ReviewEntityT) -> ReviewEntityWithWeightT:
    return {entity: 1 for entity in entities}


class RequestReviewsExecutorConfig(typing.TypedDict):
    bot_account: github_types.GitHubLogin | None
    users: dict[github_types.GitHubLogin, int]
    teams: dict[github_types.GitHubTeamSlug, int]
    random_count: int | None


class RequestReviewsExecutor(
    actions.ActionExecutor["RequestReviewsAction", RequestReviewsExecutorConfig],
):
    # This is the maximum number of review you can request on a PR.
    # It's not documented in the API, but it is shown in GitHub UI.
    # Any review passed that number is ignored by GitHub API.
    GITHUB_MAXIMUM_REVIEW_REQUEST = 15

    @classmethod
    async def create(
        cls,
        action: RequestReviewsAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> RequestReviewsExecutor:
        try:
            bot_account = await action_utils.render_bot_account(
                ctxt,
                action.config["bot_account"],
                bot_account_fallback=None,
            )
        except action_utils.RenderBotAccountFailureError as e:
            raise actions.InvalidDynamicActionConfigurationError(
                rule,
                action,
                e.title,
                e.reason,
            )

        team_errors = set()
        for team in action.config["teams"]:
            try:
                await team.has_read_permission(ctxt)
            except types.InvalidTeamError as e:
                team_errors.add(e.details)

        users = action.config["users"].copy()
        for team, weight in action.config["users_from_teams"].items():
            try:
                await team.has_read_permission(ctxt)
            except types.InvalidTeamError as e:
                team_errors.add(e.details)
            else:
                users.update(
                    {
                        user: weight
                        for user in await ctxt.repository.installation.get_team_members(
                            team.team,
                        )
                    },
                )

        if team_errors:
            raise actions.InvalidDynamicActionConfigurationError(
                rule,
                action,
                "Invalid requested teams",
                "\n".join(team_errors),
            )

        return cls(
            ctxt,
            rule,
            RequestReviewsExecutorConfig(
                {
                    "bot_account": bot_account,
                    "users": users,
                    "teams": {
                        team.team: weight
                        for team, weight in typing.cast(
                            dict[types._GitHubTeam, int],
                            action.config["teams"],
                        ).items()
                    },
                    "random_count": action.config["random_count"],
                },
            ),
        )

    def _get_random_reviewers(
        self,
        random_count: int,
        random_number: int,
        pr_author: str,
    ) -> set[str]:
        choices = {
            **{user.lower(): weight for user, weight in self.config["users"].items()},
            **{
                f"@{team.lower()}": weight
                for team, weight in self.config["teams"].items()
            },
        }

        try:
            del choices[pr_author.lower()]
        except KeyError:
            pass

        count = min(random_count, len(choices))

        return utils.get_random_choices(
            random_number,
            choices,
            count,
        )

    def _get_reviewers(
        self,
        pr_id: int,
        existing_reviews: set[str],
        pr_author: str,
    ) -> tuple[set[str], set[str]]:
        if self.config["random_count"] is None:
            user_reviews_to_request = {user.lower() for user in self.config["users"]}
            team_reviews_to_request = {t.lower() for t in self.config["teams"]}
        else:
            team_reviews_to_request = set()
            user_reviews_to_request = set()

            for reviewer in self._get_random_reviewers(
                self.config["random_count"],
                pr_id,
                pr_author,
            ):
                if reviewer.startswith("@"):
                    team_reviews_to_request.add(reviewer[1:].lower())
                else:
                    user_reviews_to_request.add(reviewer.lower())
        user_reviews_to_request -= existing_reviews
        user_reviews_to_request -= {pr_author.lower()}

        # Team starts with @
        team_reviews_to_request -= {
            e[1:] for e in existing_reviews if e.startswith("@")
        }

        return user_reviews_to_request, team_reviews_to_request

    async def run(self) -> check_api.Result:
        # Using consolidated data to avoid already done API lookup
        reviews_keys = (
            "approved-reviews-by",
            "dismissed-reviews-by",
            "changes-requested-reviews-by",
            "commented-reviews-by",
            "review-requested",
        )
        pull_attrs = condition_value_querier.PullRequest(self.ctxt)
        existing_reviews = {
            user.lower()
            for key in reviews_keys
            for user in typing.cast(
                list[str],
                await pull_attrs.get_attribute_value(key),
            )
        }

        user_reviews_to_request_set, team_reviews_to_request_set = self._get_reviewers(
            self.ctxt.pull["id"],
            existing_reviews,
            self.ctxt.pull["user"]["login"],
        )
        user_reviews_to_request = list(user_reviews_to_request_set)
        team_reviews_to_request = list(team_reviews_to_request_set)

        if user_reviews_to_request or team_reviews_to_request:
            requested_reviews_nb = len(
                typing.cast(
                    list[str],
                    await pull_attrs.get_attribute_value("review_requested"),
                ),
            )

            already_at_max = requested_reviews_nb == self.GITHUB_MAXIMUM_REVIEW_REQUEST
            will_exceed_max = (
                len(user_reviews_to_request)
                + len(team_reviews_to_request)
                + requested_reviews_nb
                > self.GITHUB_MAXIMUM_REVIEW_REQUEST
            )

            if already_at_max:
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    "Maximum number of reviews already requested",
                    f"The maximum number of {self.GITHUB_MAXIMUM_REVIEW_REQUEST} reviews has been reached.\n"
                    "Unable to request reviews for additional users.",
                )

            if will_exceed_max:
                max_number_of_reviews = (
                    self.GITHUB_MAXIMUM_REVIEW_REQUEST - requested_reviews_nb
                )
                if max_number_of_reviews < len(user_reviews_to_request):
                    user_reviews_to_request = user_reviews_to_request[
                        :max_number_of_reviews
                    ]

                max_number_of_reviews -= len(user_reviews_to_request)
                if max_number_of_reviews > 0:
                    team_reviews_to_request = team_reviews_to_request[
                        :max_number_of_reviews
                    ]
                else:
                    team_reviews_to_request = []

            try:
                on_behalf = await action_utils.get_github_user_from_bot_account(
                    self.ctxt.repository,
                    "request review",
                    self.config["bot_account"],
                    required_permissions=[],
                )
            except action_utils.BotAccountNotFoundError as e:
                return check_api.Result(e.status, e.title, e.reason)

            try:
                await self.ctxt.client.post(
                    f"{self.ctxt.base_url}/pulls/{self.ctxt.pull['number']}/requested_reviewers",
                    oauth_token=on_behalf.oauth_access_token if on_behalf else None,
                    json={
                        "reviewers": user_reviews_to_request,
                        "team_reviewers": team_reviews_to_request,
                    },
                )
            except http.HTTPUnauthorizedError:
                if on_behalf is None:
                    raise
                return action_utils.get_invalid_credentials_report(on_behalf)
            except http.HTTPClientSideError as e:  # pragma: no cover
                return check_api.Result(
                    check_api.Conclusion.FAILURE,
                    "Unable to create review request",
                    f"GitHub error: [{e.status_code}] `{e.message}`",
                )

            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                self.ctxt.pull["base"]["ref"],
                "action.request_reviews",
                signals.EventRequestReviewsMetadata(
                    {
                        "reviewers": user_reviews_to_request,
                        "team_reviewers": team_reviews_to_request,
                    },
                ),
                self.rule.get_signal_trigger(),
            )

            if will_exceed_max:
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    "Maximum number of reviews already requested",
                    f"The maximum number of {self.GITHUB_MAXIMUM_REVIEW_REQUEST} reviews has been reached.\n"
                    "Unable to request reviews for additional users.",
                )

            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "New reviews requested",
                "",
            )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "No new reviewers to request",
            "",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class RequestReviewsAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN

    _random_weight = voluptuous.Required(
        voluptuous.All(int, voluptuous.Range(min=1, max=65535)),
        default=1,
    )

    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("users", default=list): voluptuous.Any(
            voluptuous.All(
                types.ListOf(
                    types.GitHubLogin,
                    RequestReviewsExecutor.GITHUB_MAXIMUM_REVIEW_REQUEST * 10,
                ),
                voluptuous.Coerce(_ensure_weight),
            ),
            {
                types.GitHubLogin: _random_weight,
            },
        ),
        voluptuous.Required("teams", default=list): voluptuous.Any(
            voluptuous.All(
                types.ListOf(
                    types.GitHubTeam,
                    RequestReviewsExecutor.GITHUB_MAXIMUM_REVIEW_REQUEST * 10,
                ),
                voluptuous.Coerce(_ensure_weight),
            ),
            {
                types.GitHubTeam: _random_weight,
            },
        ),
        voluptuous.Required("users_from_teams", default=list): voluptuous.Any(
            voluptuous.All(
                types.ListOf(
                    types.GitHubTeam,
                    RequestReviewsExecutor.GITHUB_MAXIMUM_REVIEW_REQUEST * 10,
                ),
                voluptuous.Coerce(_ensure_weight),
            ),
            {
                types.GitHubTeam: _random_weight,
            },
        ),
        voluptuous.Required("random_count", default=None): voluptuous.Any(
            voluptuous.All(
                int,
                voluptuous.Range(
                    1,
                    RequestReviewsExecutor.GITHUB_MAXIMUM_REVIEW_REQUEST,
                ),
            ),
            None,
        ),
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
    }

    executor_class = RequestReviewsExecutor
