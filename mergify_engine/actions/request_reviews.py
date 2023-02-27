from __future__ import annotations

import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.actions import utils as action_utils
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription
from mergify_engine.dashboard import user_tokens
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


ReviewEntityWithWeightT = dict[types.GitHubLogin, int] | dict[types.GitHubTeam, int]
ReviewEntityT = list[types.GitHubLogin] | list[types.GitHubTeam]


def _ensure_weight(entities: ReviewEntityT) -> ReviewEntityWithWeightT:
    return {entity: 1 for entity in entities}


class RequestReviewsExecutorConfig(typing.TypedDict):
    bot_account: user_tokens.UserTokensUser | None
    users: dict[github_types.GitHubLogin, int]
    teams: dict[github_types.GitHubTeamSlug, int]
    random_count: int | None


class RequestReviewsExecutor(
    actions.ActionExecutor["RequestReviewsAction", RequestReviewsExecutorConfig]
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
                required_feature=subscription.Features.BOT_ACCOUNT,
                missing_feature_message="Request reviews with `bot_account` set are disabled",
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
                    f"Unable to request review: user `{bot_account}` is unknown. ",
                    f"Please make sure `{bot_account}` has logged in Mergify dashboard.",
                )

        if action.config[
            "random_count"
        ] is not None and not ctxt.subscription.has_feature(
            subscription.Features.RANDOM_REQUEST_REVIEWS
        ):
            raise prr_config.InvalidPullRequestRule(
                "Random request reviews are disabled",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        team_errors = set()
        for team in action.config["teams"].keys():
            try:
                await team.has_read_permission(ctxt)
            except types.InvalidTeam as e:
                team_errors.add(e.details)

        users = action.config["users"].copy()
        for team, weight in action.config["users_from_teams"].items():
            try:
                await team.has_read_permission(ctxt)
            except types.InvalidTeam as e:
                team_errors.add(e.details)
            else:
                users.update(
                    {
                        user: weight
                        for user in await ctxt.repository.installation.get_team_members(
                            team.team
                        )
                    }
                )

        if team_errors:
            raise prr_config.InvalidPullRequestRule(
                "Invalid requested teams",
                "\n".join(team_errors),
            )

        return cls(
            ctxt,
            rule,
            RequestReviewsExecutorConfig(
                {
                    "bot_account": github_user,
                    "users": users,
                    "teams": {
                        team.team: weight
                        for team, weight in typing.cast(
                            dict[types._GitHubTeam, int], action.config["teams"]
                        ).items()
                    },
                    "random_count": action.config["random_count"],
                }
            ),
        )

    def _get_random_reviewers(
        self, random_count: int, random_number: int, pr_author: str
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
        self, pr_id: int, existing_reviews: set[str], pr_author: str
    ) -> tuple[set[str], set[str]]:
        if self.config["random_count"] is None:
            user_reviews_to_request = {
                user.lower() for user in self.config["users"].keys()
            }
            team_reviews_to_request = {t.lower() for t in self.config["teams"].keys()}
        else:
            team_reviews_to_request = set()
            user_reviews_to_request = set()

            for reviewer in self._get_random_reviewers(
                self.config["random_count"], pr_id, pr_author
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
        existing_reviews = {
            user.lower()
            for key in reviews_keys
            for user in await getattr(self.ctxt.pull_request, key)
        }

        user_reviews_to_request, team_reviews_to_request = self._get_reviewers(
            self.ctxt.pull["id"],
            existing_reviews,
            self.ctxt.pull["user"]["login"],
        )

        if user_reviews_to_request or team_reviews_to_request:
            requested_reviews_nb = len(
                typing.cast(list[str], await self.ctxt.pull_request.review_requested)
            )

            already_at_max = requested_reviews_nb == self.GITHUB_MAXIMUM_REVIEW_REQUEST
            will_exceed_max = (
                len(user_reviews_to_request)
                + len(team_reviews_to_request)
                + requested_reviews_nb
                > self.GITHUB_MAXIMUM_REVIEW_REQUEST
            )

            if not already_at_max:
                try:
                    await self.ctxt.client.post(
                        f"{self.ctxt.base_url}/pulls/{self.ctxt.pull['number']}/requested_reviewers",
                        oauth_token=self.config["bot_account"]["oauth_access_token"]
                        if self.config["bot_account"]
                        else None,
                        json={
                            "reviewers": list(user_reviews_to_request),
                            "team_reviewers": list(team_reviews_to_request),
                        },
                    )
                except http.HTTPClientSideError as e:  # pragma: no cover
                    return check_api.Result(
                        check_api.Conclusion.PENDING,
                        "Unable to create review request",
                        f"GitHub error: [{e.status_code}] `{e.message}`",
                    )
                await signals.send(
                    self.ctxt.repository,
                    self.ctxt.pull["number"],
                    "action.request_reviewers",
                    signals.EventRequestReviewsMetadata(
                        {
                            "reviewers": list(user_reviews_to_request),
                            "team_reviewers": list(team_reviews_to_request),
                        }
                    ),
                    self.rule.get_signal_trigger(),
                )

            if already_at_max or will_exceed_max:
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    "Maximum number of reviews already requested",
                    f"The maximum number of {self.GITHUB_MAXIMUM_REVIEW_REQUEST} reviews has been reached.\n"
                    "Unable to request reviews for additional users.",
                )

            return check_api.Result(
                check_api.Conclusion.SUCCESS, "New reviews requested", ""
            )
        else:
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "No new reviewers to request", ""
            )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class RequestReviewsAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN

    _random_weight = voluptuous.Required(
        voluptuous.All(int, voluptuous.Range(min=1, max=65535)), default=1
    )

    validator = {
        voluptuous.Required("users", default=list): voluptuous.Any(
            voluptuous.All(
                [types.GitHubLogin],
                voluptuous.Coerce(_ensure_weight),
            ),
            {
                types.GitHubLogin: _random_weight,
            },
        ),
        voluptuous.Required("teams", default=list): voluptuous.Any(
            voluptuous.All(
                [types.GitHubTeam],
                voluptuous.Coerce(_ensure_weight),
            ),
            {
                types.GitHubTeam: _random_weight,
            },
        ),
        voluptuous.Required("users_from_teams", default=list): voluptuous.Any(
            voluptuous.All(
                [types.GitHubTeam],
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
                    1, RequestReviewsExecutor.GITHUB_MAXIMUM_REVIEW_REQUEST
                ),
            ),
            None,
        ),
        voluptuous.Required("bot_account", default=None): types.Jinja2WithNone,
    }

    executor_class = RequestReviewsExecutor
