import dataclasses
import datetime
import typing

import voluptuous

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.models.github import user as github_user
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import train_car
from mergify_engine.rules import types


GitHubLoginSchema = voluptuous.Schema(types.GitHubLogin)


@dataclasses.dataclass
class RenderBotAccountFailure(Exception):
    status: check_api.Conclusion
    title: str
    reason: str


class MissingMergeQueueCheckRun(Exception):
    pass


@typing.overload
async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: github_types.GitHubLogin,
    option_name: str = "bot_account",
) -> github_types.GitHubLogin:
    ...


@typing.overload
async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: None,
    option_name: str = "bot_account",
) -> github_types.GitHubLogin | None:
    ...


async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: github_types.GitHubLogin | None,
    option_name: str = "bot_account",
) -> github_types.GitHubLogin | None:
    if bot_account_template is None:
        if bot_account_fallback is None:
            return None

        bot_account = str(bot_account_fallback)
    else:
        try:
            bot_account = await ctxt.pull_request.render_template(bot_account_template)
        except context.RenderTemplateFailure as rmf:
            raise RenderBotAccountFailure(
                check_api.Conclusion.FAILURE,
                f"Invalid {option_name} template",
                str(rmf),
            )

    if bot_account:
        # NOTE(sileht): we strip in case of the template have an unperfect yaml
        # multiline ending or jinja2 eol layout
        bot_account = bot_account.strip()

    if not bot_account:
        return None

    if not bot_account.endswith("[bot]"):
        try:
            bot_account = GitHubLoginSchema(bot_account)
        except voluptuous.Invalid as e:
            raise RenderBotAccountFailure(
                check_api.Conclusion.FAILURE, f"Invalid {option_name} value", str(e)
            )

    return typing.cast(github_types.GitHubLogin, bot_account)


async def render_users_template(ctxt: context.Context, users: list[str]) -> set[str]:
    wanted = set()
    for user in set(users):
        try:
            user = await ctxt.pull_request.render_template(user)
        except context.RenderTemplateFailure:
            # NOTE: this should never happen since
            # the template is validated when parsing the config 🤷
            continue
        else:
            wanted.add(user)

    return wanted


@dataclasses.dataclass
class BotAccountNotFound(Exception):
    status: check_api.Conclusion
    title: str
    reason: str


@typing.overload
async def get_github_user_from_bot_account(
    repository: context.Repository,
    purpose: str,
    login: github_types.GitHubLogin,
    required_permissions: list[github_types.GitHubRepositoryPermission],
) -> github_user.GitHubUser:
    ...


@typing.overload
async def get_github_user_from_bot_account(
    repository: context.Repository,
    purpose: str,
    login: github_types.GitHubLogin | None,
    required_permissions: list[github_types.GitHubRepositoryPermission],
) -> github_user.GitHubUser | None:
    ...


async def get_github_user_from_bot_account(
    repository: context.Repository,
    purpose: str,
    login: github_types.GitHubLogin | None,
    required_permissions: list[github_types.GitHubRepositoryPermission],
) -> github_user.GitHubUser | None:
    if login is None:
        return None

    if login.endswith("[bot]"):
        raise BotAccountNotFound(
            check_api.Conclusion.FAILURE,
            f"Unable to {purpose}: GitHub App bot `{login}` can't be impersonated. ",
            "",
        )

    if required_permissions:
        try:
            user = await repository.installation.get_user(login)
            permission = await repository.get_user_permission(user)
        except http.HTTPNotFound:
            raise BotAccountNotFound(
                check_api.Conclusion.ACTION_REQUIRED,
                f"User `{login}` used as `bot_account` is unknown",
                f"Please make sure `{login}` exists and has logged into the [Mergify dashboard]({settings.DASHBOARD_UI_FRONT_URL}).",
            )

        if permission not in required_permissions:
            quoted_required_permissions = [f"`{p}`" for p in required_permissions]
            if len(quoted_required_permissions) == 1:
                fancy_perm = quoted_required_permissions[0]
            else:
                fancy_perm = ", ".join(quoted_required_permissions[0:-1])
                fancy_perm += f" or {quoted_required_permissions[-1]}"
            required_permissions[0:-1]
            # `write` or `maintain`
            raise BotAccountNotFound(
                check_api.Conclusion.ACTION_REQUIRED,
                (
                    f"`{login}` account used as "
                    f"`bot_account` must have {fancy_perm} permission, "
                    f"not `{permission}`"
                ),
                "",
            )

    if not settings.SAAS_MODE:
        for (
            hardcoded_id,
            hardcoded_login,
            hardcoded_oauth_access_token,
        ) in settings.ACCOUNT_TOKENS:
            if hardcoded_login.lower() == login.lower():
                return github_user.GitHubUser(
                    id=hardcoded_id,
                    login=hardcoded_login,
                    oauth_access_token=hardcoded_oauth_access_token.get_secret_value(),
                )

    async with database.create_session() as session:
        on_behalf = await github_user.GitHubUser.get_by_login(session, login)

    if on_behalf is None:
        raise BotAccountNotFound(
            check_api.Conclusion.FAILURE,
            f"Unable to {purpose}: user `{login}` is unknown. ",
            f"Please make sure `{login}` has logged in Mergify dashboard.",
        )

    return on_behalf


def get_invalid_credentials_report(
    on_behalf: github_user.GitHubUser,
) -> check_api.Result:
    return check_api.Result(
        check_api.Conclusion.ACTION_REQUIRED,
        f"User `{on_behalf.login}` used as `bot_account` as invalid credentials",
        f"Please make sure `{on_behalf.login}` logout and re-login into the [Mergify dashboard]({settings.DASHBOARD_UI_FRONT_URL}).",
    )


async def get_unqueue_reason_from_outcome(
    ctxt: context.Context, error_if_unknown: bool = True
) -> queue_utils.BaseUnqueueReason:
    from mergify_engine.queue.merge_train import train_car_state as tcs

    check = await ctxt.get_merge_queue_check_run()
    if check is None:
        raise MissingMergeQueueCheckRun(
            "get_unqueue_reason_from_outcome() called but check is not there"
        )

    if check["conclusion"] == "cancelled":
        # NOTE(sileht): should not be possible as unqueue command already
        # remove the pull request from the queue
        return queue_utils.PrDequeued(ctxt.pull["number"], " by an `unqueue` command")

    train_car_state = tcs.TrainCarStateForSummary.deserialize_from_summary(check)
    if (
        train_car_state is None
        or train_car_state.outcome == train_car.TrainCarOutcome.UNKNOWN
    ):
        is_check_outdated = check["completed_at"] and date.fromisoformat(
            check["completed_at"]
        ) < date.utcnow() - datetime.timedelta(days=7)

        # NOTE(charly): We just log details to not stuck a PR and handle it later
        if error_if_unknown and not is_check_outdated:
            ctxt.log.error(
                "Merge queue check doesn't contain any TrainCarState",
                check=check,
                train_car_state=train_car_state,
            )
        return queue_utils.PrDequeued(
            ctxt.pull["number"], " due to failing checks or checks timeout"
        )

    return tcs.unqueue_reason_from_train_car_state(train_car_state)
