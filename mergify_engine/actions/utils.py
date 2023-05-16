import dataclasses
import typing

import voluptuous

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.models import github_user
from mergify_engine.rules import types


GitHubLoginSchema = voluptuous.Schema(types.GitHubLogin)


@dataclasses.dataclass
class RenderBotAccountFailure(Exception):
    status: check_api.Conclusion
    title: str
    reason: str


@typing.overload
async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: github_types.GitHubLogin,
    option_name: str = "bot_account",
    required_permissions: None | (list[github_types.GitHubRepositoryPermission]) = None,
) -> github_types.GitHubLogin:
    ...


@typing.overload
async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: None,
    option_name: str = "bot_account",
    required_permissions: None | (list[github_types.GitHubRepositoryPermission]) = None,
) -> github_types.GitHubLogin | None:
    ...


async def render_bot_account(
    ctxt: context.Context,
    bot_account_template: str | None,
    *,
    bot_account_fallback: github_types.GitHubLogin | None,
    option_name: str = "bot_account",
    required_permissions: None | (list[github_types.GitHubRepositoryPermission]) = None,
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

    try:
        bot_account = typing.cast(
            github_types.GitHubLogin, GitHubLoginSchema(bot_account)
        )
    except voluptuous.Invalid as e:
        raise RenderBotAccountFailure(
            check_api.Conclusion.FAILURE,
            f"Invalid {option_name} value",
            str(e),
        )

    if required_permissions is None:
        required_permissions = (
            github_types.GitHubRepositoryPermission.permissions_above(
                github_types.GitHubRepositoryPermission.WRITE
            )
        )

    if required_permissions:
        try:
            user = await ctxt.repository.installation.get_user(bot_account)
            permission = await ctxt.repository.get_user_permission(user)
        except http.HTTPNotFound:
            raise RenderBotAccountFailure(
                check_api.Conclusion.ACTION_REQUIRED,
                f"User `{bot_account}` used as `{option_name}` is unknown",
                f"Please make sure `{bot_account}` exists and has logged into the [Mergify dashboard]({settings.DASHBOARD_UI_FRONT_URL}).",
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
            raise RenderBotAccountFailure(
                check_api.Conclusion.ACTION_REQUIRED,
                (
                    f"`{bot_account}` account used as "
                    f"`{option_name}` must have {fancy_perm} permission, "
                    f"not `{permission}`"
                ),
                "",
            )

    return bot_account


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
    purpose: str,
    login: github_types.GitHubLogin,
) -> github_user.GitHubUser:
    ...


@typing.overload
async def get_github_user_from_bot_account(
    purpose: str,
    login: github_types.GitHubLogin | None,
) -> github_user.GitHubUser | None:
    ...


async def get_github_user_from_bot_account(
    purpose: str,
    login: github_types.GitHubLogin | None,
) -> github_user.GitHubUser | None:
    if login is None:
        return None

    if login.endswith("[bot]"):
        raise BotAccountNotFound(
            check_api.Conclusion.FAILURE,
            f"Unable to {purpose}: GitHub App bot `{login}` can't be impersonated. ",
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
