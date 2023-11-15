import typing

import fastapi
import imia
import starlette

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models.github import user as github_user


def get_current_user(request: fastapi.Request) -> github_user.GitHubUser:
    if request.auth and request.auth.is_authenticated:
        return typing.cast(github_user.GitHubUser, request.auth.user)

    raise fastapi.HTTPException(401)


CurrentUser = typing.Annotated[
    github_user.GitHubUser,
    fastapi.Depends(get_current_user),
]
RequiredLogin = CurrentUser


def is_mergify_admin(
    auth: imia.user_token.UserToken,
    connection: starlette.requests.HTTPConnection,
) -> bool:
    return (
        auth
        and auth.is_authenticated
        and imia.impersonation.get_original_user(connection).id
        in settings.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO
    )


def mergify_admin_login_required(request: fastapi.Request, _: RequiredLogin) -> None:
    if not is_mergify_admin(request.auth, request):
        raise fastapi.HTTPException(403)


_RawGitHubAccountId = typing.Annotated[
    github_types.GitHubAccountIdType,
    fastapi.Path(description="The GitHub account id"),
]


async def _get_validated_github_account_id(
    github_account_id: _RawGitHubAccountId,
    logged_user: CurrentUser,
) -> github_types.GitHubAccountIdType:
    # NOTE(sileht): We get the installation only to check the account still exists and Mergify is still installed
    await github.get_installation_from_account_id(github_account_id)
    return github_account_id


GitHubAccountId = typing.Annotated[
    github_types.GitHubAccountIdType,
    fastapi.Depends(_get_validated_github_account_id),
]


class Membership(typing.TypedDict):
    role: github_types.GitHubMembershipRole
    user: github_types.GitHubAccount
    organization: typing.NotRequired[github_types.GitHubOrganization]


async def get_membership(
    account_id: GitHubAccountId,
    logged_user: CurrentUser,
) -> Membership:
    if account_id == logged_user.id:
        # We are always admin of our account
        return Membership(role="admin", user=logged_user.to_github_account())

    async with github.AsyncGitHubInstallationClient(
        github.GitHubTokenAuth(logged_user.oauth_access_token),
    ) as client:
        try:
            gh_membership = typing.cast(
                github_types.GitHubMembership,
                await client.item(f"/user/memberships/orgs/{account_id}"),
            )

            return Membership(
                role=gh_membership["role"],
                user=gh_membership["user"],
                organization=gh_membership["organization"],
            )
        except (http.HTTPNotFound, http.HTTPForbidden):
            raise fastapi.HTTPException(403)


async def github_admin_role_required(
    account_id: GitHubAccountId,
    logged_user: CurrentUser,
) -> None:
    membership = await get_membership(account_id, logged_user)
    if membership["role"] != "admin":
        raise fastapi.HTTPException(403)
