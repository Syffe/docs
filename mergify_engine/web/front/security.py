import typing

import fastapi
import imia
import starlette

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models import github_user


def get_current_user(request: fastapi.Request) -> github_user.GitHubUser:
    if request.auth and request.auth.is_authenticated:
        return typing.cast(github_user.GitHubUser, request.auth.user)

    raise fastapi.HTTPException(401)


CurrentUser = typing.Annotated[
    github_user.GitHubUser, fastapi.Depends(get_current_user)
]
RequiredLogin = CurrentUser


def is_mergify_admin(
    auth: imia.user_token.UserToken, connection: starlette.requests.HTTPConnection
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


GitHubAccountId = typing.Annotated[
    github_types.GitHubAccountIdType,
    fastapi.Path(description="The GitHub account id"),
]


async def get_membership(
    account_id: GitHubAccountId, logged_user: CurrentUser
) -> github_types.GitHubMembership:
    if account_id == logged_user.id:
        # We are always admin of our account
        return github_types.GitHubMembership(
            state="active",
            role="admin",
            user=logged_user.to_github_account(),
            organization=logged_user.to_github_account(),
        )

    async with github.AsyncGitHubInstallationClient(
        github.GitHubTokenAuth(logged_user.oauth_access_token)
    ) as client:
        try:
            return typing.cast(
                github_types.GitHubMembership,
                await client.item(f"/user/memberships/orgs/{account_id}"),
            )
        except http.HTTPNotFound:
            raise fastapi.HTTPException(403)


async def github_admin_role_required(
    account_id: GitHubAccountId, logged_user: CurrentUser
) -> None:
    membership = await get_membership(account_id, logged_user)
    if membership["role"] != "admin":
        raise fastapi.HTTPException(403)
