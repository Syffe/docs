import typing

import fastapi
import imia
import starlette

from mergify_engine import settings
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
        and auth.user.id in settings.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO
    )


def mergify_admin_login_required(request: fastapi.Request, _: RequiredLogin) -> None:
    if not is_mergify_admin(request.auth, request):
        raise fastapi.HTTPException(403)
