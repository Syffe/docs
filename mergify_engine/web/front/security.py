import typing

import fastapi
import imia
import starlette

from mergify_engine import config
from mergify_engine.models import github_user


def get_current_user(request: fastapi.Request) -> github_user.GitHubUser:
    if request.auth and request.auth.is_authenticated:
        return typing.cast(github_user.GitHubUser, request.auth.user)

    raise fastapi.HTTPException(401)


login_required = get_current_user


def is_mergify_admin(
    auth: imia.user_token.UserToken, connection: starlette.requests.HTTPConnection
) -> bool:
    return (
        auth
        and auth.is_authenticated
        and auth.user.id in config.DASHBOARD_UI_GITHUB_IDS_ALLOWED_TO_SUDO
    )


def mergify_admin_login_required(
    request: fastapi.Request,
    _: None = fastapi.Depends(login_required),  # noqa: B008
) -> None:
    if not is_mergify_admin(request.auth, request):
        raise fastapi.HTTPException(403)
