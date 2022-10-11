import typing

import fastapi

from mergify_engine.models import github_user


def get_current_user(request: fastapi.Request) -> github_user.GitHubUser:
    if request.auth and request.auth.is_authenticated:
        return typing.cast(github_user.GitHubUser, request.auth.user)

    raise fastapi.HTTPException(401)


login_required = get_current_user
