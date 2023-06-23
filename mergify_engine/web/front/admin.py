import contextlib
import io
import typing

import daiquiri
import fastapi
import fastapi.responses
import imia
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.orm.exc

from mergify_engine import database
from mergify_engine import debug
from mergify_engine import github_types
from mergify_engine.clients import dashboard
from mergify_engine.models import github_user
from mergify_engine.web.front import security


LOG = daiquiri.getLogger(__name__)


async def _get_user(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    _filter: sqlalchemy.sql.ColumnElement[typing.Any],
) -> github_user.GitHubUser | None:
    result = await session.execute(
        sqlalchemy.select(github_user.GitHubUser).where(_filter)
    )
    return result.unique().scalar_one_or_none()


async def _get_user_by_id(
    session: sqlalchemy.ext.asyncio.AsyncSession, _id: int
) -> github_user.GitHubUser | None:
    return await _get_user(session, github_user.GitHubUser.id == _id)


async def _get_user_by_login(
    session: sqlalchemy.ext.asyncio.AsyncSession, login: str
) -> github_user.GitHubUser | None:
    return await _get_user(session, github_user.GitHubUser.login == login)


async def select_user_from_login(
    session: sqlalchemy.ext.asyncio.AsyncSession, login: github_types.GitHubLogin
) -> github_user.GitHubUser:
    account = await _get_user_by_login(session, login)

    if account is not None:
        return account

    # Check if the login is an organization with billing system
    async with dashboard.AsyncDashboardSaasClient() as client:
        try:
            associated_users = await client.get_associated_users(login)
        except dashboard.NoAssociatedUsersFound as e:
            raise fastapi.HTTPException(status_code=404, detail=str(e))

        for associated_user in associated_users:
            user = await _get_user_by_id(session, associated_user["id"])
            if user is not None:
                return user

    raise fastapi.HTTPException(
        status_code=404,
        detail="User or Organization has no Mergify account",
    )


router = fastapi.APIRouter(tags=["front"])


@router.get(
    "/sudo/{login}",
    dependencies=[
        fastapi.Depends(security.mergify_admin_login_required),
    ],
)
async def sudo(
    request: fastapi.Request, login: github_types.GitHubLogin, session: database.Session
) -> fastapi.Response:
    from_user = imia.impersonation.get_original_user(request).login

    if imia.impersonation.impersonation_is_active(request):
        request.session.pop("sudo", None)
        request.session.pop("sudoGrantedTo", None)
        imia.impersonation.exit_impersonation(request)

    LOG.info("sudo to %s requested for %s", login, from_user, gh_owner=from_user)
    user = await select_user_from_login(session, login)
    LOG.info("sudo to %s granted for %s", login, from_user, gh_owner=from_user)

    if from_user != login:
        imia.impersonation.impersonate(request, user)
        request.session["sudo"] = True
        request.session["sudoGrantedTo"] = from_user
    return fastapi.responses.JSONResponse({})


@router.get(
    "/sudo-debug/{login}/{repository}/pull/{pull_number}",
    dependencies=[
        fastapi.Depends(security.mergify_admin_login_required),
    ],
)
async def sudo_debug(
    request: fastapi.Request,
    login: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
    pull_number: github_types.GitHubPullRequestNumber,
    session: database.Session,
) -> fastapi.Response:
    from_user = imia.impersonation.get_original_user(request).login

    url = f"https://github.com/{login}/{repository}/pull/{pull_number}"
    LOG.info("sudo-debug to %s granted for %s", url, from_user)

    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        await debug.report(url)
    return fastapi.responses.Response(f.getvalue(), media_type="text/plain")
