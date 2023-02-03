import typing

import daiquiri
import fastapi
import fastapi.responses
import imia
import sqlalchemy
import sqlalchemy.orm.exc

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.clients import dashboard
from mergify_engine.clients import http
from mergify_engine.models import github_user
from mergify_engine.web.front import security


LOG = daiquiri.getLogger(__name__)


class AssociatedUsersAccount(typing.TypedDict):
    id: int


class AssociatedUser(typing.TypedDict):
    id: int
    membership: github_types.GitHubMembershipRole | None


class AssociatedUsers(typing.TypedDict):
    account: AssociatedUsersAccount
    associated_users: list[AssociatedUser]


async def _get_user(
    session: sqlalchemy.ext.asyncio.AsyncSession, _filter: sqlalchemy.sql.ClauseElement
) -> github_user.GitHubUser | None:
    result = await session.execute(
        sqlalchemy.select(github_user.GitHubUser).where(_filter)
    )
    return typing.cast(
        github_user.GitHubUser | None, result.unique().scalar_one_or_none()
    )


async def _get_user_by_id(
    session: sqlalchemy.ext.asyncio.AsyncSession, _id: int
) -> github_user.GitHubUser | None:
    return await _get_user(session, github_user.GitHubUser.id == _id)


async def _get_user_by_login(
    session: sqlalchemy.ext.asyncio.AsyncSession, login: str
) -> github_user.GitHubUser | None:
    return await _get_user(session, github_user.GitHubUser.login == login)


async def select_user_from_login(
    session: sqlalchemy.ext.asyncio.AsyncSession, login: str
) -> github_user.GitHubUser:
    account = await _get_user_by_login(session, login)

    if account is not None:
        return account

    # Check if the login is an organization with billing system
    async with dashboard.AsyncDashboardSaasClient() as client:
        try:
            resp = await client.get(
                url=f"/engine/associated-users/{login}",
                follow_redirects=True,
            )
        except http.HTTPNotFound:
            raise fastapi.HTTPException(
                status_code=404,
                detail="User or Organization has no Mergify account",
            )
        data = typing.cast(AssociatedUsers, resp.json())
        # Try admin first
        associated_users = sorted(
            data["associated_users"],
            key=lambda x: x["membership"] != "admin",
        )

        for associated_user in associated_users:
            user = await _get_user_by_id(session, associated_user["id"])
            if user is not None:
                return user

    raise fastapi.HTTPException(
        status_code=404,
        detail="User or Organization has no Mergify account",
    )


router = fastapi.APIRouter()


@router.get(
    "/sudo/{login}",  # noqa: FS003
    dependencies=[
        fastapi.Depends(security.mergify_admin_login_required),
    ],
)
async def sudo(
    request: fastapi.Request,
    login: str,
    session: sqlalchemy.ext.asyncio.AsyncSession = fastapi.Depends(  # noqa: B008
        models.get_session
    ),
) -> fastapi.Response:
    from_user = request.auth.user.login
    LOG.info("sudo to %s requested for %s", login, from_user, gh_owner=from_user)
    user = await select_user_from_login(session, login)
    LOG.info("sudo to %s granted for %s", login, from_user, gh_owner=from_user)
    request.session["sudo"] = True
    request.session["sudoGrantedTo"] = from_user
    imia.impersonation.impersonate(request, user)
    return fastapi.responses.JSONResponse({})
