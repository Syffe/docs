import secrets
import typing

import fastapi
import pydantic
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import application_keys
from mergify_engine.models import github as gh_models
from mergify_engine.web import utils
from mergify_engine.web.front import security


@pydantic.dataclasses.dataclass
class ApplicationJSON:
    id: pydantic.UUID4
    name: str
    created_at: str
    created_by: github_types.GitHubAccount | None


@pydantic.dataclasses.dataclass
class ApplicationJSONForEngine(ApplicationJSON):
    github_account: github_types.GitHubAccount


@pydantic.dataclasses.dataclass
class ApplicationJSONWithAPIKey(ApplicationJSON):
    api_access_key: str
    api_secret_key: str


@pydantic.dataclasses.dataclass
class ApplicationsList:
    applications: list[ApplicationJSON]


class ApplicationBody(pydantic.BaseModel):
    name: utils.PostgresTextField[str]


router = fastapi.APIRouter(
    tags=["front"],
    dependencies=[
        fastapi.Depends(security.get_current_user),
    ],
)


@router.get(
    "/github-account/{github_account_id}/applications",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def list_applications(
    github_account_id: security.GitHubAccountId,
    session: database.Session,
) -> ApplicationsList:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey).where(
            application_keys.ApplicationKey.github_account_id == github_account_id,
        ),
    )
    try:
        applications = result.unique().scalars().all()
    except sqlalchemy.exc.NoResultFound:
        raise fastapi.HTTPException(status_code=404)

    return ApplicationsList(
        applications=[
            ApplicationJSON(
                id=application.id,
                name=application.name,
                created_by=None
                if application.created_by is None
                else application.created_by.to_github_account(),
                created_at=application.created_at.isoformat(),
            )
            for application in applications
        ],
    )


@router.post(
    "/github-account/{github_account_id}/applications",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def create_application(
    github_account_id: security.GitHubAccountId,
    json: ApplicationBody,
    session: database.Session,
    current_user: security.CurrentUser,
    membership: typing.Annotated[
        security.Membership,
        fastapi.Depends(security.get_membership),
    ],
) -> ApplicationJSONWithAPIKey:
    # NOTE(sileht): the engine and dashboard token verification expected the
    # access key is 32 chars long. So we have only 28 chars left for the random string
    # token_urlsafe encode bytes in base64, so 21 bytes is encoded in 28 chars.
    api_access_key = f"mka_{secrets.token_urlsafe(21)}"
    api_secret_key = secrets.token_urlsafe(32)  # 256bytes encoded in base64

    if "organization" in membership:
        account = await gh_models.GitHubAccount.get_or_create(
            session,
            github_types.GitHubAccount(
                id=github_account_id,
                login=membership["organization"]["login"],
                type="Organization",
                avatar_url=gh_models.GitHubAccount.build_avatar_url(github_account_id),
            ),
        )
    else:
        account = await gh_models.GitHubAccount.get_or_create(
            session,
            github_types.GitHubAccount(
                id=github_account_id,
                login=membership["user"]["login"],
                type=membership["user"]["type"],
                avatar_url=gh_models.GitHubAccount.build_avatar_url(github_account_id),
            ),
        )

    application = application_keys.ApplicationKey()

    for attr, value in json:
        setattr(application, attr, value)
    application.api_access_key = api_access_key
    application.api_secret_key = api_secret_key
    application.github_account_id = github_account_id
    application.created_by_github_user_id = current_user.id

    session.add(account)
    session.add(application)

    try:
        await session.commit()
    except application_keys.ApplicationKeyLimitReached:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Maximun number of applications reached",
        )

    await session.refresh(application)

    return ApplicationJSONWithAPIKey(
        id=application.id,
        name=application.name,
        created_by=None
        if application.created_by is None
        else application.created_by.to_github_account(),
        created_at=application.created_at.isoformat(),
        api_access_key=api_access_key,
        api_secret_key=api_secret_key,
    )


@router.patch(
    "/github-account/{github_account_id}/applications/{application_id}",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def update_application(
    github_account_id: security.GitHubAccountId,
    application_id: pydantic.UUID4,
    json: ApplicationBody,
    session: database.Session,
) -> ApplicationJSON:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey)
        .where(application_keys.ApplicationKey.id == application_id)
        .where(application_keys.ApplicationKey.github_account_id == github_account_id),
    )
    try:
        application = result.unique().scalar_one()
    except sqlalchemy.exc.NoResultFound:
        raise fastapi.HTTPException(status_code=404)

    for attr, value in json:
        setattr(application, attr, value)

    await session.merge(application)
    await session.commit()

    return ApplicationJSON(
        id=application.id,
        name=application.name,
        created_by=None
        if application.created_by is None
        else application.created_by.to_github_account(),
        created_at=application.created_at.isoformat(),
    )


@router.delete(
    "/github-account/{github_account_id}/applications/{application_id}",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def delete_application(
    github_account_id: security.GitHubAccountId,
    application_id: pydantic.UUID4,
    session: database.Session,
) -> fastapi.responses.Response:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey)
        .where(application_keys.ApplicationKey.id == application_id)
        .where(application_keys.ApplicationKey.github_account_id == github_account_id),
    )
    try:
        application = result.unique().scalar_one()
    except sqlalchemy.exc.NoResultFound:
        raise fastapi.HTTPException(
            status_code=404,
        )
    await session.delete(application)
    await session.commit()
    return fastapi.responses.Response(status_code=204)
