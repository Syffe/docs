import secrets
import typing

import fastapi
import pydantic
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import application_keys
from mergify_engine.models import github_account
from mergify_engine.web.front import security


@pydantic.dataclasses.dataclass
class ApplicationJSON:
    id: int
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
    name: str = pydantic.Field(
        min_length=1,
        max_length=255,
        json_schema_extra={"strip_whitespace": True},
    )


router = fastapi.APIRouter(
    tags=["front"],
    dependencies=[
        fastapi.Depends(security.get_current_user),
    ],
)


@router.get(
    "/github-account/{account_id}/applications",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def list_applications(
    account_id: int, session: database.Session
) -> ApplicationsList:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey).where(
            application_keys.ApplicationKey.github_account_id == account_id
        )
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
        ]
    )


APPLICATIONS_LIMIT = 200


@router.post(
    "/github-account/{account_id}/applications",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def create_application(
    account_id: security.GitHubAccountId,
    json: ApplicationBody,
    session: database.Session,
    current_user: security.CurrentUser,
    membership: typing.Annotated[
        security.Membership, fastapi.Depends(security.get_membership)
    ],
) -> ApplicationJSONWithAPIKey:
    # NOTE(sileht): the engine and dashboard token verification expected the
    # access key is 32 chars long. So we have only 28 chars left for the random string
    # token_urlsafe encode bytes in base64, so 21 bytes is encoded in 28 chars.
    api_access_key = f"mka_{secrets.token_urlsafe(21)}"
    api_secret_key = secrets.token_urlsafe(32)  # 256bytes encoded in base64

    result = await session.execute(
        sqlalchemy.select(sqlalchemy.func.count(application_keys.ApplicationKey.id))
        .join(application_keys.ApplicationKey.github_account)
        .where(application_keys.ApplicationKey.github_account_id == account_id)
    )
    try:
        count = result.scalar_one()
    except sqlalchemy.exc.NoResultFound:
        raise fastapi.HTTPException(status_code=404)
    if count >= APPLICATIONS_LIMIT:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"too many applications : {count} > {APPLICATIONS_LIMIT - 1}",
        )

    if "organization" in membership:
        await github_account.GitHubAccount.create_or_update(
            session,
            github_account.GitHubAccountDict(
                id=account_id,
                login=membership["organization"]["login"],
                type="Organization",
            ),
        )
    else:
        await github_account.GitHubAccount.create_or_update(
            session,
            github_account.GitHubAccountDict(
                id=account_id,
                login=membership["user"]["login"],
                type=membership["user"]["type"],
            ),
        )

    application = application_keys.ApplicationKey()

    for attr, value in json:
        setattr(application, attr, value)
    application.api_access_key = api_access_key
    application.api_secret_key = api_secret_key
    application.github_account_id = account_id
    application.created_by_github_user_id = current_user.id

    session.add(application)
    await session.commit()
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
    "/github-account/{account_id}/applications/{application_id}",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def update_application(
    account_id: security.GitHubAccountId,
    application_id: int,
    json: ApplicationBody,
    session: database.Session,
) -> ApplicationJSON:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey)
        .join(application_keys.ApplicationKey.github_account)
        .where(application_keys.ApplicationKey.id == application_id)
        .where(application_keys.ApplicationKey.github_account_id == account_id)
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
    "/github-account/{account_id}/applications/{application_id}",
    dependencies=[
        fastapi.Depends(security.github_admin_role_required),
    ],
)
async def delete_application(
    account_id: security.GitHubAccountId,
    application_id: int,
    session: database.Session,
) -> fastapi.responses.Response:
    result = await session.execute(
        sqlalchemy.select(application_keys.ApplicationKey)
        .where(application_keys.ApplicationKey.id == application_id)
        .where(application_keys.ApplicationKey.github_account_id == account_id)
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
