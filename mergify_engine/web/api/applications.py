import typing

import daiquiri
import fastapi
import pydantic

from mergify_engine.dashboard import application as application_mod
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


@pydantic.dataclasses.dataclass
class ApplicationResponse:
    id: int
    name: str
    account_scope: typing.Optional[application_mod.ApplicationAccountScope]


router = fastapi.APIRouter(
    tags=["applications"],
    dependencies=[
        fastapi.Depends(security.require_authentication),
    ],
)


@router.get(
    "/application",  # noqa: FS003
    summary="Get current application",
    description="Get the current authenticated application",
    response_model=ApplicationResponse,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "Not found"},
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "id": 123456,
                        "name": "an application name",
                        "account_scope": {
                            "id": 123456,
                            "login": "Mergifyio",
                        },
                    }
                }
            }
        },
    },
)
async def application(
    application: application_mod.Application = fastapi.Depends(  # noqa: B008
        security.get_application
    ),
) -> ApplicationResponse:
    return ApplicationResponse(
        id=application.id,
        name=application.name,
        account_scope=application.account_scope,
    )
