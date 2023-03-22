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
    account_scope: application_mod.ApplicationAccountScope


router = fastapi.APIRouter(tags=["applications"])


@router.get(
    "/application",
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
    application: typing.Annotated[
        application_mod.Application,
        fastapi.Security(security.get_application_without_scope_verification),
    ],
) -> ApplicationResponse:
    return ApplicationResponse(
        id=application.id,
        name=application.name,
        account_scope=application.account_scope,
    )
