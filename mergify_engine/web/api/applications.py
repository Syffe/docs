import typing

import daiquiri
import fastapi
import pydantic
import typing_extensions

from mergify_engine import github_types
from mergify_engine.clients import github
from mergify_engine.models import application_keys
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


class ApplicationAccountScope(typing_extensions.TypedDict):
    id: github_types.GitHubAccountIdType
    login: github_types.GitHubLogin


@pydantic.dataclasses.dataclass
class ApplicationResponse:
    id: int
    name: str
    account_scope: ApplicationAccountScope


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
                    },
                },
            },
        },
    },
)
async def application(
    application: typing.Annotated[
        application_keys.ApplicationKey,
        fastapi.Security(security.get_application_without_scope_verification),
    ],
) -> ApplicationResponse:
    # NOTE(sileht): We get the installation only to check the account still exists and Mergify is still installed
    await github.get_installation_from_account_id(application.github_account.id)

    return ApplicationResponse(
        id=application.id,
        name=application.name,
        account_scope={
            "id": github_types.GitHubAccountIdType(application.github_account.id),
            "login": application.github_account.login,
        },
    )
