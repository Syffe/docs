import typing

import daiquiri
import fastapi
import pydantic

from mergify_engine import database
from mergify_engine import eventlogs
from mergify_engine import events
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


router = fastapi.APIRouter(
    tags=["eventlogs"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


Event = typing.Annotated[
    eventlogs.Event,
    pydantic.Field(discriminator="type"),
]


class EventLogsResponse(pagination.PageResponse[Event]):
    items_key: typing.ClassVar[str] = "events"
    events: list[Event] = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The list of events of a pull request",
            },
        }
    )


@router.get(
    "/repos/{owner}/{repository}/pulls/{pull}/events",
    summary="Get the events log of a pull request",
    description="Get the events log of the requested pull request",
    deprecated=True,
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
        },
    },
    # NOTE(lecrepont01): remove with old API deprecation and MRGFY-2849
    openapi_extra={
        "responses": {
            "200": {
                "description": "Successful Response.\n\n"
                "**Important note**: response attributes `events[].timestamp` and "
                "`events[].event` are deprecated and being replaced by `events[].received_at` and "
                "`events[].type` respectively. Please use those instead."
            }
        }
    },
)
async def get_pull_request_eventlogs(
    session: database.Session,
    repository_ctxt: security.Repository,
    pull: typing.Annotated[
        github_types.GitHubPullRequestNumber,
        fastapi.Path(description="Pull request number"),
    ],
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    page = await events.get(session, current_page, repository_ctxt, pull)

    return EventLogsResponse(page)  # type: ignore[misc, call-arg]


@router.get(
    "/repos/{owner}/{repository}/events",
    summary="Get the events log of a repository",
    description="Get the events log of the requested repository",
    deprecated=True,
    dependencies=[fastapi.Depends(security.check_subscription_feature_eventlogs)],
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
        },
    },
    # NOTE(lecrepont01): remove with old API deprecation and MRGFY-2849
    openapi_extra={
        "responses": {
            "200": {
                "description": "Successful Response.\n\n"
                "**Important note**: response attributes `events[].timestamp` and "
                "`events[].event` are deprecated and being replaced by `events[].received_at` and "
                "`events[].type` respectively. Please use those instead."
            }
        }
    },
)
async def get_repository_eventlogs(
    session: database.Session,
    repository_ctxt: security.Repository,
    current_page: pagination.CurrentPage,
) -> EventLogsResponse:
    page = await events.get(session, current_page, repository_ctxt)

    return EventLogsResponse(page)  # type: ignore[misc, call-arg]
