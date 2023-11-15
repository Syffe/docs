import datetime
import typing

import fastapi
import pydantic

from mergify_engine import database
from mergify_engine import events as evt_utils
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.models import enumerations
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["events"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


class EventsResponse(pagination.PageResponse[evt_utils.Event]):
    items_key: typing.ClassVar[str] = "events"
    events: list[evt_utils.Event] = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The list of events",
            },
        },
    )


@router.get(
    "/repos/{owner}/{repository}/logs",
    summary="Get the events log",
    description="Get the events logs of the requested repository",
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
                "`events[].type` respectively. Please use those instead.",
            },
        },
    },
)
async def get_repository_events(
    session: database.Session,
    repository: security.Repository,
    page: pagination.CurrentPage,
    pull_request: typing.Annotated[
        github_types.GitHubPullRequestNumber | None,
        fastapi.Query(description="Get the events for the specified pull request"),
    ] = None,
    base_ref: typing.Annotated[
        github_types.GitHubRefType | None,
        fastapi.Query(description="Get events for PRs to the given base ref"),
    ] = None,
    event_type: typing.Annotated[
        list[enumerations.EventType] | None,
        fastapi.Query(description="The specific types of events to select"),
    ] = None,
    received_from: typing.Annotated[
        datetime.datetime | None,
        fastapi.Query(description="Get the events received from this date"),
    ] = None,
    received_to: typing.Annotated[
        datetime.datetime | None,
        fastapi.Query(description="Get the events received until this date"),
    ] = None,
) -> EventsResponse:
    page_response = await evt_utils.get(
        session,
        page,
        repository,
        pull_request,
        base_ref,
        event_type,
        received_from,
        received_to,
    )

    return EventsResponse(  # type: ignore[call-arg]
        page=page_response,
        query_parameters={
            "pull_request": pull_request,
            "event_type": event_type,
            "received_from": received_from,
            "received_to": received_to,
        },
    )
