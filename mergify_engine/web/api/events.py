import datetime
import typing

import fastapi
import pydantic.dataclasses

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
        }
    )


@router.get(
    "/repos/{owner}/{repository}/logs",
    summary="Get the events log",
    description="Get the events logs of the requested repository",
    response_model=EventsResponse,
    responses={
        **api.default_responses,  # type: ignore
        200: {
            "headers": pagination.LinkHeader,
            "content": {
                "application/json": {
                    "example": {
                        "size": 0,
                        "per_page": 0,
                        "total": 0,
                        "events": [
                            {
                                "id": 0,
                                "received_at": "2019-08-24T14:15:22Z",
                                "timestamp": "2019-08-24T14:15:22Z",
                                "trigger": "string",
                                "repository": "string",
                                "pull_request": 0,
                                "type": "action.assign",
                                "event": "action.assign",
                                "metadata": {
                                    "added": ["string"],
                                    "removed": ["string"],
                                },
                            }
                        ],
                    }
                }
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
    # avoid circular import
    from mergify_engine import eventlogs

    # NOTE(lecrepont01): ensure transition from redis db to postgreSQL
    if await eventlogs.use_events_redis_backend(repository):
        page_response = await eventlogs.get(
            repository,
            page,
            pull_request,
            event_type,
            received_from,
            received_to,
            new_format=True,
        )
    else:
        page_response = await evt_utils.get(
            session,
            page,
            repository,
            pull_request,
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
