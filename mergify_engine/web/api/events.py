import datetime
import typing

import daiquiri
import fastapi
import pydantic.dataclasses

from mergify_engine import database
from mergify_engine import eventlogs
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.models import enumerations
from mergify_engine.models import events
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)


router = fastapi.APIRouter(
    tags=["events"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


Event = typing.Annotated[
    eventlogs.Event,
    pydantic.Field(discriminator="type"),
]


class EventsResponse(pagination.PageResponse[Event]):
    items_key: typing.ClassVar[str] = "events"
    events: list[Event] = pydantic.Field(
        json_schema_extra={
            "metadata": {
                "description": "The list of events",
            },
        }
    )


def format_event_item_response(event: events.Event) -> dict[str, typing.Any]:
    result: dict[str, typing.Any] = {
        "id": event.id,
        "type": event.type.value,
        "received_at": event.received_at,
        "trigger": event.trigger,
        "pull_request": event.pull_request,
        "repository": event.repository.name,
    }
    # place child model (event specific) attributes in the metadata key
    if event.__class__.__bases__[0] == events.Event:
        result["metadata"] = {
            col.name: getattr(event, col.name)
            for col in event.__table__.columns
            if col.name not in ["id"]
        }

    return result


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
    repository_ctxt: security.Repository,
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
    backward = page.cursor is not None and page.cursor.startswith("-")

    results = await events.Event.get(
        session,
        page,
        backward,
        repository_ctxt,
        pull_request,
        event_type,
        received_from,
        received_to,
    )

    if results and backward:
        cursor_next = f"-{results[0].id}"
        cursor_prev = f"+{results[-1].id}" if not page.cursor == "-" else None
    elif results:
        cursor_next = f"+{results[-1].id}"
        cursor_prev = f"-{results[0].id}" if page.cursor else None
    else:
        cursor_next = None
        cursor_prev = None

    events_list: list[eventlogs.Event] = []
    for raw in results:
        event = typing.cast(eventlogs.GenericEvent, format_event_item_response(raw))
        try:
            events_list.append(eventlogs.cast_event_item(event, key="type"))
        except eventlogs.UnsupportedEvent as err:
            LOG.error(err.message, event=err.event)

    return EventsResponse(  # type: ignore[call-arg]
        page=pagination.Page(
            items=events_list,
            current=page,
            total=await events.Event.total(session),
            cursor_prev=cursor_prev,
            cursor_next=cursor_next,
        )
    )
