"""
Managing the new eventlog system which is stored in a PostgreSQL database
"""
from __future__ import annotations

import datetime
import typing

import daiquiri
import pydantic
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
from mergify_engine import eventlogs
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import signals
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_models
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)

EVENT_NAME_TO_MODEL = {
    subclass.__mapper_args__["polymorphic_identity"]: typing.cast(
        evt_models.Event, subclass
    )
    for subclass in evt_models.Event.__subclasses__()
}


class EventNotHandled(Exception):
    pass


async def insert(
    event: signals.EventName,
    repository: github_types.GitHubRepository | gh_models.GitHubRepositoryDict,
    pull_request: github_types.GitHubPullRequestNumber | None,
    base_ref: github_types.GitHubRefType | None,
    trigger: str,
    metadata: signals.EventMetadata,
) -> None:
    try:
        event_model = EVENT_NAME_TO_MODEL[event]
    except KeyError:
        raise EventNotHandled(f"Event '{event}' not supported in database")

    async for attempt in database.tenacity_retry_on_pk_integrity_error(
        (gh_models.GitHubRepository, gh_models.GitHubAccount)
    ):
        with attempt:
            async with database.create_session() as session:
                event_obj = await event_model.create(
                    session,
                    repository=repository,
                    pull_request=pull_request,
                    base_ref=base_ref,
                    trigger=trigger,
                    metadata=metadata,
                )
                session.add(event_obj)
                await session.commit()


def format_event_item_response(event: evt_models.Event) -> dict[str, typing.Any]:
    # NOTE(lecrepont01): duplicate fields (MRGFY-2555)
    result: dict[str, typing.Any] = {
        "id": event.id,
        "event": event.type.value,
        "type": event.type.value,
        "timestamp": event.received_at,
        "received_at": event.received_at,
        "trigger": event.trigger,
        "pull_request": event.pull_request,
        "repository": event.repository.full_name,
        "base_ref": event.base_ref,
    }

    inspector = sqlalchemy.inspect(event.__class__)

    # place child model (event specific) attributes in the metadata key
    if event.__class__.__bases__[0] == evt_models.Event:
        metadata = {}
        for col in event.__table__.columns:
            if col.name not in ("id",):
                metadata[col.name] = getattr(event, col.name)
        for r in inspector.relationships:
            if r.key not in ("repository",):
                metadata[r.key] = getattr(event, r.key)._as_dict()
        result["metadata"] = metadata

    return result


Event = typing.Annotated[
    eventlogs.Event,
    pydantic.Field(discriminator="type"),
]


async def get(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    page: pagination.CurrentPage,
    repository: context.Repository,
    pull_request: github_types.GitHubPullRequestNumber | None = None,
    base_ref: github_types.GitHubRefType | None = None,
    event_type: list[enumerations.EventType] | None = None,
    received_from: datetime.datetime | None = None,
    received_to: datetime.datetime | None = None,
) -> pagination.Page[Event]:
    backward = page.cursor is not None and page.cursor.startswith("-")
    results = await evt_models.Event.get(
        session,
        page,
        backward,
        repository,
        pull_request,
        base_ref,
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
            events_list.append(eventlogs.cast_event_item(event))
        except eventlogs.UnsupportedEvent as err:
            LOG.error(err.message, event=err.event)

    return pagination.Page(
        items=events_list,
        current=page,
        total=None,
        cursor_prev=cursor_prev,
        cursor_next=cursor_next,
    )
