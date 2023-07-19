from __future__ import annotations

import typing

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.models import events as evt_model


if typing.TYPE_CHECKING:
    from mergify_engine.models import github_repository


EVENT_NAME_TO_MODEL = {
    subclass.__mapper_args__["polymorphic_identity"]: typing.cast(
        evt_model.Event, subclass
    )
    for subclass in evt_model.Event.__subclasses__()
}


class EventNotHandled(Exception):
    pass


async def insert(
    event: signals.EventName,
    repository: github_types.GitHubRepository | github_repository.GitHubRepositoryDict,
    pull_request: github_types.GitHubPullRequestNumber | None,
    trigger: str,
    metadata: signals.EventMetadata,
) -> None:
    try:
        event_model = EVENT_NAME_TO_MODEL[event]
    except KeyError:
        raise EventNotHandled(f"Event '{event}' not supported in database")

    async with database.create_session() as session:
        event_obj = await event_model.create(
            session,
            repository=repository,
            pull_request=pull_request,
            trigger=trigger,
            metadata=metadata,
        )
        session.add(event_obj)
        await session.commit()
