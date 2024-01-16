import dataclasses
import datetime
import itertools
import typing

import daiquiri
import pydantic
import sqlalchemy
import sqlalchemy.ext.asyncio
import typing_extensions

from mergify_engine import context
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import signals
from mergify_engine.models import enumerations
from mergify_engine.models import events as evt_models
from mergify_engine.models import github as gh_models


LOG = daiquiri.getLogger(__name__)


class EventBase(typing_extensions.TypedDict):
    id: int
    received_at: datetime.datetime
    trigger: str
    repository: str
    pull_request: int | None
    base_ref: github_types.GitHubRefType | None


class EventMetadata(typing_extensions.TypedDict):
    pass


class EventBaseNoMetadata(EventBase):
    metadata: signals.EventNoMetadata


class EventAssign(EventBase):
    type: typing.Literal["action.assign"]
    metadata: signals.EventAssignMetadata


class EventBackport(EventBase):
    type: typing.Literal["action.backport"]
    metadata: signals.EventCopyMetadata


class EventClose(EventBase):
    type: typing.Literal["action.close"]
    metadata: signals.EventCloseMetadata


class EventComment(EventBase):
    type: typing.Literal["action.comment"]
    metadata: signals.EventCommentMetadata


class EventCopy(EventBase):
    type: typing.Literal["action.copy"]
    metadata: signals.EventCopyMetadata


class EventEdit(EventBase):
    type: typing.Literal["action.edit"]
    metadata: signals.EventEditMetadata


class EventDeleteHeadBranch(EventBase):
    type: typing.Literal["action.delete_head_branch"]
    metadata: signals.EventDeleteHeadBranchMetadata


class EventDismissReviews(EventBase):
    type: typing.Literal["action.dismiss_reviews"]
    metadata: signals.EventDismissReviewsMetadata


class EventLabel(EventBase):
    type: typing.Literal["action.label"]
    metadata: signals.EventLabelMetadata


class EventMerge(EventBase):
    type: typing.Literal["action.merge"]
    metadata: signals.EventMergeMetadata


class EventPostCheck(EventBase):
    type: typing.Literal["action.post_check"]
    metadata: signals.EventPostCheckMetadata


class EventGithubActions(EventBase):
    type: typing.Literal["action.github_actions"]
    metadata: signals.EventGithubActionsMetadata


class EventQueueEnter(EventBase):
    type: typing.Literal["action.queue.enter"]
    metadata: signals.EventQueueEnterMetadata


class EventQueueLeave(EventBase):
    type: typing.Literal["action.queue.leave"]
    metadata: signals.EventQueueLeaveMetadata


class EventQueueChange(EventBase):
    type: typing.Literal["action.queue.change"]
    metadata: signals.EventQueueChangeMetadata


class EventQueueChecksStart(EventBase):
    type: typing.Literal["action.queue.checks_start"]
    metadata: signals.EventQueueChecksStartMetadata


class EventQueueChecksEnd(EventBase):
    type: typing.Literal["action.queue.checks_end"]
    metadata: signals.EventQueueChecksEndMetadata


class EventQueueMerged(EventBase):
    type: typing.Literal["action.queue.merged"]
    metadata: signals.EventQueueMergedMetadata


class EventRebase(EventBaseNoMetadata):
    type: typing.Literal["action.rebase"]


class EventRefresh(EventBaseNoMetadata):
    type: typing.Literal["action.refresh"]


class EventRequeue(EventBaseNoMetadata):
    type: typing.Literal["action.requeue"]


class EventRequestReviews(EventBase):
    type: typing.Literal["action.request_reviews"]
    metadata: signals.EventRequestReviewsMetadata


class EventReview(EventBase):
    type: typing.Literal["action.review"]
    metadata: signals.EventReviewMetadata


class EventSquash(EventBaseNoMetadata):
    type: typing.Literal["action.squash"]


class EventUnqueue(EventBaseNoMetadata):
    type: typing.Literal["action.unqueue"]


class EventUpdate(EventBaseNoMetadata):
    type: typing.Literal["action.update"]


class EventQueueFreezeCreate(EventBase):
    type: typing.Literal["queue.freeze.create"]
    metadata: signals.EventQueueFreezeCreateMetadata


class EventQueueFreezeUpdate(EventBase):
    type: typing.Literal["queue.freeze.update"]
    metadata: signals.EventQueueFreezeUpdateMetadata


class EventQueueFreezeDelete(EventBase):
    type: typing.Literal["queue.freeze.delete"]
    metadata: signals.EventQueueFreezeDeleteMetadata


class EventQueuePauseCreate(EventBase):
    type: typing.Literal["queue.pause.create"]
    metadata: signals.EventQueuePauseCreateMetadata


class EventQueuePauseUpdate(EventBase):
    type: typing.Literal["queue.pause.update"]
    metadata: signals.EventQueuePauseUpdateMetadata


class EventQueuePauseDelete(EventBase):
    type: typing.Literal["queue.pause.delete"]
    metadata: signals.EventQueuePauseDeleteMetadata


Event = (
    EventAssign
    | EventBackport
    | EventClose
    | EventComment
    | EventCopy
    | EventDeleteHeadBranch
    | EventDismissReviews
    | EventEdit
    | EventLabel
    | EventMerge
    | EventPostCheck
    | EventGithubActions
    | EventQueueEnter
    | EventQueueLeave
    | EventQueueChecksStart
    | EventQueueChecksEnd
    | EventQueueMerged
    | EventRebase
    | EventRefresh
    | EventRequestReviews
    | EventRequeue
    | EventReview
    | EventSquash
    | EventUnqueue
    | EventUpdate
    | EventQueueFreezeCreate
    | EventQueueFreezeUpdate
    | EventQueueFreezeDelete
    | EventQueuePauseCreate
    | EventQueuePauseUpdate
    | EventQueuePauseDelete
    | EventQueueChange
)

SUPPORTED_EVENT_NAMES = list(
    itertools.chain(
        *[
            evt.__annotations__["type"].__args__
            for evt in Event.__args__  # type: ignore[attr-defined]
        ],
    ),
)


class GenericEvent(EventBase):
    type: signals.EventName
    metadata: signals.EventMetadata


@dataclasses.dataclass
class UnsupportedEventError(Exception):
    event: GenericEvent
    message: str = "unsupported event-type, skipping"


class EventLogsSignal(signals.SignalBase):
    async def __call__(
        self,
        repository: context.Repository,
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event not in SUPPORTED_EVENT_NAMES:
            return

        if event in EVENT_NAME_TO_MODEL:
            await insert(
                event=event,
                repository=repository.repo,
                pull_request=pull_request,
                base_ref=base_ref,
                metadata=metadata,
                trigger=trigger,
            )
        else:
            LOG.debug("skipping event-type not supported in database", event=event)


def cast_event_item(event: GenericEvent) -> Event:
    match event["type"]:
        case "action.assign":
            return typing.cast(EventAssign, event)
        case "action.backport":
            return typing.cast(EventBackport, event)
        case "action.close":
            return typing.cast(EventClose, event)
        case "action.comment":
            return typing.cast(EventComment, event)
        case "action.copy":
            return typing.cast(EventCopy, event)
        case "action.delete_head_branch":
            return typing.cast(EventDeleteHeadBranch, event)
        case "action.dismiss_reviews":
            return typing.cast(EventDismissReviews, event)
        case "action.label":
            return typing.cast(EventLabel, event)
        case "action.merge":
            return typing.cast(EventMerge, event)
        case "action.post_check":
            return typing.cast(EventPostCheck, event)
        case "action.queue.checks_start":
            return typing.cast(EventQueueChecksStart, event)
        case "action.queue.checks_end":
            return typing.cast(EventQueueChecksEnd, event)
        case "action.github_actions":
            return typing.cast(EventGithubActions, event)
        case "action.queue.enter":
            return typing.cast(EventQueueEnter, event)
        case "action.queue.leave":
            return typing.cast(EventQueueLeave, event)
        case "action.queue.change":
            return typing.cast(EventQueueChange, event)
        case "action.queue.merged":
            return typing.cast(EventQueueMerged, event)
        case "action.rebase":
            return typing.cast(EventRebase, event)
        case "action.refresh":
            return typing.cast(EventRefresh, event)
        case "action.request_reviews":
            return typing.cast(EventRequestReviews, event)
        case "action.requeue":
            return typing.cast(EventRequeue, event)
        case "action.review":
            return typing.cast(EventReview, event)
        case "action.squash":
            return typing.cast(EventSquash, event)
        case "action.unqueue":
            return typing.cast(EventUnqueue, event)
        case "action.update":
            return typing.cast(EventUpdate, event)
        case "action.edit":
            return typing.cast(EventEdit, event)
        case "queue.freeze.create":
            return typing.cast(EventQueueFreezeCreate, event)
        case "queue.freeze.update":
            return typing.cast(EventQueueFreezeUpdate, event)
        case "queue.freeze.delete":
            return typing.cast(EventQueueFreezeDelete, event)
        case "queue.pause.create":
            return typing.cast(EventQueuePauseCreate, event)
        case "queue.pause.update":
            return typing.cast(EventQueuePauseUpdate, event)
        case "queue.pause.delete":
            return typing.cast(EventQueuePauseDelete, event)

    raise UnsupportedEventError(event)


EVENT_NAME_TO_MODEL = {
    subclass.__mapper_args__["polymorphic_identity"]: typing.cast(
        evt_models.Event,
        subclass,
    )
    for subclass in evt_models.Event.__subclasses__()
}


class EventNotHandledError(Exception):
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
        raise EventNotHandledError(f"Event '{event}' not supported in database")

    async for attempt in database.tenacity_retry_on_pk_integrity_error(
        (gh_models.GitHubRepository, gh_models.GitHubAccount),
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
    result: dict[str, typing.Any] = {
        "id": event.id,
        "type": event.type.value,
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
            if col.name != "id":
                metadata[col.name] = getattr(event, col.name)
        for r in inspector.relationships:
            if r.key != "repository":
                metadata[r.key] = getattr(event, r.key)._as_dict()
        result["metadata"] = metadata

    return result


EventField = typing.Annotated[
    Event,
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
) -> pagination.Page[EventField]:
    events = await evt_models.Event.get(
        session,
        page.cursor,
        page.per_page,
        repository,
        pull_request,
        base_ref,
        event_type,
        received_from,
        received_to,
    )
    first_event_id = str(events[0].id) if events else None
    last_event_id = str(events[-1].id) if events else None

    casted_events: list[Event] = []
    for raw in events:
        event = typing.cast(GenericEvent, format_event_item_response(raw))
        try:
            casted_events.append(cast_event_item(event))
        except UnsupportedEventError as err:
            LOG.error(err.message, event=err.event)

    return pagination.Page(
        items=casted_events,
        current=page,
        cursor_prev=page.cursor.previous(first_event_id, last_event_id),
        cursor_next=page.cursor.next(first_event_id, last_event_id),
    )


async def delete_outdated_events() -> None:
    async with database.create_session() as session:
        await evt_models.Event.delete_outdated(
            session,
            retention_time=database.CLIENT_DATA_RETENTION_TIME,
        )
        await session.commit()
