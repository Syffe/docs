"""
Managing the old eventlog system which is stored in a Redis database
"""
import dataclasses
import datetime
import itertools
import typing

import daiquiri
import typing_extensions

from mergify_engine import github_types
from mergify_engine import signals


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)


class EventBase(typing_extensions.TypedDict):
    id: int
    received_at: datetime.datetime
    timestamp: datetime.datetime
    trigger: str
    repository: str
    pull_request: int | None
    base_ref: github_types.GitHubRefType | None


class EventMetadata(typing_extensions.TypedDict):
    pass


class EventBaseNoMetadata(EventBase):
    metadata: signals.EventNoMetadata


class EventAssign(EventBase):
    event: typing.Literal["action.assign"]
    type: typing.Literal["action.assign"]
    metadata: signals.EventAssignMetadata


class EventBackport(EventBase):
    event: typing.Literal["action.backport"]
    type: typing.Literal["action.backport"]
    metadata: signals.EventCopyMetadata


class EventClose(EventBase):
    event: typing.Literal["action.close"]
    type: typing.Literal["action.close"]
    metadata: signals.EventCloseMetadata


class EventComment(EventBase):
    event: typing.Literal["action.comment"]
    type: typing.Literal["action.comment"]
    metadata: signals.EventCommentMetadata


class EventCopy(EventBase):
    event: typing.Literal["action.copy"]
    type: typing.Literal["action.copy"]
    metadata: signals.EventCopyMetadata


class EventEdit(EventBase):
    event: typing.Literal["action.edit"]
    type: typing.Literal["action.edit"]
    metadata: signals.EventEditMetadata


class EventDeleteHeadBranch(EventBase):
    event: typing.Literal["action.delete_head_branch"]
    type: typing.Literal["action.delete_head_branch"]
    metadata: signals.EventDeleteHeadBranchMetadata


class EventDismissReviews(EventBase):
    event: typing.Literal["action.dismiss_reviews"]
    type: typing.Literal["action.dismiss_reviews"]
    metadata: signals.EventDismissReviewsMetadata


class EventLabel(EventBase):
    event: typing.Literal["action.label"]
    type: typing.Literal["action.label"]
    metadata: signals.EventLabelMetadata


class EventMerge(EventBase):
    event: typing.Literal["action.merge"]
    type: typing.Literal["action.merge"]
    metadata: signals.EventMergeMetadata


class EventPostCheck(EventBase):
    event: typing.Literal["action.post_check"]
    type: typing.Literal["action.post_check"]
    metadata: signals.EventPostCheckMetadata


class EventGithubActions(EventBase):
    event: typing.Literal["action.github_actions"]
    type: typing.Literal["action.github_actions"]
    metadata: signals.EventGithubActionsMetadata


class EventQueueEnter(EventBase):
    event: typing.Literal["action.queue.enter"]
    type: typing.Literal["action.queue.enter"]
    metadata: signals.EventQueueEnterMetadata


class EventQueueLeave(EventBase):
    event: typing.Literal["action.queue.leave"]
    type: typing.Literal["action.queue.leave"]
    metadata: signals.EventQueueLeaveMetadata


class EventQueueChange(EventBase):
    event: typing.Literal["action.queue.change"]
    type: typing.Literal["action.queue.change"]
    metadata: signals.EventQueueChangeMetadata


class EventQueueChecksStart(EventBase):
    event: typing.Literal["action.queue.checks_start"]
    type: typing.Literal["action.queue.checks_start"]
    metadata: signals.EventQueueChecksStartMetadata


class EventQueueChecksEnd(EventBase):
    event: typing.Literal["action.queue.checks_end"]
    type: typing.Literal["action.queue.checks_end"]
    metadata: signals.EventQueueChecksEndMetadata


class EventQueueMerged(EventBase):
    event: typing.Literal["action.queue.merged"]
    type: typing.Literal["action.queue.merged"]
    metadata: signals.EventQueueMergedMetadata


class EventRebase(EventBaseNoMetadata):
    event: typing.Literal["action.rebase"]
    type: typing.Literal["action.rebase"]


class EventRefresh(EventBaseNoMetadata):
    event: typing.Literal["action.refresh"]
    type: typing.Literal["action.refresh"]


class EventRequeue(EventBaseNoMetadata):
    event: typing.Literal["action.requeue"]
    type: typing.Literal["action.requeue"]


class EventRequestReviews(EventBase):
    event: typing.Literal["action.request_reviews"]
    type: typing.Literal["action.request_reviews"]
    metadata: signals.EventRequestReviewsMetadata


class EventReview(EventBase):
    event: typing.Literal["action.review"]
    type: typing.Literal["action.review"]
    metadata: signals.EventReviewMetadata


class EventSquash(EventBaseNoMetadata):
    event: typing.Literal["action.squash"]
    type: typing.Literal["action.squash"]


class EventUnqueue(EventBaseNoMetadata):
    event: typing.Literal["action.unqueue"]
    type: typing.Literal["action.unqueue"]


class EventUpdate(EventBaseNoMetadata):
    event: typing.Literal["action.update"]
    type: typing.Literal["action.update"]


class EventQueueFreezeCreate(EventBase):
    event: typing.Literal["queue.freeze.create"]
    type: typing.Literal["queue.freeze.create"]
    metadata: signals.EventQueueFreezeCreateMetadata


class EventQueueFreezeUpdate(EventBase):
    event: typing.Literal["queue.freeze.update"]
    type: typing.Literal["queue.freeze.update"]
    metadata: signals.EventQueueFreezeUpdateMetadata


class EventQueueFreezeDelete(EventBase):
    event: typing.Literal["queue.freeze.delete"]
    type: typing.Literal["queue.freeze.delete"]
    metadata: signals.EventQueueFreezeDeleteMetadata


class EventQueuePauseCreate(EventBase):
    event: typing.Literal["queue.pause.create"]
    type: typing.Literal["queue.pause.create"]
    metadata: signals.EventQueuePauseCreateMetadata


class EventQueuePauseUpdate(EventBase):
    event: typing.Literal["queue.pause.update"]
    type: typing.Literal["queue.pause.update"]
    metadata: signals.EventQueuePauseUpdateMetadata


class EventQueuePauseDelete(EventBase):
    event: typing.Literal["queue.pause.delete"]
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
            evt.__annotations__["event"].__args__
            for evt in Event.__args__  # type: ignore[attr-defined]
        ],
    ),
)


class GenericEvent(EventBase):
    event: signals.EventName
    type: signals.EventName
    metadata: signals.EventMetadata


@dataclasses.dataclass
class UnsupportedEvent(Exception):
    event: GenericEvent
    message: str = "unsupported event-type, skipping"


class EventLogsSignal(signals.SignalBase):
    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        from mergify_engine import events as evt_utils

        if event not in SUPPORTED_EVENT_NAMES:
            return

        if event in evt_utils.EVENT_NAME_TO_MODEL:
            await evt_utils.insert(
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

    raise UnsupportedEvent(event)
