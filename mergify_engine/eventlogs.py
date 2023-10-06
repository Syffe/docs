"""
Managing the old eventlog system which is stored in a Redis database
"""
import dataclasses
import datetime
import itertools
import re
import time
import typing

import daiquiri
import msgpack
import typing_extensions

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.models import enumerations


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)

EVENTLOGS_LONG_RETENTION = datetime.timedelta(days=7)
EVENTLOGS_SHORT_RETENTION = datetime.timedelta(days=1)

DB_SWITCH_KEY = "events-db-switch-marker"


async def use_events_redis_backend(
    repository: "context.Repository",
) -> bool:
    redis = repository.installation.redis.cache

    result = await redis.get(DB_SWITCH_KEY)
    if result is None:
        ts = time.time()
        await redis.set(DB_SWITCH_KEY, ts)
    else:
        ts = float(result.decode())

    return (date.utcnow() - date.fromtimestamp(ts)) < EVENTLOGS_LONG_RETENTION


class EventBase(typing_extensions.TypedDict):
    id: int
    received_at: datetime.datetime
    timestamp: datetime.datetime
    trigger: str
    repository: str
    pull_request: int | None


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


class EventRequestReviewers(EventBase):
    # FIXME(lecrepont01): remove according to MRGFY-2461
    event: typing.Literal["action.request_reviewers"]
    type: typing.Literal["action.request_reviewers"]
    metadata: signals.EventRequestReviewsMetadata


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


def _get_repository_key(
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
) -> str:
    return f"eventlogs-repository/{owner_id}/{repo_id}"


def _get_pull_request_key(
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
    pull_request: github_types.GitHubPullRequestNumber,
) -> str:
    return f"eventlogs/{owner_id}/{repo_id}/{pull_request}"


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
    | EventRequestReviewers
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
)

SUPPORTED_EVENT_NAMES = list(
    itertools.chain(
        *[
            evt.__annotations__["event"].__args__
            for evt in Event.__args__  # type: ignore[attr-defined]
        ]
    )
)

DEFAULT_VERSION = "1.0"


class GenericEvent(EventBase):
    event: signals.EventName
    type: signals.EventName
    metadata: signals.EventMetadata


class RedisGenericEvent(typing_extensions.TypedDict):
    pull_request: int | None
    timestamp: datetime.datetime
    event: signals.EventName
    repository: str
    metadata: signals.EventMetadata
    trigger: str


@dataclasses.dataclass
class UnsupportedEvent(Exception):
    event: GenericEvent
    message: str = "unsupported event-type, skipping"


class EventLogsSignal(signals.SignalBase):
    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber | None,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event not in SUPPORTED_EVENT_NAMES:
            return

        redis = repository.installation.redis.eventlogs

        if repository.installation.subscription.has_feature(
            subscription.Features.EVENTLOGS_LONG
        ):
            retention = EVENTLOGS_LONG_RETENTION
        elif repository.installation.subscription.has_feature(
            subscription.Features.EVENTLOGS_SHORT
        ):
            retention = EVENTLOGS_SHORT_RETENTION
        else:
            return

        pipe = await redis.pipeline()
        now = date.utcnow()
        fields = {
            b"version": DEFAULT_VERSION,
            b"data": msgpack.packb(
                RedisGenericEvent(
                    {
                        "timestamp": now,
                        "event": event,
                        "repository": repository.repo["full_name"],
                        "pull_request": pull_request,
                        "metadata": metadata,
                        "trigger": trigger,
                    }
                ),
                datetime=True,
            ),
        }
        minid = redis_utils.get_expiration_minid(retention)

        repo_key = _get_repository_key(
            repository.installation.owner_id, repository.repo["id"]
        )
        await pipe.xadd(repo_key, fields=fields, minid=minid)
        await pipe.expire(repo_key, int(retention.total_seconds()))

        if pull_request is not None:
            pull_key = _get_pull_request_key(
                repository.installation.owner_id, repository.repo["id"], pull_request
            )
            await pipe.xadd(pull_key, fields=fields, minid=minid)
            await pipe.expire(pull_key, int(retention.total_seconds()))

        await pipe.execute()

        if settings.EVENTLOG_EVENTS_DB_INGESTION:
            from mergify_engine import events as evt_utils

            if event in evt_utils.EVENT_NAME_TO_MODEL:
                await evt_utils.insert(
                    event=event,
                    repository=repository.repo,
                    pull_request=pull_request,
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
        case "action.queue.merged":
            return typing.cast(EventQueueMerged, event)
        case "action.rebase":
            return typing.cast(EventRebase, event)
        case "action.refresh":
            return typing.cast(EventRefresh, event)
        case "action.request_reviewers":  # MRGFY-2461
            return typing.cast(EventRequestReviewers, event)
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


REDIS_CURSOR_RE = re.compile(r"[+-](?:(\d+)-(\d+))?")


async def get(
    repository: "context.Repository",
    page: pagination.CurrentPage,
    pull_request: github_types.GitHubPullRequestNumber | None = None,
    event_type: list[enumerations.EventType] | None = None,
    received_from: datetime.datetime | None = None,
    received_to: datetime.datetime | None = None,
) -> pagination.Page[Event]:
    redis = repository.installation.redis.eventlogs
    if repository.installation.subscription.has_feature(
        subscription.Features.EVENTLOGS_LONG
    ):
        retention = EVENTLOGS_LONG_RETENTION
    elif repository.installation.subscription.has_feature(
        subscription.Features.EVENTLOGS_SHORT
    ):
        retention = EVENTLOGS_SHORT_RETENTION
    else:
        return pagination.Page([], page)

    if pull_request is None:
        key = _get_repository_key(
            repository.installation.owner_id, repository.repo["id"]
        )
    else:
        key = _get_pull_request_key(
            repository.installation.owner_id, repository.repo["id"], pull_request
        )

    older_event = date.utcnow() - retention
    older_event_id = f"{int(older_event.timestamp())}"

    pipe = await redis.pipeline()

    if page.cursor and not REDIS_CURSOR_RE.fullmatch(page.cursor):
        raise pagination.InvalidCursor(page.cursor)

    if not page.cursor:
        # first page
        from_ = "+"
        to_ = older_event_id
        look_backward = False
    elif page.cursor == "-":
        # last page
        from_ = "-"
        to_ = "+"
        look_backward = True
    elif page.cursor.startswith("+"):
        # next page
        from_ = f"({page.cursor[1:]}"
        to_ = older_event_id
        look_backward = False
    elif page.cursor.startswith("-"):
        # prev page
        from_ = f"({page.cursor[1:]}"
        to_ = "+"
        look_backward = True
    else:
        raise pagination.InvalidCursor(page.cursor)

    items: list[tuple[bytes, GenericEvent]] = []
    while len(items) < page.per_page:
        await pipe.xlen(key)
        if look_backward:
            await pipe.xrange(key, min=from_, max=to_, count=page.per_page)
        else:
            await pipe.xrevrange(key, max=from_, min=to_, count=page.per_page)

        total, partial_items = await pipe.execute()

        for raw_id, raw_event in partial_items:
            unpacked_event = msgpack.unpackb(raw_event[b"data"], timestamp=3)
            # add event_type, received_from, received_to filtering from new API
            if (
                (
                    event_type is not None
                    and unpacked_event["event"] not in [e.value for e in event_type]
                )
                or (
                    received_from is not None
                    and unpacked_event["timestamp"] < received_from
                )
                or (
                    received_to is not None
                    and unpacked_event["timestamp"] > received_to
                )
            ):
                continue

            unpacked_event = msgpack.unpackb(raw_event[b"data"], timestamp=3)
            # NOTE(lecrepont01): temporary field deduplication (MRGFY-2555)
            unpacked_event["received_at"] = unpacked_event["timestamp"]
            unpacked_event["type"] = unpacked_event["event"]
            # make an id out of the stream id
            unpacked_event["id"] = int(raw_id.decode().replace("-", ""))

            event = typing.cast(GenericEvent, unpacked_event)

            items.append((raw_id, event))

        # no more data in Redis
        if len(partial_items) < page.per_page:
            break

        from_ = f"({partial_items[-1][0].decode()}"

    if look_backward:
        items = list(reversed(items))

    if page.cursor == "-":
        # NOTE(sileht): On last page, as we look backward and the query doesn't
        # use event ID, we may have more item that we need. So here, we remove
        # those that shouldn't be there.
        items = items[(total % page.per_page) :]

    if page.cursor and items:
        cursor_prev = f"-{items[0][0].decode()}"
    else:
        cursor_prev = None
    if items:
        cursor_next = f"+{items[-1][0].decode()}"
    else:
        cursor_next = None

    events: list[Event] = []
    for _, event in items:
        try:
            events.append(cast_event_item(event))
        except UnsupportedEvent as err:
            LOG.error(err.message, event=err.event)

    # If filtering is done we can't compute the total so we return 0
    compute_total = event_type is None and received_from is None and received_to is None

    return pagination.Page(
        items=events,
        current=page,
        total=total if compute_total else None,
        cursor_prev=cursor_prev,
        cursor_next=cursor_next,
    )
