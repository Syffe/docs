import dataclasses
import datetime
import re
import typing

import msgpack

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import signals
from mergify_engine.dashboard import subscription
from mergify_engine.queue import utils as queue_utils


if typing.TYPE_CHECKING:
    from mergify_engine import context


MERGE_QUEUE_STATS_RETENTION: datetime.timedelta = datetime.timedelta(days=30)
VERSION: str = "1.0"


AvailableStatsKeyT = typing.Literal[
    "time_to_merge",
    "failure_by_reason",
    "checks_duration",
]


@dataclasses.dataclass
class BaseQueueStats:
    queue_name: str
    # List of variables to not include in the return of the `to_dict()`
    # if using an `_` is not enough/appropriate.
    _todict_ignore_vars: typing.ClassVar[typing.Tuple[str, ...]] = ()

    @property
    def redis_key_name(self) -> str:
        # eg: BaseQueueStats -> base_queue_stats
        return (
            re.sub(r"([A-Z][a-z]+)", r"\1_", self.__class__.__name__)
            .rstrip("_")
            .lower()
        )

    def to_dict(self) -> dict[str, typing.Union[str, int]]:
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in self._todict_ignore_vars
        }


TimeToMergeT = typing.List[int]


@dataclasses.dataclass
class TimeToMerge(BaseQueueStats):
    time_seconds: int


# Every key is an `abort_code` of a class that inherits from `BaseAbortReason`
# in mergify_engine/queue/utils.py
class FailureByReasonT(typing.TypedDict):
    PR_AHEAD_DEQUEUED: int
    PR_AHEAD_FAILED_TO_MERGE: int
    PR_WITH_HIGHER_PRIORITY_QUEUED: int
    PR_QUEUED_TWICE: int
    SPECULATIVE_CHECK_NUMBER_REDUCED: int
    CHECKS_TIMEOUT: int
    CHECKS_FAILED: int
    QUEUE_RULE_MISSING: int
    UNEXPECTED_QUEUE_CHANGE: int


@dataclasses.dataclass
class FailureByReason(BaseQueueStats):
    _ABORT_CODE_TO_INT_MAPPING: typing.ClassVar[
        typing.Dict[queue_utils.AbortCodeT, int]
    ] = {
        queue_utils.PrAheadDequeued.abort_code: 1,
        queue_utils.PrAheadFailedToMerge.abort_code: 2,
        queue_utils.PrWithHigherPriorityQueued.abort_code: 3,
        queue_utils.PrQueuedTwice.abort_code: 4,
        queue_utils.SpeculativeCheckNumberReduced.abort_code: 5,
        queue_utils.ChecksTimeout.abort_code: 6,
        queue_utils.ChecksFailed.abort_code: 7,
        queue_utils.QueueRuleMissing.abort_code: 8,
        queue_utils.UnexpectedQueueChange.abort_code: 9,
    }
    _INT_TO_ABORT_CODE_MAPPING: typing.ClassVar[
        typing.Dict[int, queue_utils.AbortCodeT]
    ] = {v: k for k, v in _ABORT_CODE_TO_INT_MAPPING.items()}

    reason_code: int
    reason_code_str: queue_utils.AbortCodeT = dataclasses.field(init=False)

    _todict_ignore_vars = ("reason_code_str",)

    def __post_init__(self):
        self.reason_code_str = self._INT_TO_ABORT_CODE_MAPPING[self.reason_code]

    @classmethod
    def from_reason_code_str(
        cls, reason_code_str: queue_utils.AbortCodeT, **kwargs: typing.Any
    ) -> "FailureByReason":
        return cls(
            reason_code=cls._ABORT_CODE_TO_INT_MAPPING[reason_code_str],
            **kwargs,
        )


ChecksDurationT = typing.List[int]


@dataclasses.dataclass
class ChecksDuration(BaseQueueStats):
    duration_seconds: int


def _get_repository_key(
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
) -> str:
    return f"merge-queue-stats/repository/{owner_id}/{repo_id}"


def get_statistic_redis_key(
    repository_owner_id: github_types.GitHubAccountIdType,
    repository_id: github_types.GitHubRepositoryIdType,
    stat_name: AvailableStatsKeyT,
) -> str:
    return f"{_get_repository_key(repository_owner_id, repository_id)}/{stat_name}"


def _get_seconds_since_datetime(past_datetime: datetime.datetime) -> int:
    return int((date.utcnow() - past_datetime).total_seconds())


def get_stats_from_event_metadata(
    event_name: signals.EventName,
    metadata: signals.EventMetadata,
) -> typing.Optional[BaseQueueStats]:
    if event_name == "action.queue.leave":
        metadata = typing.cast(signals.EventQueueLeaveMetadata, metadata)
        if not metadata["merged"]:
            return None

        return TimeToMerge(
            queue_name=metadata["queue_name"],
            time_seconds=_get_seconds_since_datetime(metadata["queued_at"]),
        )

    elif event_name == "action.queue.checks_end":
        metadata = typing.cast(signals.EventQueueChecksEndMetadata, metadata)
        if metadata["aborted"]:
            if metadata["abort_code"] is None:
                return None

            return FailureByReason.from_reason_code_str(
                queue_name=metadata["queue_name"],
                reason_code_str=metadata["abort_code"],
            )

        checks_ended_at = metadata["speculative_check_pull_request"].get(
            "checks_ended_at"
        )
        if checks_ended_at is None:
            raise RuntimeError(
                "Received an EventQueueChecksEndMetadata without 'checks_ended_at' set"
            )

        return ChecksDuration(
            queue_name=metadata["queue_name"],
            duration_seconds=_get_seconds_since_datetime(checks_ended_at),
        )

    raise RuntimeError(f"Received unhandled event {event_name}")


class StatisticsSignal(signals.SignalBase):
    SUPPORTED_EVENT_NAMES: typing.ClassVar[typing.Tuple[str, ...]] = (
        "action.queue.leave",  # time_to_merge
        "action.queue.checks_end",  # check_duration and failure_by_reason
    )

    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event not in self.SUPPORTED_EVENT_NAMES:
            return

        redis = repository.installation.redis.stats

        if not repository.installation.subscription.has_feature(
            subscription.Features.MERGE_QUEUE_STATS
        ):
            return

        pipe = await redis.pipeline()

        stats = get_stats_from_event_metadata(event, metadata)
        if stats is None:
            return

        stat_redis_key = get_statistic_redis_key(
            repository.installation.owner_id,
            repository.repo["id"],
            typing.cast(
                AvailableStatsKeyT,
                stats.redis_key_name,
            ),
        )
        fields = {
            b"version": VERSION,
            b"data": msgpack.packb(
                stats.to_dict(),
                datetime=True,
            ),
        }

        minid = redis_utils.get_expiration_minid(MERGE_QUEUE_STATS_RETENTION)

        await pipe.xadd(stat_redis_key, fields=fields, minid=minid)
        await pipe.expire(
            stat_redis_key, int(MERGE_QUEUE_STATS_RETENTION.total_seconds())
        )
        await pipe.execute()


# bytes = timestamp
RedisXRangeT = typing.List[typing.Tuple[bytes, typing.Any]]


async def _get_stat_items(
    repository: "context.Repository",
    stat_name: AvailableStatsKeyT,
    queue_name: str,
    start_at: int | None,
    end_at: int | None,
) -> typing.AsyncGenerator[dict[str, typing.Any], None]:
    redis = repository.installation.redis.stats

    redis_repo_key = _get_repository_key(
        repository.installation.owner_id, repository.repo["id"]
    )
    full_redis_key = f"{redis_repo_key}/{stat_name}"

    if start_at is not None:
        older_event_id = str(start_at)
    else:
        older_event_id = "-"

    if end_at is not None:
        newer_event_id = str(end_at)
    else:
        newer_event_id = "+"

    items = await redis.xrange(full_redis_key, min=older_event_id, max=newer_event_id)
    for _, raw_stat in items:
        stat = msgpack.unpackb(raw_stat[b"data"], timestamp=3)
        if stat["queue_name"] == queue_name:
            yield stat


async def get_time_to_merge_stats(
    repository: "context.Repository",
    queue_name: str,
    start_at: int | None,
    end_at: int | None,
) -> TimeToMergeT:
    stats = []
    async for stat in _get_stat_items(
        repository, "time_to_merge", queue_name, start_at, end_at
    ):
        stat_obj = TimeToMerge(**stat)
        stats.append(stat_obj.time_seconds)

    return stats


async def get_checks_duration_stats(
    repository: "context.Repository",
    queue_name: str,
    start_at: int | None,
    end_at: int | None,
) -> ChecksDurationT:
    stats = []
    async for stat in _get_stat_items(
        repository, "checks_duration", queue_name, start_at, end_at
    ):
        stat_obj = ChecksDuration(**stat)
        stats.append(stat_obj.duration_seconds)

    return stats


async def get_failure_by_reason_stats(
    repository: "context.Repository",
    queue_name: str,
    start_at: int | None,
    end_at: int | None,
) -> FailureByReasonT:
    stats = FailureByReasonT(
        {
            "PR_AHEAD_DEQUEUED": 0,
            "PR_AHEAD_FAILED_TO_MERGE": 0,
            "PR_WITH_HIGHER_PRIORITY_QUEUED": 0,
            "PR_QUEUED_TWICE": 0,
            "SPECULATIVE_CHECK_NUMBER_REDUCED": 0,
            "CHECKS_TIMEOUT": 0,
            "CHECKS_FAILED": 0,
            "QUEUE_RULE_MISSING": 0,
            "UNEXPECTED_QUEUE_CHANGE": 0,
        }
    )

    async for stat in _get_stat_items(
        repository, "failure_by_reason", queue_name, start_at, end_at
    ):
        stat_obj = FailureByReason(**stat)
        stats[stat_obj.reason_code_str] += 1

    return stats
