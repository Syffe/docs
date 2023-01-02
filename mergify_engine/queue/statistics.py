from collections import abc
import dataclasses
import datetime
import re
import typing

import msgpack

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine.dashboard import subscription
from mergify_engine.queue import utils as queue_utils


if typing.TYPE_CHECKING:
    from mergify_engine import context


# The maximum time in the past we allow users to query
QUERY_MERGE_QUEUE_STATS_RETENTION: datetime.timedelta = datetime.timedelta(days=30)
# The real retention time for redis
BACKEND_MERGE_QUEUE_STATS_RETENTION: datetime.timedelta = (
    QUERY_MERGE_QUEUE_STATS_RETENTION * 2
)
VERSION: str = "1.0"


class TimestampTooFar(Exception):
    pass


def get_redis_query_older_id() -> int:
    return int((date.utcnow() - QUERY_MERGE_QUEUE_STATS_RETENTION).timestamp() * 1000)


AvailableStatsKeyT = typing.Literal[
    "time_to_merge",
    "failure_by_reason",
    "checks_duration",
]


@dataclasses.dataclass
class BaseQueueStats:
    queue_name: rules.QueueName
    branch_name: str
    # List of variables to not include in the return of the `to_dict()`
    # if using an `_` is not enough/appropriate.
    _todict_ignore_vars: typing.ClassVar[tuple[str, ...]] = ()

    @property
    def redis_key_name(self) -> str:
        # eg: BaseQueueStats -> base_queue_stats
        return (
            re.sub(r"([A-Z][a-z]+)", r"\1_", self.__class__.__name__)
            .rstrip("_")
            .lower()
        )

    def to_dict(self) -> dict[str, str | int]:
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in self._todict_ignore_vars
        }


TimeToMergeT = list[int]


@dataclasses.dataclass
class TimeToMerge(BaseQueueStats):
    time_seconds: int


# Every key is an `abort_code` of a class that inherits from `BaseAbortReason`
# in mergify_engine/queue/utils.py
class FailureByReasonT(typing.TypedDict):
    PR_DEQUEUED: int
    PR_AHEAD_DEQUEUED: int
    PR_AHEAD_FAILED_TO_MERGE: int
    PR_WITH_HIGHER_PRIORITY_QUEUED: int
    PR_QUEUED_TWICE: int
    SPECULATIVE_CHECK_NUMBER_REDUCED: int
    CHECKS_TIMEOUT: int
    CHECKS_FAILED: int
    QUEUE_RULE_MISSING: int
    UNEXPECTED_QUEUE_CHANGE: int
    PR_FROZEN_NO_CASCADING: int
    TARGET_BRANCH_CHANGED: int
    TARGET_BRANCH_MISSING: int
    PR_UNEXPECTEDLY_FAILED_TO_MERGE: int


@dataclasses.dataclass
class FailureByReason(BaseQueueStats):
    _ABORT_CODE_TO_INT_MAPPING: typing.ClassVar[dict[queue_utils.AbortCodeT, int]] = {
        queue_utils.PrAheadDequeued.unqueue_code: 1,
        queue_utils.PrAheadFailedToMerge.unqueue_code: 2,
        queue_utils.PrWithHigherPriorityQueued.unqueue_code: 3,
        queue_utils.PrQueuedTwice.unqueue_code: 4,
        queue_utils.SpeculativeCheckNumberReduced.unqueue_code: 5,
        queue_utils.ChecksTimeout.unqueue_code: 6,
        queue_utils.ChecksFailed.unqueue_code: 7,
        queue_utils.QueueRuleMissing.unqueue_code: 8,
        queue_utils.UnexpectedQueueChange.unqueue_code: 9,
        queue_utils.PrFrozenNoCascading.unqueue_code: 11,
        queue_utils.TargetBranchMissing.unqueue_code: 12,
        queue_utils.TargetBranchChanged.unqueue_code: 13,
        queue_utils.PrDequeued.unqueue_code: 14,
        queue_utils.PrUnexpectedlyFailedToMerge.unqueue_code: 15,
    }
    _INT_TO_UNQUEUE_CODE_MAPPING: typing.ClassVar[dict[int, queue_utils.AbortCodeT]] = {
        v: k for k, v in _ABORT_CODE_TO_INT_MAPPING.items()
    }

    reason_code: int
    reason_code_str: queue_utils.AbortCodeT = dataclasses.field(init=False)

    _todict_ignore_vars = ("reason_code_str",)

    def __post_init__(self) -> None:
        self.reason_code_str = self._INT_TO_UNQUEUE_CODE_MAPPING[self.reason_code]

    @classmethod
    def from_reason_code_str(
        cls, reason_code_str: queue_utils.AbortCodeT, **kwargs: typing.Any
    ) -> "FailureByReason":
        return cls(
            reason_code=cls._ABORT_CODE_TO_INT_MAPPING[reason_code_str],
            **kwargs,
        )


ChecksDurationT = list[int]


@dataclasses.dataclass
class ChecksDuration(BaseQueueStats):
    duration_seconds: int


class QueueChecksOutcomeT(FailureByReasonT):
    SUCCESS: int


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


async def get_stats_from_event_metadata(
    event_name: signals.EventName,
    metadata: signals.EventMetadata,
) -> BaseQueueStats | None:
    if event_name == "action.queue.leave":
        metadata = typing.cast(signals.EventQueueLeaveMetadata, metadata)
        if not metadata["merged"]:
            return None

        return TimeToMerge(
            queue_name=rules.QueueName(metadata["queue_name"]),
            branch_name=metadata["branch"],
            time_seconds=(
                _get_seconds_since_datetime(metadata["queued_at"])
                - metadata.get("seconds_waiting_for_schedule", 0)
                - metadata.get("seconds_waiting_for_freeze", 0)
            ),
        )

    elif event_name == "action.queue.checks_end":
        metadata = typing.cast(signals.EventQueueChecksEndMetadata, metadata)
        if metadata["aborted"]:
            if metadata["abort_code"] is None:
                return None

            return FailureByReason.from_reason_code_str(
                queue_name=rules.QueueName(metadata["queue_name"]),
                branch_name=metadata["branch"],
                reason_code_str=metadata["abort_code"],
            )

        checks_ended_at = metadata["speculative_check_pull_request"].get(
            "checks_ended_at"
        )
        if checks_ended_at is None:
            raise RuntimeError(
                "Received an EventQueueChecksEndMetadata without 'checks_ended_at' set"
            )

        checks_started_at = metadata["speculative_check_pull_request"].get(
            "checks_started_at"
        )
        if checks_started_at is None:
            return None

        duration_seconds = int((checks_ended_at - checks_started_at).total_seconds())

        return ChecksDuration(
            queue_name=rules.QueueName(metadata["queue_name"]),
            branch_name=metadata["branch"],
            duration_seconds=duration_seconds,
        )

    raise RuntimeError(f"Received unhandled event {event_name}")


class StatisticsSignal(signals.SignalBase):
    SUPPORTED_EVENT_NAMES: typing.ClassVar[tuple[str, ...]] = (
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

        stats = await get_stats_from_event_metadata(event, metadata)
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

        minid = redis_utils.get_expiration_minid(BACKEND_MERGE_QUEUE_STATS_RETENTION)
        # NOTE(greesb):
        # We need to manually specify id just for tests to work properly.
        # If we do not manually specify the id using our own `date` (which will be mocked by freezegun),
        # redis is going to automatically pick the id with its own timestamp, which will not be mocked,
        # thus causing the tests to fail because the expected id will not be there.
        id_timestamp = int(date.utcnow().timestamp() * 1000)

        pipe = await redis.pipeline()
        await pipe.xadd(stat_redis_key, id=id_timestamp, fields=fields, minid=minid)
        await pipe.expire(
            stat_redis_key, int(BACKEND_MERGE_QUEUE_STATS_RETENTION.total_seconds())
        )
        await pipe.execute()


# bytes = timestamp
RedisXRangeT = list[tuple[bytes, typing.Any]]


async def _get_stats_items(
    repository: "context.Repository",
    stats_name_list: list[AvailableStatsKeyT],
    older_event_id: str,
    newer_event_id: str,
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
) -> abc.AsyncGenerator[dict[str, typing.Any], None]:
    redis = repository.installation.redis.stats

    if branch_name is None:
        branch_name = utils.extract_default_branch(repository.repo)

    redis_repo_key = _get_repository_key(
        repository.installation.owner_id, repository.repo["id"]
    )

    pipe = await redis.pipeline()
    for stat_name in stats_name_list:
        full_redis_key = f"{redis_repo_key}/{stat_name}"
        await pipe.xrange(full_redis_key, min=older_event_id, max=newer_event_id)

    results = await pipe.execute()
    for result in results:
        for _, raw_stat in result:
            stat = msgpack.unpackb(raw_stat[b"data"], timestamp=3)
            # NOTE(greesb): Replace ".get()" by "[]" when all the stats
            # will have a branch_name (1 month from the time this modification has been merged)
            if (queue_name is None or stat["queue_name"] == queue_name) and stat.get(
                "branch_name"
            ) == branch_name:
                yield stat


async def _get_stats_items_date_range(
    repository: "context.Repository",
    stats_name_list: list[AvailableStatsKeyT],
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> abc.AsyncGenerator[dict[str, typing.Any], None]:
    redis_query_older_id = get_redis_query_older_id()
    if start_at is not None and start_at * 1000 > redis_query_older_id:
        older_event_id = str(start_at * 1000)
    else:
        older_event_id = str(redis_query_older_id)

    if end_at is not None:
        newer_event_id = str(end_at * 1000)
    else:
        newer_event_id = "+"

    async for item in _get_stats_items(
        repository,
        stats_name_list,
        older_event_id,
        newer_event_id,
        queue_name,
        branch_name,
    ):
        yield item


async def _get_stats_items_at_timestamp(
    repository: "context.Repository",
    stats_name_list: list[AvailableStatsKeyT],
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
    at: int | None = None,
) -> abc.AsyncGenerator[dict[str, typing.Any], None]:
    redis_query_older_id = get_redis_query_older_id()
    if at is not None:
        if at * 1000 < redis_query_older_id:
            raise TimestampTooFar()

        at_date = datetime.datetime.fromtimestamp(at)
        older_event_id = str(
            int((at_date - QUERY_MERGE_QUEUE_STATS_RETENTION).timestamp() * 1000)
        )
        newer_event_id = str(at * 1000)
    else:
        older_event_id = str(redis_query_older_id)
        newer_event_id = "+"

    async for item in _get_stats_items(
        repository,
        stats_name_list,
        older_event_id,
        newer_event_id,
        queue_name,
        branch_name,
    ):
        yield item


async def get_time_to_merge_stats(
    repository: "context.Repository",
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
    at: int | None = None,
) -> dict[rules.QueueName, TimeToMergeT]:
    stats: dict[rules.QueueName, TimeToMergeT] = {}

    async for stat in _get_stats_items_at_timestamp(
        repository,
        ["time_to_merge"],
        queue_name=queue_name,
        branch_name=branch_name,
        at=at,
    ):
        stat_obj = TimeToMerge(**stat)
        if stat_obj.queue_name not in stats:
            stats[stat_obj.queue_name] = []

        stats[stat_obj.queue_name].append(stat_obj.time_seconds)

    return stats


async def get_checks_duration_stats(
    repository: "context.Repository",
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> dict[rules.QueueName, ChecksDurationT]:
    stats: dict[rules.QueueName, ChecksDurationT] = {}
    async for stat in _get_stats_items_date_range(
        repository,
        ["checks_duration"],
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    ):
        stat_obj = ChecksDuration(**stat)
        if stat_obj.queue_name not in stats:
            stats[stat_obj.queue_name] = []

        stats[stat_obj.queue_name].append(stat_obj.duration_seconds)

    return stats


BASE_QUEUE_CHECKS_OUTCOME_T_DICT: QueueChecksOutcomeT = QueueChecksOutcomeT(
    {
        "PR_DEQUEUED": 0,
        "PR_AHEAD_DEQUEUED": 0,
        "PR_AHEAD_FAILED_TO_MERGE": 0,
        "PR_WITH_HIGHER_PRIORITY_QUEUED": 0,
        "PR_QUEUED_TWICE": 0,
        "SPECULATIVE_CHECK_NUMBER_REDUCED": 0,
        "CHECKS_TIMEOUT": 0,
        "CHECKS_FAILED": 0,
        "QUEUE_RULE_MISSING": 0,
        "UNEXPECTED_QUEUE_CHANGE": 0,
        "PR_FROZEN_NO_CASCADING": 0,
        "SUCCESS": 0,
        "TARGET_BRANCH_CHANGED": 0,
        "TARGET_BRANCH_MISSING": 0,
        "PR_UNEXPECTEDLY_FAILED_TO_MERGE": 0,
    }
)


async def get_queue_checks_outcome_stats(
    repository: "context.Repository",
    queue_name: rules.QueueName | None = None,
    branch_name: str | None = None,
    start_at: int | None = None,
    end_at: int | None = None,
) -> dict[rules.QueueName, QueueChecksOutcomeT]:
    stats_dict: dict[rules.QueueName, QueueChecksOutcomeT] = {}
    # Retrieve all the checks duration on the same period of time, this will tell us
    # the number of success, since if a check wasn't aborted it is added as a `CheckDuration` stat,
    # and if it was aborted it is added as a `FailureByReason` stat.
    async for stat in _get_stats_items_date_range(
        repository,
        ["checks_duration", "failure_by_reason"],
        queue_name=queue_name,
        branch_name=branch_name,
        start_at=start_at,
        end_at=end_at,
    ):
        if stat["queue_name"] not in stats_dict:
            stats_dict[stat["queue_name"]] = BASE_QUEUE_CHECKS_OUTCOME_T_DICT.copy()

        if "duration_seconds" in stat:
            stats_dict[stat["queue_name"]]["SUCCESS"] += 1
        else:
            stat_obj = FailureByReason(**stat)
            stats_dict[stat["queue_name"]][stat_obj.reason_code_str] += 1

    return stats_dict
