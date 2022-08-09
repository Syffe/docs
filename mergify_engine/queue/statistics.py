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


@dataclasses.dataclass
class TimeToMerge(BaseQueueStats):
    time_seconds: int


@dataclasses.dataclass
class FailureByReason(BaseQueueStats):
    _ABORT_CODE_TO_INT_MAPPING: typing.ClassVar[typing.Dict[str, int]] = {
        queue_utils.PrAheadDequeued.get_abort_code(): 1,
        queue_utils.PrAheadFailedToMerge.get_abort_code(): 2,
        queue_utils.PrWithHigherPriorityQueued.get_abort_code(): 3,
        queue_utils.PrQueuedTwice.get_abort_code(): 4,
        queue_utils.SpeculativeCheckNumberReduced.get_abort_code(): 5,
        queue_utils.ChecksTimeout.get_abort_code(): 6,
        queue_utils.ChecksFailed.get_abort_code(): 7,
        queue_utils.QueueRuleMissing.get_abort_code(): 8,
        queue_utils.UnexpectedQueueChange.get_abort_code(): 9,
    }
    _INT_TO_ABORT_CODE_MAPPING: typing.ClassVar[typing.Dict[int, str]] = {
        v: k for k, v in _ABORT_CODE_TO_INT_MAPPING.items()
    }

    reason_code_str: str
    reason_code: int = dataclasses.field(init=False)

    _todict_ignore_vars = ("reason_code_str",)

    def __post_init__(self):
        self.reason_code = self._ABORT_CODE_TO_INT_MAPPING[self.reason_code_str]


@dataclasses.dataclass
class ChecksDuration(BaseQueueStats):
    duration_seconds: int


def _get_repository_key(
    owner_id: github_types.GitHubAccountIdType,
    repo_id: github_types.GitHubRepositoryIdType,
) -> str:
    return f"merge-queue-stats/repository/{owner_id}/{repo_id}"


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

            return FailureByReason(
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
        if redis is None:
            return

        if not repository.installation.subscription.has_feature(
            subscription.Features.MERGE_QUEUE_STATS
        ):
            return

        base_repo_key = _get_repository_key(
            repository.installation.owner_id, repository.repo["id"]
        )

        pipe = await redis.pipeline()

        stats = get_stats_from_event_metadata(event, metadata)
        if stats is None:
            return

        stat_redis_key = f"{base_repo_key}/{stats.redis_key_name}"
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
