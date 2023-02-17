import datetime
import typing

from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.dashboard import subscription
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web.api import statistics as stats_api


if typing.TYPE_CHECKING:
    from mergify_engine import context


class StatisticsAccuracyMeasurement(signals.SignalBase):
    SUPPORTED_EVENT_NAMES: typing.ClassVar[tuple[str, ...]] = ("action.queue.leave",)

    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber | None,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event not in self.SUPPORTED_EVENT_NAMES:
            return

        if not repository.installation.subscription.has_feature(
            subscription.Features.MERGE_QUEUE_STATS
        ):
            return

        metadata = typing.cast(signals.EventQueueLeaveMetadata, metadata)
        if not metadata["merged"]:
            return

        queued_at = metadata["queued_at"].astimezone(datetime.timezone.utc)
        stat_raw = await stats_api.get_time_to_merge_stats_for_queue(
            repository,
            qr_config.QueueName(metadata["queue_name"]),
            metadata["branch"],
            int(queued_at.timestamp()),
        )
        measured_stat = stat_raw["median"]
        if measured_stat is None:
            return

        real_stat = (date.utcnow() - queued_at).total_seconds()

        tags = [
            f"github_login:{repository.installation.owner_login}",
            f"repo:{repository.repo['name']}",
            f"branch:{metadata['branch']}",
            f"queue_name:{metadata['queue_name']}",
        ]

        statsd.gauge(
            "statistics.time_to_merge.accuracy.median_value",
            measured_stat,
            tags=tags,
        )
        statsd.gauge(
            "statistics.time_to_merge.accuracy.real_value",
            real_stat,
            tags=tags,
        )
        statsd.gauge(
            "statistics.time_to_merge.accuracy.seconds_waiting_for_queue_freeze",
            metadata.get("seconds_waiting_for_freeze", 0),
            tags=tags,
        )
