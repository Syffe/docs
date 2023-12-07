import datetime
import typing

from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.queue import merge_train
from mergify_engine.web.api.queues import estimated_time_to_merge as eta_queues_api
from mergify_engine.web.api.statistics import utils as web_stat_utils


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import partition_rules as partr_config

# The retention time for the eta in Redis
BACKEND_MERGE_QUEUE_STATS_RETENTION = datetime.timedelta(days=60)


def get_statistic_redis_key(
    repository_owner_id: github_types.GitHubAccountIdType,
    repository_id: github_types.GitHubRepositoryIdType,
    partition_name: "partr_config.PartitionRuleName | None",
    pull_number: github_types.GitHubPullRequestNumber,
) -> str:
    return f"merge-queue-stats/repository/{repository_owner_id}/{repository_id}/eta_accuracy/{partition_name}/{pull_number}"


class StatisticsAccuracyMeasurement(signals.SignalBase):
    SUPPORTED_EVENT_NAMES: typing.ClassVar[tuple[str, ...]] = (
        "action.queue.checks_start",
        "action.queue.leave",
    )

    async def __call__(
        self,
        repository: "context.Repository",
        pull_request_number: github_types.GitHubPullRequestNumber | None,
        base_ref: github_types.GitHubRefType | None,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event not in self.SUPPORTED_EVENT_NAMES or pull_request_number is None:
            return

        if not repository.installation.subscription.has_feature(
            subscription.Features.MERGE_QUEUE_STATS,
        ):
            return

        if event == "action.queue.leave":
            await self.queue_leave(repository, pull_request_number, metadata)
        else:
            async with database.create_session() as session:
                await self.queue_checks_start(
                    session,
                    repository,
                    pull_request_number,
                    metadata,
                )

    async def queue_checks_start(
        self,
        session: database.Session,
        repository: "context.Repository",
        pull_request_number: github_types.GitHubPullRequestNumber,
        metadata: signals.EventMetadata,
    ) -> None:
        partition_rules = repository.mergify_config["partition_rules"]
        queue_rules = repository.mergify_config["queue_rules"]

        stats = (
            await web_stat_utils.get_queue_check_durations_per_partition_queue_branch(
                session,
                repository.repo["id"],
                partition_rules.names,
                queue_rules.names,
            )
        )

        metadata = typing.cast(signals.EventQueueChecksStartMetadata, metadata)
        convoy = merge_train.Convoy(
            repository,
            github_types.GitHubRefType(metadata["branch"]),
        )
        await convoy.load_from_redis()

        embarked_pulls_list = await convoy.find_embarked_pull_with_train(
            pull_request_number,
        )
        pipe = await repository.installation.redis.stats.pipeline()

        for train, (car, embarked_pull, _, position) in embarked_pulls_list:
            previous_eta = None
            if position > 0:
                # If queue is in position>0, retrieve eta of previous PR
                prev_pr_number = None
                found = False
                for train_car in train._cars:
                    for ep in train_car.still_queued_embarked_pulls:
                        if ep.user_pull_request_number == pull_request_number:
                            found = True
                            break

                        prev_pr_number = ep.user_pull_request_number

                    if found:
                        break

                if prev_pr_number is not None:
                    prev_pr_redis_key = get_statistic_redis_key(
                        repository.installation.owner_id,
                        repository.repo["id"],
                        train.partition_name,
                        prev_pr_number,
                    )
                    raw_previous_pr_data = (
                        await repository.installation.redis.stats.get(prev_pr_redis_key)
                    )
                    if raw_previous_pr_data is not None:
                        previous_pr_data = json.loads(raw_previous_pr_data)
                        previous_eta = previous_pr_data["eta"]

            eta = await eta_queues_api.get_estimation_from_stats(
                train,
                embarked_pull,
                position,
                stats,
                car,
                previous_eta,
            )
            if eta is None:
                continue

            redis_key = get_statistic_redis_key(
                repository.installation.owner_id,
                repository.repo["id"],
                train.partition_name,
                pull_request_number,
            )

            data = {
                "eta": eta,
            }

            await pipe.set(
                redis_key,
                json.dumps(data),
                ex=int(BACKEND_MERGE_QUEUE_STATS_RETENTION.total_seconds()),
            )

        await pipe.execute()

    async def queue_leave(
        self,
        repository: "context.Repository",
        pull_request_number: github_types.GitHubPullRequestNumber,
        metadata: signals.EventMetadata,
    ) -> None:
        metadata = typing.cast(signals.EventQueueLeaveMetadata, metadata)
        if not metadata["merged"]:
            return

        real_stat = date.utcnow()

        redis_key = get_statistic_redis_key(
            repository.installation.owner_id,
            repository.repo["id"],
            metadata["partition_name"],
            pull_request_number,
        )
        raw_measured_stat = await repository.installation.redis.stats.getdel(redis_key)
        if raw_measured_stat is None:
            return

        measured_stat = json.loads(raw_measured_stat)["eta"]

        tags = [
            f"github_login:{repository.installation.owner_login}",
            f"repo:{repository.repo['name']}",
            f"branch:{metadata['branch']}",
            f"queue_name:{metadata['queue_name']}",
            f"partition_name:{metadata['partition_name']}",
        ]

        statsd.gauge(
            "statistics.estimated_time_of_merge.accuracy.minutes_difference",
            ((real_stat - measured_stat).total_seconds()) / 60,
            tags=tags,
        )

        seconds_waiting_for_queue_freeze = metadata.get("seconds_waiting_for_freeze", 0)
        statsd.gauge(
            "statistics.estimated_time_of_merge.accuracy.minutes_difference_minus_queue_freeze",
            (
                (
                    real_stat
                    - datetime.timedelta(seconds=seconds_waiting_for_queue_freeze)
                    - measured_stat
                ).total_seconds()
            )
            / 60,
            tags=tags,
        )
