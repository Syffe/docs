import collections
import dataclasses
import time

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine import worker_pusher
from mergify_engine.worker import dedicated_workers_cache_syncer_service
from mergify_engine.worker import shared_workers_spawner_service
from mergify_engine.worker import stream_lua
from mergify_engine.worker import stream_service_base
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class MonitoringStreamService(task.SimpleService):
    shared_stream_tasks_per_process: int
    shared_stream_processes: int
    dedicated_stream_processes: int
    service_dedicated_workers_cache_syncer: dedicated_workers_cache_syncer_service.DedicatedWorkersCacheSyncerService

    @property
    def global_shared_tasks_count(self) -> int:
        return self.shared_stream_tasks_per_process * self.shared_stream_processes

    async def work(self) -> None:
        # TODO(sileht): maybe also graph streams that are before `now`
        # to see the diff between the backlog and the upcoming work to do
        now = time.time()
        org_buckets: list[
            tuple[bytes, float]
        ] = await self.redis_links.stream.zrangebyscore(
            "streams",
            min=0,
            max=now,
            withscores=True,
        )
        # NOTE(sileht): The latency may not be exact with the next StreamSelector
        # based on hash+modulo
        if len(org_buckets) > self.global_shared_tasks_count:
            latency = now - org_buckets[self.global_shared_tasks_count][1]
            statsd.timing("engine.streams.latency", latency)
        else:
            statsd.timing("engine.streams.latency", 0)

        statsd.gauge("engine.streams.backlog", len(org_buckets))
        statsd.gauge("engine.workers.count", self.global_shared_tasks_count)
        statsd.gauge(
            "engine.processes.count",
            self.shared_stream_processes,
            tags=["type:shared"],
        )
        statsd.gauge(
            "engine.processes.count",
            self.dedicated_stream_processes,
            tags=["type:dedicated"],
        )
        statsd.gauge(
            "engine.workers-per-process.count", self.shared_stream_tasks_per_process
        )

        # TODO(sileht): maybe we can do something with the bucket scores to
        # build a latency metric
        bucket_backlogs: dict[
            worker_pusher.Priority, dict[str, int]
        ] = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))

        for org_bucket, _ in org_buckets:
            owner_id = stream_service_base.StreamService.extract_owner(
                stream_lua.BucketOrgKeyType(org_bucket.decode())
            )
            if owner_id in self.service_dedicated_workers_cache_syncer.owner_ids:
                worker_id = f"dedicated-{owner_id}"
            else:
                worker_id = shared_workers_spawner_service.SharedStreamService.get_shared_worker_id_for(
                    owner_id, self.global_shared_tasks_count
                )
            bucket_contents: list[
                tuple[bytes, float]
            ] = await self.redis_links.stream.zrangebyscore(
                org_bucket, min=0, max="+inf", withscores=True
            )
            for _, score in bucket_contents:
                prio = worker_pusher.get_priority_level_from_score(score)
                bucket_backlogs[prio][worker_id] += 1

        for priority, bucket_backlog in bucket_backlogs.items():
            for worker_id, length in bucket_backlog.items():
                statsd.gauge(
                    "engine.buckets.backlog",
                    length,
                    tags=[f"priority:{priority.name}", f"worker_id:{worker_id}"],
                )
