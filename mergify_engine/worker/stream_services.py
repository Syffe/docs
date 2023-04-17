import abc
import collections
import dataclasses
import functools
import hashlib
import time

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
import sentry_sdk

from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.worker import stream
from mergify_engine.worker import stream_lua
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class DedicatedWorkersCacheSyncerService:
    redis_links: redis_utils.RedisLinks
    dedicated_workers_syncer_idle_time: float

    _task: task.TaskRetriedForever = dataclasses.field(init=False)

    owner_ids: set[github_types.GitHubAccountIdType] = dataclasses.field(
        init=False, default_factory=set
    )

    def __post_init__(self) -> None:
        self._task = task.TaskRetriedForever(
            "dedicated workers cache syncer",
            self._sync_dedicated_workers_cache,
            self.dedicated_workers_syncer_idle_time,
        )

    @tracer.wrap("sync_dedicated_workers_cache", span_type="worker")
    async def _sync_dedicated_workers_cache(self) -> None:
        self.owner_ids = await stream.get_dedicated_worker_owner_ids_from_redis(
            self.redis_links.stream
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._task]


@dataclasses.dataclass
class StreamService(abc.ABC):
    redis_links: redis_utils.RedisLinks
    retry_handled_exception_forever: bool

    _owners_cache: stream.OwnerLoginsCache = dataclasses.field(
        init=False, default_factory=stream.OwnerLoginsCache
    )

    @abc.abstractmethod
    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        ...

    @staticmethod
    def extract_owner(
        bucket_org_key: stream_lua.BucketOrgKeyType,
    ) -> github_types.GitHubAccountIdType:
        return github_types.GitHubAccountIdType(int(bucket_org_key.split("~")[1]))

    async def _get_next_bucket_to_proceed(
        self,
        stream_processor: stream.Processor,
    ) -> stream_lua.BucketOrgKeyType | None:
        now = time.time()
        for org_bucket in await self.redis_links.stream.zrangebyscore(
            "streams", min=0, max=now
        ):
            bucket_org_key = stream_lua.BucketOrgKeyType(org_bucket.decode())
            owner_id = self.extract_owner(bucket_org_key)
            if not self.should_handle_owner(stream_processor, owner_id):
                continue

            has_pull_requests_to_process = (
                await stream_processor.select_pull_request_bucket(bucket_org_key)
            )
            if not has_pull_requests_to_process:
                continue

            return bucket_org_key

        return None

    async def _stream_worker_task(self, stream_processor: stream.Processor) -> None:
        logs.WORKER_ID.set(stream_processor.worker_id)

        bucket_org_key = await self._get_next_bucket_to_proceed(stream_processor)
        if bucket_org_key is None:
            return

        LOG.debug(
            "worker %s take org bucket: %s",
            stream_processor.worker_id,
            bucket_org_key,
        )

        statsd.increment(
            "engine.streams.selected",
            tags=[f"worker_id:{stream_processor.worker_id}"],
        )

        owner_id = self.extract_owner(bucket_org_key)
        owner_login_for_tracing = self._owners_cache.get(owner_id)
        try:
            with tracer.trace(
                "org bucket processing",
                span_type="worker",
                resource=owner_login_for_tracing,
            ) as span:
                span.set_tag("gh_owner", owner_login_for_tracing)
                with sentry_sdk.Hub(sentry_sdk.Hub.current) as hub:
                    with hub.configure_scope() as scope:
                        scope.set_tag("gh_owner", owner_login_for_tracing)
                        scope.set_user({"username": owner_login_for_tracing})
                        await stream_processor.consume(
                            bucket_org_key, owner_id, owner_login_for_tracing
                        )
        finally:
            LOG.debug(
                "worker %s release org bucket: %s",
                stream_processor.worker_id,
                bucket_org_key,
            )


@dataclasses.dataclass
class DedicatedStreamService(StreamService):
    process_index: int
    idle_sleep_time: float
    dedicated_stream_processes: int
    dedicated_workers_spawner_idle_time: float
    dedicated_workers_cache_syncer: DedicatedWorkersCacheSyncerService
    _dedicated_workers_spawner_task: task.TaskRetriedForever = dataclasses.field(
        init=False
    )
    _dedicated_worker_tasks: dict[
        github_types.GitHubAccountIdType, task.TaskRetriedForever
    ] = dataclasses.field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._dedicated_workers_spawner_task = task.TaskRetriedForever(
            "dedicated workers spawner",
            self.dedicated_workers_spawner_task,
            self.dedicated_workers_spawner_idle_time,
            must_shutdown_first=True,
        )
        LOG.info("dedicated worker spawner started")

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [
            *list(self._dedicated_worker_tasks.values()),
            self._dedicated_workers_spawner_task,
        ]

    async def dedicated_stream_worker_task(
        self, owner_id: github_types.GitHubAccountIdType
    ) -> None:
        stream_processor = stream.Processor(
            self.redis_links,
            worker_id=f"dedicated-{owner_id}",
            dedicated_owner_id=owner_id,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        return owner_id == stream_processor.dedicated_owner_id

    @tracer.wrap("dedicated_workers_spawner_task", span_type="worker")
    async def dedicated_workers_spawner_task(self) -> None:
        expected_workers = self.get_my_dedicated_worker_ids_from_cache()
        current_workers = set(self._dedicated_worker_tasks.keys())

        to_stop = current_workers - expected_workers
        to_start = expected_workers - current_workers

        if to_stop:
            LOG.info(
                "dedicated workers to stop",
                workers=to_stop,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

        tasks = [self._dedicated_worker_tasks[owner_id] for owner_id in to_stop]
        if tasks:
            await task.stop_and_wait(tasks)

        for owner_id in to_stop:
            del self._dedicated_worker_tasks[owner_id]

        if to_start:
            LOG.info(
                "dedicated workers to start",
                workers=to_start,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )
        for owner_id in to_start:
            self._dedicated_worker_tasks[owner_id] = task.TaskRetriedForever(
                f"dedicated-{owner_id}",
                functools.partial(self.dedicated_stream_worker_task, owner_id),
                self.idle_sleep_time,
            )

        if to_start or to_stop:
            LOG.info(
                "new dedicated workers setup",
                workers=set(self._dedicated_worker_tasks),
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

    def get_my_dedicated_worker_ids_from_cache(
        self,
    ) -> set[github_types.GitHubAccountIdType]:
        if self.dedicated_stream_processes:
            return set(
                sorted(self.dedicated_workers_cache_syncer.owner_ids)[
                    self.process_index :: self.dedicated_stream_processes
                ]
            )
        return set()


@dataclasses.dataclass
class SharedStreamService(StreamService):
    process_index: int
    idle_sleep_time: float
    shared_stream_tasks_per_process: int
    shared_stream_processes: int
    dedicated_workers_cache_syncer: DedicatedWorkersCacheSyncerService

    _shared_worker_tasks: list[task.TaskRetriedForever] = dataclasses.field(
        init=False, default_factory=list
    )

    def __post_init__(self) -> None:
        worker_ids = self.get_shared_worker_ids()
        LOG.info(
            "workers starting",
            count=len(worker_ids),
            process_index=self.process_index,
            shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
        )
        for worker_id in worker_ids:
            self._shared_worker_tasks.append(
                task.TaskRetriedForever(
                    f"shared-{worker_id}",
                    functools.partial(
                        self.shared_stream_worker_task,
                        worker_id,
                    ),
                    self.idle_sleep_time,
                )
            )
        LOG.info(
            "workers started",
            count=len(worker_ids),
            process_index=self.process_index,
            shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
        )

    def get_shared_worker_ids(self) -> list[int]:
        return list(
            range(
                self.process_index * self.shared_stream_tasks_per_process,
                (self.process_index + 1) * self.shared_stream_tasks_per_process,
            )
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return self._shared_worker_tasks

    @property
    def global_shared_tasks_count(self) -> int:
        return self.shared_stream_tasks_per_process * self.shared_stream_processes

    async def shared_stream_worker_task(self, shared_worker_id: int) -> None:
        stream_processor = stream.Processor(
            self.redis_links,
            worker_id=f"shared-{shared_worker_id}",
            dedicated_owner_id=None,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        if owner_id in self.dedicated_workers_cache_syncer.owner_ids:
            return False

        shared_worker_id = self.get_shared_worker_id_for(
            owner_id, self.global_shared_tasks_count
        )
        return shared_worker_id == stream_processor.worker_id

    @staticmethod
    def get_shared_worker_id_for(
        owner_id: github_types.GitHubAccountIdType, global_shared_tasks_count: int
    ) -> str:
        hashed = hashlib.blake2s(str(owner_id).encode())
        shared_id = int(hashed.hexdigest(), 16) % global_shared_tasks_count
        return f"shared-{shared_id}"


@dataclasses.dataclass
class MonitoringStreamService:
    redis_links: redis_utils.RedisLinks
    monitoring_idle_time: float
    shared_stream_tasks_per_process: int
    shared_stream_processes: int
    dedicated_stream_processes: int
    dedicated_workers_cache_syncer: DedicatedWorkersCacheSyncerService

    _stream_monitoring_task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._stream_monitoring_task = task.TaskRetriedForever(
            "monitoring", self.monitoring_task, self.monitoring_idle_time
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._stream_monitoring_task]

    @property
    def global_shared_tasks_count(self) -> int:
        return self.shared_stream_tasks_per_process * self.shared_stream_processes

    @tracer.wrap("monitoring_task", span_type="worker")
    async def monitoring_task(self) -> None:
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
            owner_id = StreamService.extract_owner(
                stream_lua.BucketOrgKeyType(org_bucket.decode())
            )
            if owner_id in self.dedicated_workers_cache_syncer.owner_ids:
                worker_id = f"dedicated-{owner_id}"
            else:
                worker_id = SharedStreamService.get_shared_worker_id_for(
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
