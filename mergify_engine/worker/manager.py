import argparse
import asyncio
import dataclasses
import functools
import os
import signal
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from redis import exceptions as redis_exceptions
import tenacity

from mergify_engine import redis_utils
from mergify_engine import service
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine.clients import github
from mergify_engine.worker import ci_dump_service
from mergify_engine.worker import gitter_service
from mergify_engine.worker import stream_services
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


# NOTE(sileht): for security reason we empty the os.environ at runtime
# We can access it only when modules load.
_DYNO = os.getenv("DYNO")


def get_process_index_from_env() -> int:
    if _DYNO:
        return int(_DYNO.rsplit(".", 1)[-1]) - 1
    return 0


def wait_before_next_retry(retry_state: tenacity.RetryCallState) -> typing.Any:
    return retry_state.next_action.__dict__["sleep"]


async def ping_redis(
    redis: redis_utils.RedisStream | redis_utils.RedisCache,
    redis_name: str,
) -> None:
    def retry_log(retry_state: tenacity.RetryCallState) -> None:
        statsd.increment("redis.client.connection.errors")
        LOG.warning(
            "Couldn't connect to Redis %s, retrying in %d seconds...",
            redis_name,
            wait_before_next_retry(retry_state),
        )

    async for attempt in tenacity.AsyncRetrying(
        wait=tenacity.wait_exponential(multiplier=0.2, max=5),
        retry=tenacity.retry_if_exception_type(redis_exceptions.ConnectionError),
        before_sleep=retry_log,
    ):
        with attempt:
            await redis.ping()


class ServiceProtocol(typing.Protocol):
    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        ...


ServiceT = typing.TypeVar("ServiceT", bound=ServiceProtocol)
ServiceNameT = typing.Literal[
    "shared-stream",
    "dedicated-stream",
    "stream-monitoring",
    "delayed-refresh",
    "gitter",
    "ci-dump",
]
ServiceNamesT = set[ServiceNameT]
AVAILABLE_WORKER_SERVICES = set(ServiceNameT.__dict__["__args__"])


def asyncio_event_set_by_default() -> asyncio.Event:
    evt = asyncio.Event()
    evt.set()
    return evt


@dataclasses.dataclass
class ServiceManager:
    enabled_services: ServiceNamesT = dataclasses.field(
        default_factory=lambda: AVAILABLE_WORKER_SERVICES.copy()
    )

    # DelayedRefreshService
    delayed_refresh_idle_time: float = 60

    # DedicatedWorkersCacheSyncerService
    dedicated_workers_syncer_idle_time: float = 30

    # DedicatedStreamService
    dedicated_workers_spawner_idle_time: float = 60
    dedicated_stream_processes: int = settings.DEDICATED_STREAM_PROCESSES

    # SharedStreamService
    shared_stream_tasks_per_process: int = settings.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = settings.SHARED_STREAM_PROCESSES

    # SharedStreamService & DedicatedStreamService
    idle_sleep_time: float = 0.42
    process_index: int = dataclasses.field(default_factory=get_process_index_from_env)
    retry_handled_exception_forever: bool = True

    # GitterService
    gitter_concurrent_jobs: int = settings.MAX_GITTER_CONCURRENT_JOBS

    # MonitoringStreamService & GitterService
    monitoring_idle_time: float = 60

    # CIDumpService
    ci_dump_idle_time: float = 60

    _redis_links: redis_utils.RedisLinks = dataclasses.field(
        init=False, default_factory=lambda: redis_utils.RedisLinks(name="worker")
    )
    _services: list[ServiceProtocol] = dataclasses.field(default_factory=list)

    _stopped: asyncio.Event = dataclasses.field(
        init=False, default_factory=asyncio_event_set_by_default
    )
    _stop_task: asyncio.Task[None] | None = dataclasses.field(init=False, default=None)

    def get_service(self, _type: type[ServiceT]) -> ServiceT | None:
        for serv in self._services:
            if isinstance(serv, _type):
                return serv
        return None

    async def start(self) -> None:
        if self._stop_task is not None:
            raise RuntimeError("Worker can't be restarted")

        self._stopped.clear()

        LOG.info(
            "worker process start",
            enabled_services=self.enabled_services,
            process_index=self.process_index,
            dedicated_stream_processes=self.dedicated_stream_processes,
            shared_stream_tasks_per_process=self.shared_stream_processes,
            shared_stream_processes=self.shared_stream_processes,
        )

        await ping_redis(self._redis_links.stream, "Stream")
        await ping_redis(self._redis_links.cache, "Cache")

        await github.GitHubAppInfo.warm_cache(self._redis_links.cache)

        if "gitter" in self.enabled_services:
            gitter_serv = gitter_service.GitterService(
                concurrent_jobs=self.gitter_concurrent_jobs,
                monitoring_idle_time=self.monitoring_idle_time,
                idle_sleep_time=self.idle_sleep_time,
            )
            self._services.append(gitter_serv)
        dedicated_workers_cache_syncer = stream_services.DedicatedWorkersCacheSyncerService(
            self._redis_links,
            dedicated_workers_syncer_idle_time=self.dedicated_workers_syncer_idle_time,
        )
        self._services.append(dedicated_workers_cache_syncer)

        if "shared-stream" in self.enabled_services:
            self._services.append(
                stream_services.SharedStreamService(
                    self._redis_links,
                    retry_handled_exception_forever=self.retry_handled_exception_forever,
                    process_index=self.process_index,
                    idle_sleep_time=self.idle_sleep_time,
                    shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
                    shared_stream_processes=self.shared_stream_processes,
                    dedicated_workers_cache_syncer=dedicated_workers_cache_syncer,
                )
            )

        if "dedicated-stream" in self.enabled_services:
            self._services.append(
                stream_services.DedicatedStreamService(
                    self._redis_links,
                    retry_handled_exception_forever=self.retry_handled_exception_forever,
                    process_index=self.process_index,
                    idle_sleep_time=self.idle_sleep_time,
                    dedicated_stream_processes=self.dedicated_stream_processes,
                    dedicated_workers_spawner_idle_time=self.dedicated_workers_spawner_idle_time,
                    dedicated_workers_cache_syncer=dedicated_workers_cache_syncer,
                )
            )

        if "delayed-refresh" in self.enabled_services:
            # Circular dependency issue
            from mergify_engine.worker import delayed_refresh_service

            self._services.append(
                delayed_refresh_service.DelayedRefreshService(
                    self._redis_links,
                    delayed_refresh_idle_time=self.delayed_refresh_idle_time,
                )
            )

        if "stream-monitoring" in self.enabled_services:
            self._services.append(
                stream_services.MonitoringStreamService(
                    self._redis_links,
                    monitoring_idle_time=self.monitoring_idle_time,
                    shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
                    shared_stream_processes=self.shared_stream_processes,
                    dedicated_stream_processes=self.dedicated_stream_processes,
                    dedicated_workers_cache_syncer=dedicated_workers_cache_syncer,
                )
            )

        if "ci-dump" in self.enabled_services:
            self._services.append(
                ci_dump_service.CIDumpService(
                    self._redis_links,
                    ci_dump_idle_time=self.ci_dump_idle_time,
                )
            )

    async def _stop_all_tasks(self) -> None:
        tasks_that_must_shutdown_first: list[task.TaskRetriedForever] = []
        other_tasks: list[task.TaskRetriedForever] = []
        for serv in self._services:
            for a_task in serv.tasks:
                if a_task.must_shutdown_first:
                    tasks_that_must_shutdown_first.append(a_task)
                else:
                    other_tasks.append(a_task)

        await task.stop_and_wait(tasks_that_must_shutdown_first)
        await task.stop_and_wait(other_tasks)

    async def _shutdown(self) -> None:
        LOG.info("shutdown start")
        await self._stop_all_tasks()

        LOG.info("redis finalizing")
        await self._redis_links.shutdown_all()
        LOG.info("redis finalized")

        self._stopped.set()
        LOG.info("shutdown finished")

    def stop(self) -> None:
        if self._stop_task is not None:
            raise RuntimeError("Worker is already stopping")
        self._stop_task = asyncio.create_task(self._shutdown(), name="shutdown")

    async def wait_shutdown_complete(self) -> None:
        await self._stopped.wait()
        if self._stop_task:
            await self._stop_task
            self._stop_task = None
        self._services = []

    def stop_with_signal(self, signame: str) -> None:
        if self._stop_task is None:
            LOG.info("got signal %s: cleanly shutdown workers", signame)
            self.stop()
        else:
            LOG.info("got signal %s: ignoring, shutdown already in process", signame)

    def setup_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self.stop_with_signal, signame),
            )


async def run_forever(
    enabled_services: ServiceNamesT = AVAILABLE_WORKER_SERVICES,
) -> None:
    worker = ServiceManager(enabled_services=enabled_services)
    await worker.start()
    worker.setup_signals()
    await worker.wait_shutdown_complete()
    LOG.info("Exiting...")


def ServicesSet(v: str) -> ServiceNamesT:
    values = set(v.strip().split(","))
    for value in values:
        if value not in AVAILABLE_WORKER_SERVICES:
            raise ValueError(f"{v} is not a valid service")
    return typing.cast(ServiceNamesT, values)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mergify Engine Worker")
    parser.add_argument(
        "--enabled-services",
        type=ServicesSet,
        default=",".join(AVAILABLE_WORKER_SERVICES),
    )
    args = parser.parse_args(argv)

    service.setup("worker")
    signals.register()
    return asyncio.run(run_forever(enabled_services=args.enabled_services))
