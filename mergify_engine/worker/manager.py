import argparse
import asyncio
import dataclasses
import functools
import importlib
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
    "dedicated-workers-spawner",
    "shared-workers-spawner",
    "gitter",
    "stream-monitoring",
    "delayed-refresh",
    "ci-event-processing",
    "log-embedder",
    "dedicated-workers-cache-syncer",
    "event-forwarder",
]
ServiceNamesT = set[ServiceNameT]
AVAILABLE_WORKER_SERVICES = set(ServiceNameT.__dict__["__args__"])

# This ensures all declared services are imported and named correctly
for service_name in AVAILABLE_WORKER_SERVICES:
    # nosemgrep: python.lang.security.audit.non-literal-import.non-literal-import
    importlib.import_module(
        f"mergify_engine.worker.{service_name.replace('-','_')}_service"
    )


def asyncio_event_set_by_default() -> asyncio.Event:
    evt = asyncio.Event()
    evt.set()
    return evt


@dataclasses.dataclass
class ServiceManager:
    enabled_services: ServiceNamesT = dataclasses.field(
        default_factory=lambda: AVAILABLE_WORKER_SERVICES.copy()
    )
    _mandatory_services: typing.ClassVar[ServiceNamesT] = {
        "dedicated-workers-cache-syncer"
    }

    # DedicatedWorkersCacheSyncerService
    dedicated_workers_cache_syncer_idle_time: float = 30

    # DedicatedStreamService
    dedicated_workers_spawner_idle_time: float = 60
    dedicated_stream_processes: int = settings.DEDICATED_STREAM_PROCESSES

    # SharedStreamService
    shared_workers_spawner_idle_time: float = 60  # Not used
    shared_stream_tasks_per_process: int = settings.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = settings.SHARED_STREAM_PROCESSES

    # SharedStreamService & DedicatedStreamService
    worker_idle_time: float = 0.42
    process_index: int = dataclasses.field(default_factory=get_process_index_from_env)
    retry_handled_exception_forever: bool = True

    # GitterService
    gitter_concurrent_jobs: int = settings.MAX_GITTER_CONCURRENT_JOBS
    gitter_idle_time: float = 60
    gitter_worker_idle_time: float = 60

    # MonitoringStreamService
    stream_monitoring_idle_time: float = 60

    # DelayedRefreshService
    delayed_refresh_idle_time: float = 60

    # CIDownloadService
    ci_event_processing_idle_time: float = 30

    # LogEmbedderProcessingService
    log_embedder_idle_time: float = 60

    # EventForwarderService
    event_forwarder_idle_time: float = 5

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

        if not self.enabled_services:
            self._stopped.set()
            LOG.info("worker has no services enabled, shutdown..")
            return

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

        common_fields = [f.name for f in dataclasses.fields(task.SimpleService)]
        for name, service_class in sorted(
            task.SIMPLE_SERVICES_REGISTRY.items(),
            key=lambda s: s[1].loading_priority,
        ):
            if not (name in self.enabled_services or name in self._mandatory_services):
                continue

            LOG.info("%s loading", name)
            serv = service_class(
                self._redis_links,
                idle_time=getattr(self, f"{name.replace('-', '_')}_idle_time"),
                **{
                    field.name: getattr(self, field.name)
                    for field in dataclasses.fields(service_class)
                    if field.init and field.name not in common_fields
                },
            )
            # NOTE(sileht): make service available for dependant services
            setattr(self, f"service_{name.replace('-', '_')}", serv)
            self._services.append(serv)

    async def _stop_all_tasks(self) -> None:
        tasks_that_must_shutdown_first, other_tasks = self._get_tasks_to_stops()
        await task.stop_and_wait(tasks_that_must_shutdown_first)
        await task.stop_and_wait(other_tasks)

    def _get_tasks_to_stops(
        self,
    ) -> tuple[list[task.TaskRetriedForever], list[task.TaskRetriedForever]]:
        tasks_that_must_shutdown_first: list[task.TaskRetriedForever] = []
        other_tasks: list[task.TaskRetriedForever] = []
        for serv in self._services:
            for a_task in serv.tasks:
                if a_task.must_shutdown_first:
                    tasks_that_must_shutdown_first.append(a_task)
                else:
                    other_tasks.append(a_task)

        return tasks_that_must_shutdown_first, other_tasks

    async def _shutdown(self) -> None:
        LOG.info("shutdown start")
        await self._stop_all_tasks()

        LOG.debug("redis finalizing")
        await self._redis_links.shutdown_all()
        LOG.debug("redis finalized")

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


def ServicesSet(v: str) -> ServiceNamesT:
    values = set(v.strip().split(","))
    enabled_services = set()
    invalid_services = set()
    for value in values:
        if not value:
            continue
        if value in AVAILABLE_WORKER_SERVICES:
            enabled_services.add(value)
        else:
            invalid_services.add(value)

    if invalid_services:
        LOG.error("Invalid services have been ignored: %s", invalid_services)

    return typing.cast(ServiceNamesT, enabled_services)


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
