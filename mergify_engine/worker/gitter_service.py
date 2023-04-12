from __future__ import annotations

import asyncio
from collections import abc
import dataclasses
import functools
import logging
import typing
import uuid

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
import sentry_sdk

from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


GitterJobId = uuid.UUID

T = typing.TypeVar("T")


@dataclasses.dataclass
class GitterJob(typing.Generic[T]):
    owner_login_for_tracing: github_types.GitHubLogin
    logger: "logging.LoggerAdapter[logging.Logger]"
    func: abc.Callable[[], abc.Coroutine[None, None, T]]
    callback: abc.Callable[[], abc.Coroutine[None, None, None]] | None = None
    id: GitterJobId = dataclasses.field(init=False, default_factory=uuid.uuid4)
    task: asyncio.Task[T] | None = None

    def result(self) -> T:
        if self.task is None:
            raise asyncio.InvalidStateError("job not yet started")
        try:
            return self.task.result()
        finally:
            if GitterService._instance is None:
                raise RuntimeError("GitterService is not initialized")
            del GitterService._instance._jobs[self.id]


@dataclasses.dataclass
class GitterService:
    concurrent_jobs: int
    monitoring_idle_time: float
    idle_sleep_time: float

    _pools: list[task.TaskRetriedForever] = dataclasses.field(
        init=False, default_factory=list
    )
    _monitoring_task: task.TaskRetriedForever = dataclasses.field(init=False)
    _queue: asyncio.Queue[GitterJob[typing.Any]] = dataclasses.field(init=False)
    _jobs: dict[GitterJobId, GitterJob[typing.Any]] = dataclasses.field(
        init=False, default_factory=dict
    )

    _instance: typing.ClassVar["GitterService" | None] = None

    def __post_init__(self) -> None:
        self._queue = asyncio.Queue()
        for i in range(self.concurrent_jobs):
            worker_id = f"gitter-worker-{i}"
            worker = task.TaskRetriedForever(
                worker_id,
                functools.partial(self._gitter_worker, worker_id),
                self.idle_sleep_time,
            )
            self._pools.append(worker)

        self._monitoring_task = task.TaskRetriedForever(
            "gitter-monitoring",
            self._monitoring,
            self.monitoring_idle_time,
        )
        GitterService._instance = self

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [*self._pools, self._monitoring_task]

    async def _monitoring(self) -> None:
        LOG.debug("running gitter monitoring")
        queued = 0
        running = 0
        finished = 0
        for job in self._jobs.values():
            if job.task is None:
                queued += 1
            elif job.task.done():
                finished += 1
            else:
                running += 1
        statsd.gauge("engine.gitter.jobs.queued", queued)
        statsd.gauge("engine.gitter.jobs.running", running)
        statsd.gauge("engine.gitter.jobs.finished", finished)

    async def _gitter_worker(self, gitter_worker_id: str) -> None:
        logs.WORKER_ID.set(gitter_worker_id)

        LOG.debug("gitter worker waiting for job")
        # NOTE(sileht): asyncio.Queue.get() is uninterrupting even when the
        # asyncio.task is cancelled..., so we have to do polling/sleep...
        try:
            job = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        with tracer.trace(
            "gitter_worker", span_type="worker", resource=job.owner_login_for_tracing
        ) as span:
            span.set_tag("gh_owner", job.owner_login_for_tracing)
            with sentry_sdk.Hub(sentry_sdk.Hub.current) as hub:
                with hub.configure_scope() as scope:
                    scope.set_tag("gh_owner", job.owner_login_for_tracing)
                    scope.set_user({"username": job.owner_login_for_tracing})

                    try:
                        await self._gitter_worker_run_job(job)
                    finally:
                        self._queue.task_done()

    @staticmethod
    async def _gitter_worker_run_job(job: GitterJob[typing.Any]) -> None:
        job.logger.debug("gitter worker running job func", job_id=job.id)
        try:
            job.task = asyncio.create_task(job.func())
            await job.task
        except Exception:
            job.logger.debug(
                "gitter worker job func failed", job_id=job.id, exc_info=True
            )
            # NOTE(sileht): we ignore exception on purpose, the job caller must
            # reawait the Coroutine to get the result
            pass
        else:
            job.logger.debug("gitter worker finished job func", job_id=job.id)

        if job.callback is not None:
            job.logger.debug("gitter worker running job callback", job_id=job.id)
            try:
                await job.callback()
            except Exception:
                job.logger.error(
                    "gitter worker job callback failed", job_id=job.id, exc_info=True
                )
            else:
                job.logger.debug("gitter worker finished job callback", job_id=job.id)

    def _send_job(self, job: GitterJob[typing.Any]) -> None:
        self._jobs[job.id] = job
        self._queue.put_nowait(job)

    def _get_job(self, job_id: GitterJobId) -> GitterJob[typing.Any] | None:
        return self._jobs.get(job_id)


def send_job(job: GitterJob[typing.Any]) -> None:
    if GitterService._instance is None:
        raise RuntimeError("GitterService is not initialized")
    GitterService._instance._send_job(job)


def get_job(job_id: GitterJobId) -> GitterJob[typing.Any] | None:
    if GitterService._instance is None:
        raise RuntimeError("GitterService is not initialized")
    return GitterService._instance._jobs.get(job_id)
