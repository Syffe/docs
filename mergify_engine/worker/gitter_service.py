from __future__ import annotations

import asyncio
from collections import abc
import dataclasses
import functools
import typing
import uuid

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


GitterJobId = uuid.UUID

T = typing.TypeVar("T")


@dataclasses.dataclass
class GitterJob(typing.Generic[T]):
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
            worker = task.TaskRetriedForever(
                f"gitter-worker-{i}",
                functools.partial(self._gitter_worker, i),
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
        return self._pools + [self._monitoring_task]

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

    async def _gitter_worker(self, worker_id: int) -> None:
        LOG.debug("gitter worker waiting for job", worker_id=worker_id)
        # NOTE(sileht): asyncio.Queue.get() is uninterrupting even when the
        # asyncio.task is cancelled..., so we have to do polling/sleep...
        try:
            job = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            LOG.debug(
                "gitter worker running job coroutine",
                worker_id=worker_id,
                job_id=job.id,
            )
            try:
                job.task = asyncio.create_task(job.func())
                await job.task
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.debug("fail to run GitterJob", exc_info=True)
                # NOTE(sileht): we ignore exception on purpose, the job caller must
                # reawait the Coroutine to get the result
                pass  # noqa: TC202

            if job.callback is not None:
                LOG.debug(
                    "gitter worker running job coroutine",
                    worker_id=worker_id,
                    job_id=job.id,
                )
                try:
                    await job.callback()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOG.error("fail to run GitterJob callback", exc_info=True)

        finally:
            LOG.debug("gitter worker finished job", worker_id=worker_id, job_id=job.id)
            self._queue.task_done()

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
