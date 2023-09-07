import asyncio
from collections import abc
import contextlib
import dataclasses
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from ddtrace import tracer
from redis import exceptions as redis_exceptions
import sentry_sdk

from mergify_engine import logs
from mergify_engine import redis_utils


LOG = daiquiri.getLogger(__name__)

TaskRetriedForeverFuncT = abc.Callable[[], abc.Awaitable[None]]


@dataclasses.dataclass
class TaskRetriedForever:
    name: str
    func: TaskRetriedForeverFuncT
    sleep_time: float
    must_shutdown_first: bool = False
    shutdown_requested: asyncio.Event = dataclasses.field(
        init=False, default_factory=asyncio.Event
    )

    task: asyncio.Task[None] = dataclasses.field(init=False, repr=False)

    def __post_init__(self) -> None:
        LOG.info("%s starting", self.name)
        self.task = asyncio.create_task(
            self.loop_and_sleep_forever(self.name, self.func, self.sleep_time),
            name=self.name,
        )
        self.task.add_done_callback(self._exited)

    def _exited(self, fut: asyncio.Future[None]) -> None:
        LOG.info("%s task exited", self.name)

    async def loop_and_sleep_forever(
        self, name: str, func: TaskRetriedForeverFuncT, sleep_time: float
    ) -> None:
        logs.WORKER_TASK.set(name)
        with sentry_sdk.Hub(sentry_sdk.Hub.current):
            while not self.shutdown_requested.is_set():
                try:
                    await func()
                except redis_exceptions.ConnectionError:
                    statsd.increment("redis.client.connection.errors")
                    LOG.warning(
                        "%s task lost Redis connection",
                        name,
                        exc_info=True,
                    )
                except Exception:
                    LOG.error("%s task failed", name, exc_info=True)
                except asyncio.CancelledError:
                    if self.shutdown_requested.is_set():
                        raise

                    # FIXME(sileht): This should never ever occurs, but INC-78 prove the reverse
                    # We prefer to ignore it and restart the worker to continue the events processing
                    LOG.warning(
                        "%s task unexpectedly cancelled, ignoring", name, exc_info=True
                    )

                    task = asyncio.current_task()
                    if task is None:
                        raise RuntimeError("No current task during CancelledError")
                    task.uncancel()

                    continue

                with contextlib.suppress(asyncio.TimeoutError):
                    async with asyncio.timeout(self.sleep_time):
                        await self.shutdown_requested.wait()


@dataclasses.dataclass
class SimpleService:
    redis_links: redis_utils.RedisLinks
    idle_time: float

    loading_priority: typing.ClassVar[int] = 100

    main_task: TaskRetriedForever = dataclasses.field(init=False)
    main_task_must_shutdown_first: typing.ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        SIMPLE_SERVICES_REGISTRY[cls.get_name()] = cls

    @classmethod
    def get_name(cls) -> str:
        # NOTE(sileht): drop _service.py suffix
        return cls.__module__.split(".")[-1][:-8].replace("_", "-")

    def __post_init__(self) -> None:
        traced_work = tracer.wrap(self.get_name(), span_type="worker")(self.work)
        self.main_task = TaskRetriedForever(
            name=self.get_name(),
            func=traced_work,
            sleep_time=self.idle_time,
            must_shutdown_first=self.main_task_must_shutdown_first,
        )

    @property
    def tasks(self) -> list[TaskRetriedForever]:
        return [self.main_task]

    async def work(self) -> None:
        raise NotImplementedError()


SIMPLE_SERVICES_REGISTRY: dict[str, type[SimpleService]] = {}


async def stop_and_wait(tasks: list[TaskRetriedForever]) -> None:
    names = [t.name for t in tasks]
    LOG.info("tasks stopping", tasks=names, count=len(tasks))
    if tasks:
        for t in tasks:
            t.shutdown_requested.set()
        pendings = {t.task for t in tasks}
        while pendings:
            # NOTE(sileht): sometime tasks didn't get cancelled correctly (eg:
            # the CancelledError didn't reach the top of the stack), this means
            # somewhere we have a catch-all except that didn't re-raise
            # CancelledError. To workaround the issue, we just re-cancel the
            # task and hope the current frame is not within this buggy
            # try/except.
            for a_task in pendings:
                a_task.cancel(msg="shutdown")
            _, pendings = await asyncio.wait(pendings, timeout=0)
            if pendings:
                LOG.warning(
                    "some tasks are too slow to exits, retrying",
                    tasks=[t.get_name() for t in pendings],
                )
    LOG.info("tasks stopped", tasks=names, count=len(tasks))
