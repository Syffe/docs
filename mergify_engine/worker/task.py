import asyncio
from collections import abc
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from redis import exceptions as redis_exceptions
import sentry_sdk


LOG = daiquiri.getLogger(__name__)

TaskRetriedForeverFuncT = abc.Callable[[], abc.Awaitable[None]]


class TaskRetriedForever(asyncio.Task[typing.Any]):
    def __init__(
        self,
        name: str,
        func: TaskRetriedForeverFuncT,
        sleep_time: float,
        must_shutdown_first: bool = True,
    ) -> None:
        LOG.info(f"{name} starting")
        self.must_shutdown_first = must_shutdown_first
        self._stopping = asyncio.Event()
        self.name = name
        super().__init__(
            self.with_dedicated_sentry_hub(
                self.loop_and_sleep_forever(func, sleep_time)
            ),
            name=name,
        )
        LOG.info(f"{name} started")

    def stop(self) -> None:
        if self._stopping.is_set():
            raise RuntimeError(f"Worker task `{self.get_name()}` already stopped")
        self._stopping.set()

    @staticmethod
    async def with_dedicated_sentry_hub(coro: abc.Awaitable[None]) -> None:
        with sentry_sdk.Hub(sentry_sdk.Hub.current):
            await coro

    async def loop_and_sleep_forever(
        self, func: TaskRetriedForeverFuncT, sleep_time: float
    ) -> None:
        while not self._stopping.is_set():
            try:
                await func()
            except asyncio.CancelledError:
                LOG.info("%s task killed", self.get_name())
                return
            except redis_exceptions.ConnectionError:
                statsd.increment("redis.client.connection.errors")
                LOG.warning(
                    "%s task lost Redis connection",
                    self.get_name(),
                    exc_info=True,
                )
            except Exception:
                LOG.error("%s task failed", self.get_name(), exc_info=True)

            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=sleep_time)
            except asyncio.CancelledError:
                LOG.info("%s task killed", self.get_name())
                return
            except asyncio.TimeoutError:
                pass

        LOG.debug("%s task exited", self.get_name())


async def stop_wait_and_kill(
    tasks: list[TaskRetriedForever], timeout: float | None = None
) -> None:
    names = [t.name for t in tasks]
    LOG.info("tasks stopping", tasks=names, count=len(tasks))
    for a_task in tasks:
        a_task.stop()
    if tasks:
        _, pendings = await asyncio.wait(tasks, timeout=timeout)
        if pendings:
            pending_names = [t.name for t in pendings]
            LOG.info("tasks killing", tasks=pending_names, count=len(pendings))
            for pending in pendings:
                pending.cancel(msg="shutdown")
            await asyncio.wait(pendings)
    LOG.info("tasks stopped", tasks=names, count=len(tasks))
