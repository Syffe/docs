import asyncio
from collections import abc
import dataclasses

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
from redis import exceptions as redis_exceptions
import sentry_sdk


LOG = daiquiri.getLogger(__name__)

TaskRetriedForeverFuncT = abc.Callable[[], abc.Awaitable[None]]


@dataclasses.dataclass
class TaskRetriedForever:
    name: str
    func: TaskRetriedForeverFuncT
    sleep_time: float
    must_shutdown_first: bool = False

    task: asyncio.Task[None] = dataclasses.field(init=False, repr=False)

    def __post_init__(self) -> None:
        LOG.info(f"{self.name} starting")
        self.task = asyncio.create_task(
            self.loop_and_sleep_forever(self.name, self.func, self.sleep_time),
            name=self.name,
        )
        self.task.add_done_callback(self._exited)

    def _exited(self, fut: asyncio.Future[None]) -> None:
        LOG.info("%s task exited", self.name)

    @staticmethod
    async def loop_and_sleep_forever(
        name: str, func: TaskRetriedForeverFuncT, sleep_time: float
    ) -> None:
        with sentry_sdk.Hub(sentry_sdk.Hub.current):
            while True:
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

                await asyncio.sleep(sleep_time)


async def stop_and_wait(tasks: list[TaskRetriedForever]) -> None:
    names = [t.name for t in tasks]
    LOG.info("tasks stopping", tasks=names, count=len(tasks))
    if tasks:
        pendings = {t.task for t in tasks}
        for a_task in pendings:
            a_task.cancel(msg="shutdown")
        while pendings:
            _, pendings = await asyncio.wait(pendings, timeout=0.42)
            if pendings:
                LOG.warning(
                    "some tasks are too slow to exits, retrying",
                    tasks=[t.get_name() for t in pendings],
                )
    LOG.info("tasks stopped", tasks=names, count=len(tasks))
