import dataclasses

from ddtrace import tracer

from mergify_engine import delayed_refresh
from mergify_engine import redis_utils
from mergify_engine.worker import task


@dataclasses.dataclass
class DelayedRefreshService:
    redis_links: redis_utils.RedisLinks
    delayed_refresh_idle_time: float

    _delayed_refresh_task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._delayed_refresh_task = task.TaskRetriedForever(
            "delayed_refresh",
            self.delayed_refresh_task,
            self.delayed_refresh_idle_time,
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._delayed_refresh_task]

    @tracer.wrap("delayed_refresh_task", span_type="worker")
    async def delayed_refresh_task(self) -> None:
        await delayed_refresh.send(self.redis_links)
