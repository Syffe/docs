import dataclasses

from mergify_engine import redis_utils
from mergify_engine.ci import dump
from mergify_engine.worker import task


@dataclasses.dataclass
class CIDumpStreamService:
    redis_links: redis_utils.RedisLinks
    ci_dump_stream_idle_time: float

    _ci_dump_task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._ci_dump_task = task.TaskRetriedForever(
            "ci_dump_stream",
            self.ci_dump_stream_task,
            self.ci_dump_stream_idle_time,
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._ci_dump_task]

    async def ci_dump_stream_task(self) -> None:
        await dump.dump_event_stream(self.redis_links)
