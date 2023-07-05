import dataclasses

from mergify_engine import redis_utils
from mergify_engine.ci import event_processing
from mergify_engine.worker import task


@dataclasses.dataclass
class CIEventProcessingService:
    redis_links: redis_utils.RedisLinks
    ci_event_processing_idle_time: float

    _ci_event_processing_task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._ci_event_processing_task = task.TaskRetriedForever(
            "ci_event_processing",
            self.ci_event_processing_task,
            self.ci_event_processing_idle_time,
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._ci_event_processing_task]

    async def ci_event_processing_task(self) -> None:
        await event_processing.process_event_streams(self.redis_links)
