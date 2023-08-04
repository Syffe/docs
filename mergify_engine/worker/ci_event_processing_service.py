import dataclasses

from mergify_engine.ci import event_processing
from mergify_engine.worker import task


@dataclasses.dataclass
class CIEventProcessingService(task.SimpleService):
    async def work(self) -> None:
        await event_processing.process_event_streams(self.redis_links)
