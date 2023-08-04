import dataclasses

from mergify_engine import delayed_refresh
from mergify_engine.worker import task


@dataclasses.dataclass
class DelayedRefreshService(task.SimpleService):
    async def work(self) -> None:
        await delayed_refresh.send(self.redis_links)
