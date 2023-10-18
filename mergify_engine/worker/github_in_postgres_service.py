import dataclasses

from mergify_engine.github_in_postgres import process_events
from mergify_engine.worker import task


@dataclasses.dataclass
class GitHubInPostgresService(task.SimpleService):
    async def work(self) -> None:
        await process_events.store_redis_events_in_pg(self.redis_links)
