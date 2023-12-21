import dataclasses

from mergify_engine.github_in_postgres import process_events
from mergify_engine.worker import task


@dataclasses.dataclass
class GitHubInPostgresService(task.SimpleService):
    async def work(self) -> None:
        while not self.main_task.shutdown_requested.is_set():
            pending_work = await process_events.store_redis_events_in_pg(
                self.redis_links,
            )
            if not pending_work:
                # NOTE(sileht): nothing to do, sleep a bit
                return
