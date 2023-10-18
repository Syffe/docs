import dataclasses

from mergify_engine.log_embedder import github_action
from mergify_engine.worker import task


@dataclasses.dataclass
class LogEmbedderService(task.SimpleService):
    async def work(self) -> None:
        while not self.main_task.shutdown_requested.is_set():
            pending_work = await github_action.embed_logs(self.redis_links)
            if not pending_work:
                # NOTE(sileht): nothing to do, sleep a bit
                return
