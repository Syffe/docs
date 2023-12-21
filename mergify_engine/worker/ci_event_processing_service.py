import dataclasses

from mergify_engine.ci import event_processing
from mergify_engine.worker import task


@dataclasses.dataclass
class CIEventProcessingService(task.SimpleService):
    async def work(self) -> None:
        while not self.main_task.shutdown_requested.is_set():
            pending_work = any(
                (
                    await event_processing.process_workflow_run_stream(
                        self.redis_links,
                    ),
                    await event_processing.process_workflow_job_stream(
                        self.redis_links,
                    ),
                ),
            )
            if not pending_work:
                # NOTE(sileht): nothing to do, sleep a bit
                return
