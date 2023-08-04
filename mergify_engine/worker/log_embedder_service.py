import dataclasses

from mergify_engine.log_embedder import github_action
from mergify_engine.worker import task


@dataclasses.dataclass
class LogEmbedderService:
    log_embedder_idle_time: float

    _task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._task = task.TaskRetriedForever(
            "log-embedder",
            self._work,
            self.log_embedder_idle_time,
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._task]

    async def _work(self) -> None:
        while not self._task.shutdown_requested:
            pending_work = await github_action.embed_logs()
            if not pending_work:
                # NOTE(sileht): nothing to do, sleep a bit
                return
