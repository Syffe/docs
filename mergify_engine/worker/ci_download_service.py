import dataclasses

from mergify_engine import redis_utils
from mergify_engine.ci import download
from mergify_engine.worker import task


@dataclasses.dataclass
class CIDownloadService:
    redis_links: redis_utils.RedisLinks
    ci_download_idle_time: float

    _ci_download_task: task.TaskRetriedForever = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._ci_download_task = task.TaskRetriedForever(
            "ci_download",
            self.ci_download_task,
            self.ci_download_idle_time,
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [self._ci_download_task]

    async def ci_download_task(self) -> None:
        await download.download_next_repositories(self.redis_links)
