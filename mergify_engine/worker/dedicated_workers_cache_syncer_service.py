import dataclasses
import typing

from mergify_engine import github_types
from mergify_engine.worker import stream
from mergify_engine.worker import task


@dataclasses.dataclass
class DedicatedWorkersCacheSyncerService(task.SimpleService):
    loading_priority: typing.ClassVar[int] = 1

    owner_ids: set[github_types.GitHubAccountIdType] = dataclasses.field(
        init=False, default_factory=set
    )

    async def work(self) -> None:
        self.owner_ids = await stream.get_dedicated_worker_owner_ids_from_redis(
            self.redis_links.stream
        )
