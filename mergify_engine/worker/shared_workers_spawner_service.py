import dataclasses
import functools
import hashlib
import typing

import daiquiri

from mergify_engine import github_types
from mergify_engine.worker import dedicated_workers_cache_syncer_service
from mergify_engine.worker import stream
from mergify_engine.worker import stream_service_base
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class SharedStreamService(stream_service_base.StreamService):
    process_index: int
    worker_idle_time: float
    shared_stream_tasks_per_process: int
    shared_stream_processes: int
    service_dedicated_workers_cache_syncer: dedicated_workers_cache_syncer_service.DedicatedWorkersCacheSyncerService

    loading_priority: typing.ClassVar[int] = 11

    _shared_worker_tasks: list[task.TaskRetriedForever] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __post_init__(self) -> None:
        worker_ids = self.get_shared_worker_ids()
        LOG.info(
            "workers starting",
            count=len(worker_ids),
            process_index=self.process_index,
            shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
        )
        for worker_id in worker_ids:
            self._shared_worker_tasks.append(
                task.TaskRetriedForever(
                    f"shared-{worker_id}",
                    functools.partial(
                        self.shared_stream_worker_task,
                        worker_id,
                    ),
                    self.worker_idle_time,
                ),
            )
        LOG.info(
            "workers started",
            count=len(worker_ids),
            process_index=self.process_index,
            shared_stream_tasks_per_process=self.shared_stream_tasks_per_process,
        )

    def get_shared_worker_ids(self) -> list[int]:
        return list(
            range(
                self.process_index * self.shared_stream_tasks_per_process,
                (self.process_index + 1) * self.shared_stream_tasks_per_process,
            ),
        )

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return self._shared_worker_tasks

    @property
    def global_shared_tasks_count(self) -> int:
        return self.shared_stream_tasks_per_process * self.shared_stream_processes

    async def shared_stream_worker_task(self, shared_worker_id: int) -> None:
        stream_processor = stream.Processor(
            self.redis_links,
            worker_id=f"shared-{shared_worker_id}",
            dedicated_owner_id=None,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        if owner_id in self.service_dedicated_workers_cache_syncer.owner_ids:
            return False

        shared_worker_id = self.get_shared_worker_id_for(
            owner_id,
            self.global_shared_tasks_count,
        )
        return shared_worker_id == stream_processor.worker_id

    @staticmethod
    def get_shared_worker_id_for(
        owner_id: github_types.GitHubAccountIdType,
        global_shared_tasks_count: int,
    ) -> str:
        hashed = hashlib.blake2s(str(owner_id).encode())
        shared_id = int(hashed.hexdigest(), 16) % global_shared_tasks_count
        return f"shared-{shared_id}"
