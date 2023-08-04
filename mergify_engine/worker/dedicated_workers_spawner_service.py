import dataclasses
import functools
import typing

import daiquiri

from mergify_engine import github_types
from mergify_engine.worker import dedicated_workers_cache_syncer_service
from mergify_engine.worker import stream
from mergify_engine.worker import stream_service_base
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class DedicatedStreamService(stream_service_base.StreamService):
    process_index: int
    worker_idle_time: float
    dedicated_stream_processes: int
    service_dedicated_workers_cache_syncer: dedicated_workers_cache_syncer_service.DedicatedWorkersCacheSyncerService

    loading_priority: typing.ClassVar[int] = 10

    _dedicated_worker_tasks: dict[
        github_types.GitHubAccountIdType, task.TaskRetriedForever
    ] = dataclasses.field(init=False, default_factory=dict)

    @property
    def tasks(self) -> list[task.TaskRetriedForever]:
        return [
            *list(self._dedicated_worker_tasks.values()),
            self.main_task,
        ]

    async def dedicated_stream_worker_task(
        self, owner_id: github_types.GitHubAccountIdType
    ) -> None:
        stream_processor = stream.Processor(
            self.redis_links,
            worker_id=f"dedicated-{owner_id}",
            dedicated_owner_id=owner_id,
            owners_cache=self._owners_cache,
            retry_unhandled_exception_forever=self.retry_handled_exception_forever,
        )
        return await self._stream_worker_task(stream_processor)

    def should_handle_owner(
        self,
        stream_processor: stream.Processor,
        owner_id: github_types.GitHubAccountIdType,
    ) -> bool:
        return owner_id == stream_processor.dedicated_owner_id

    async def work(self) -> None:
        expected_workers = self.get_my_dedicated_worker_ids_from_cache()
        current_workers = set(self._dedicated_worker_tasks.keys())

        to_stop = current_workers - expected_workers
        to_start = expected_workers - current_workers

        if to_stop:
            LOG.info(
                "dedicated workers to stop",
                workers=to_stop,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

        tasks = [self._dedicated_worker_tasks[owner_id] for owner_id in to_stop]
        if tasks:
            await task.stop_and_wait(tasks)

        for owner_id in to_stop:
            del self._dedicated_worker_tasks[owner_id]

        if to_start:
            LOG.info(
                "dedicated workers to start",
                workers=to_start,
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )
        for owner_id in to_start:
            self._dedicated_worker_tasks[owner_id] = task.TaskRetriedForever(
                f"dedicated-{owner_id}",
                functools.partial(self.dedicated_stream_worker_task, owner_id),
                self.worker_idle_time,
            )

        if to_start or to_stop:
            LOG.info(
                "new dedicated workers setup",
                workers=set(self._dedicated_worker_tasks),
                process_index=self.process_index,
                dedicated_stream_processes=self.dedicated_stream_processes,
            )

    def get_my_dedicated_worker_ids_from_cache(
        self,
    ) -> set[github_types.GitHubAccountIdType]:
        if self.dedicated_stream_processes:
            return set(
                sorted(self.service_dedicated_workers_cache_syncer.owner_ids)[
                    self.process_index :: self.dedicated_stream_processes
                ]
            )
        return set()
