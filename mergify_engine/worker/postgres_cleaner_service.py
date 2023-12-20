import dataclasses
import typing

import daiquiri

from mergify_engine import eventlogs
from mergify_engine.ci import event_processing
from mergify_engine.models.github import check_run as gh_checkrun_model
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class PostgresCleanerService(task.SimpleService):
    sleep_before_task: typing.ClassVar[bool] = True

    async def work(self) -> None:
        try:
            await event_processing.delete_outdated_workflow_jobs()
        except Exception:
            LOG.error("Failed to delete outdated workflow_jobs", exc_info=True)

        try:
            await eventlogs.delete_outdated_events()
        except Exception:
            LOG.error("Failed to delete outdated events", exc_info=True)

        try:
            await gh_checkrun_model.CheckRun.delete_outdated_check_runs()
        except Exception:
            LOG.error("Failed to delete outdated check runs", exc_info=True)
