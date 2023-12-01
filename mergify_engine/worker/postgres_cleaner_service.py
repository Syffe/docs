import dataclasses
import typing

from mergify_engine import eventlogs
from mergify_engine.worker import task


@dataclasses.dataclass
class PostgresCleanerService(task.SimpleService):
    sleep_before_task: typing.ClassVar[bool] = True

    async def work(self) -> None:
        await eventlogs.delete_outdated_events()
