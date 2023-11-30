import dataclasses
import typing

from mergify_engine import events as evt_utils
from mergify_engine.worker import task


@dataclasses.dataclass
class PostgresCleanerService(task.SimpleService):
    sleep_before_task: typing.ClassVar[bool] = True

    async def work(self) -> None:
        await evt_utils.delete_outdated_events()
