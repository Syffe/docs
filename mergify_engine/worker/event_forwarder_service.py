import dataclasses

import daiquiri

from mergify_engine import event_forwarder
from mergify_engine.worker import task


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class EventForwarder(task.SimpleService):
    async def work(self) -> None:
        while not self.main_task.shutdown_requested.is_set():
            pending_work = await event_forwarder.forward(self.redis_links)
            if not pending_work:
                # NOTE(sileht): nothing to do, sleep a bit
                return
