from __future__ import annotations

import dataclasses
import datetime
import typing

import daiquiri
import msgpack

from mergify_engine import context
from mergify_engine import date
from mergify_engine import worker_pusher
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class QueuePause:
    repository: context.Repository = dataclasses.field(
        compare=False,
    )

    # Stored in redis
    reason: str = dataclasses.field(
        default_factory=str, metadata={"description": "The reason of the queue pause"}
    )
    pause_date: datetime.datetime = dataclasses.field(
        default_factory=date.utcnow,
        metadata={"description": "The date and time of the pause"},
    )

    class Serialized(typing.TypedDict):
        reason: str
        pause_date: datetime.datetime

    def serialized(self) -> QueuePause.Serialized:
        return self.Serialized(
            reason=self.reason,
            pause_date=self.pause_date,
        )

    @classmethod
    def deserialize(
        cls,
        repository: context.Repository,
        data: QueuePause.Serialized,
    ) -> QueuePause:
        return cls(
            repository=repository,
            reason=data["reason"],
            pause_date=data["pause_date"],
        )

    @classmethod
    def unpack(
        cls,
        repository: context.Repository,
        queue_pause_raw: typing.Any,
    ) -> QueuePause:
        # NOTE(Syffe): timestamp parameter means that timestamp variables will be converted to
        # datetime (value 3=to_datetime()). Other values can be used: 1=to_float(), 2=to_unix_ns()
        queue_pause = msgpack.unpackb(queue_pause_raw, timestamp=3)
        return cls(
            repository=repository,
            reason=queue_pause["reason"],
            pause_date=queue_pause["pause_date"],
        )

    @classmethod
    async def get(
        cls,
        repository: context.Repository,
    ) -> QueuePause | None:
        queue_pause_raw = await repository.installation.redis.queue.get(
            cls._get_redis_key(repository),
        )

        if queue_pause_raw is None:
            return None

        return cls.unpack(repository=repository, queue_pause_raw=queue_pause_raw)

    @classmethod
    def _get_redis_key(cls, repository: context.Repository) -> str:
        return f"merge-pause~{repository.installation.owner_id}~{repository.repo['id']}"

    async def save(
        self,
        queue_rules: qr_config.QueueRules,
        partition_rules: partr_config.PartitionRules,
    ) -> None:
        await self.repository.installation.redis.queue.set(
            self._get_redis_key(self.repository),
            msgpack.packb(
                {
                    "reason": self.reason,
                    "pause_date": self.pause_date,
                },
                # NOTE(Syffe): datetime parameter means that datetime variables will be converted to a timestamp
                # in order to be serialized
                datetime=True,
            ),
        )

        await self._refresh_pulls(
            queue_rules, partition_rules, source="internal/queue_pause_create"
        )

    async def delete(
        self,
        queue_rules: qr_config.QueueRules,
        partition_rules: partr_config.PartitionRules,
    ) -> bool:
        result = bool(
            await self.repository.installation.redis.queue.delete(
                self._get_redis_key(self.repository),
            )
        )

        await self._refresh_pulls(
            queue_rules, partition_rules, source="internal/queue_pause_delete"
        )

        return result

    async def _refresh_pulls(
        self,
        queue_rules: qr_config.QueueRules,
        partition_rules: partr_config.PartitionRules,
        source: str,
    ) -> None:
        async for convoy in merge_train.Convoy.iter_convoys(
            self.repository, queue_rules, partition_rules
        ):
            for train in convoy.iter_trains():
                await train.refresh_pulls(
                    source=source,
                    priority_first_pull_request=worker_pusher.Priority.immediate,
                )

    def get_pause_message(self) -> str:
        return (
            f"⏸️ Checks are suspended, the merge queue is currently paused on this repository, "
            f"for the following reason: `{self.reason}` ⏸️"
        )
