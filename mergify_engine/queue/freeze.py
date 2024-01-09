from __future__ import annotations

import dataclasses
import datetime  # noqa: TCH003
import typing

import daiquiri
import msgpack

from mergify_engine import date
from mergify_engine import worker_pusher
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class QueueFreezeWithNoQueueRuleError(Exception):
    name: str


@dataclasses.dataclass
class QueueFreeze:
    repository: context.Repository = dataclasses.field(
        compare=False,
    )
    queue_rule: qr_config.QueueRule = dataclasses.field(
        compare=False,
    )

    # Stored in redis
    name: str = dataclasses.field(metadata={"description": "Queue name"})
    reason: str = dataclasses.field(
        default_factory=str,
        metadata={"description": "Freeze reason"},
    )
    freeze_date: datetime.datetime = dataclasses.field(
        default_factory=date.utcnow,
        metadata={"description": "The date and time of the freeze"},
    )
    cascading: bool = dataclasses.field(
        default=True,
        metadata={"description": "The active status of the cascading effect"},
    )

    class Serialized(typing.TypedDict):
        name: str
        reason: str
        freeze_date: datetime.datetime
        cascading: bool

    def serialized(self) -> QueueFreeze.Serialized:
        return self.Serialized(
            name=self.name,
            reason=self.reason,
            freeze_date=self.freeze_date,
            cascading=self.cascading,
        )

    @classmethod
    def deserialize(
        cls,
        repository: context.Repository,
        queue_rule: qr_config.QueueRule,
        data: QueueFreeze.Serialized,
    ) -> QueueFreeze:
        return cls(
            repository=repository,
            queue_rule=queue_rule,
            name=data["name"],
            reason=data["reason"],
            freeze_date=data["freeze_date"],
            cascading=data["cascading"],
        )

    @classmethod
    def unpack(
        cls,
        repository: context.Repository,
        queue_rule: qr_config.QueueRule,
        queue_freeze_raw: typing.Any,
    ) -> QueueFreeze:
        # NOTE(Syffe): timestamp parameter means that timestamp variables will be converted to
        # datetime (value 3=to_datetime()). Other values can be used: 1=to_float(), 2=to_unix_ns()
        queue_freeze = msgpack.unpackb(queue_freeze_raw, timestamp=3)
        return cls(
            repository=repository,
            queue_rule=queue_rule,
            name=queue_freeze["name"],
            reason=queue_freeze["reason"],
            freeze_date=queue_freeze["freeze_date"],
            cascading=queue_freeze.get("cascading", True),  # Backward compat
        )

    @classmethod
    async def get_all(
        cls,
        repository: context.Repository,
    ) -> abc.AsyncGenerator[QueueFreeze, None]:
        async for (
            key,
            queue_freeze_raw,
        ) in repository.installation.redis.queue.hscan_iter(
            name=cls._get_redis_hash(repository),
            match=cls._get_redis_key_match(repository),
        ):
            name = cls._get_name_from_redis_key(key)
            try:
                queue_rule = repository.mergify_config["queue_rules"][name]
            except KeyError:
                # TODO(sileht): cleanup Redis in this case
                continue

            yield cls.unpack(
                repository=repository,
                queue_rule=queue_rule,
                queue_freeze_raw=queue_freeze_raw,
            )

    @classmethod
    async def get_all_non_cascading(
        cls,
        repository: context.Repository,
    ) -> abc.AsyncGenerator[QueueFreeze, None]:
        async for queue_freeze in cls.get_all(repository):
            if not queue_freeze.cascading:
                yield queue_freeze

    @classmethod
    async def get(
        cls,
        repository: context.Repository,
        queue_rule: qr_config.QueueRule,
    ) -> QueueFreeze | None:
        queue_freeze_raw = await repository.installation.redis.queue.hget(
            cls._get_redis_hash(repository),
            cls._get_redis_key(repository, queue_rule.name),
        )

        if queue_freeze_raw is None:
            return None

        return cls.unpack(
            repository=repository,
            queue_rule=queue_rule,
            queue_freeze_raw=queue_freeze_raw,
        )

    @classmethod
    def _get_redis_hash(cls, repository: context.Repository) -> str:
        return f"merge-freeze~{repository.installation.owner_id}"

    @classmethod
    def _get_redis_key(cls, repository: context.Repository, queue_name: str) -> str:
        return f"{repository.repo['id']}~{queue_name}"

    @classmethod
    def _get_name_from_redis_key(cls, key: bytes) -> qr_config.QueueName:
        return qr_config.QueueName(key.split(b"~")[1].decode())

    @classmethod
    def _get_redis_key_match(cls, repository: context.Repository) -> str:
        return f"{repository.repo['id']}~*"

    async def save(self) -> None:
        await self.repository.installation.redis.queue.hset(
            self._get_redis_hash(self.repository),
            self._get_redis_key(self.repository, self.name),
            msgpack.packb(
                {
                    "name": self.name,
                    "reason": self.reason,
                    "freeze_date": self.freeze_date,
                    "cascading": self.cascading,
                },
                # NOTE(Syffe): datetime parameter means that datetime variables will be converted to a timestamp
                # in order to be serialized
                datetime=True,
            ),
        )

        await self._refresh_pulls(source="internal/queue_freeze_create")

    async def delete(self) -> bool:
        result = bool(
            await self.repository.installation.redis.queue.hdel(
                self._get_redis_hash(self.repository),
                self._get_redis_key(self.repository, self.name),
            ),
        )

        await self._refresh_pulls(source="internal/queue_freeze_delete")

        return result

    async def _refresh_pulls(self, source: str) -> None:
        async for convoy in merge_train.Convoy.iter_convoys(self.repository):
            for train in convoy.iter_trains():
                await train.refresh_pulls(
                    source=source,
                    priority_first_pull_request=worker_pusher.Priority.immediate,
                )

    def get_freeze_message(self) -> str:
        return (
            f"❄️ The merge is currently blocked by the freeze of the queue `{self.name}`, "
            f"for the following reason: `{self.reason}` ❄️"
        )
