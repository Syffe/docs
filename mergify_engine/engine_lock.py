import contextlib
import dataclasses
import hashlib
import types
import typing

import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database


@dataclasses.dataclass
class EngineLockAcquireError(Exception):
    name: str

    def __str__(self) -> str:
        return f'lock "{self.name}" already acquired'


@dataclasses.dataclass
class EngineLockReleaseError(Exception):
    name: str

    def __str__(self) -> str:
        return f'lock "{self.name}" already released'


@dataclasses.dataclass
class EngineLock:
    session: sqlalchemy.ext.asyncio.AsyncSession
    name: str

    # NOTE(charly): PG Advisory Locks can be identified by a single 64-bit (8 bytes) key value
    # https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
    ADVISORY_LOCK_MAX_KEY_SIZE_IN_BYTES: typing.ClassVar[int] = 8

    @property
    def key(self) -> int:
        # NOTE(charly): hash the lock's name to a signed 64-bit integer
        # (PostgreSQL's bigint) using the BLAKE2 algorithm
        # https://docs.python.org/3/library/hashlib.html#blake2
        digest = hashlib.blake2b(
            self.name.encode(),
            digest_size=self.ADVISORY_LOCK_MAX_KEY_SIZE_IN_BYTES,
        ).digest()

        return int.from_bytes(digest, signed=True)

    async def acquire(self) -> None:
        result = await self.session.execute(
            sqlalchemy.func.pg_try_advisory_lock(self.key),
        )
        lock_acquired = result.one()[0]
        if not lock_acquired:
            raise EngineLockAcquireError(name=self.name)

    async def release(self) -> None:
        if not await self.try_release():
            raise EngineLockReleaseError(name=self.name)

    async def try_release(self) -> bool:
        result = await self.session.execute(
            sqlalchemy.func.pg_advisory_unlock(self.key),
        )
        return bool(result.one()[0])

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        await self.release()


@contextlib.asynccontextmanager
async def get_lock(name: str) -> typing.AsyncGenerator[None, None]:
    """Create an Engine lock while handling the database session lifecycle"""
    async with database.create_session() as session:
        async with EngineLock(session, name):
            yield
