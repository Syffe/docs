import pytest

from mergify_engine import database
from mergify_engine import engine_lock


async def test_acquire_same_lock_raises_an_error(_setup_database: None) -> None:
    async with database.create_session() as session1, database.create_session() as session2:
        lock1 = engine_lock.EngineLock(session1, "some_lock")
        await lock1.acquire()

        lock2 = engine_lock.EngineLock(session2, "some_lock")
        with pytest.raises(
            engine_lock.EngineLockAcquireError,
            match='lock "some_lock" already acquired',
        ):
            await lock2.acquire()


async def test_acquire_two_locks(_setup_database: None) -> None:
    async with database.create_session() as session1, database.create_session() as session2:
        lock1 = engine_lock.EngineLock(session1, "some_lock")
        await lock1.acquire()

        lock2 = engine_lock.EngineLock(session2, "another_lock")
        await lock2.acquire()


async def test_release(_setup_database: None) -> None:
    async with database.create_session() as session:
        lock = engine_lock.EngineLock(session, "some_lock")
        await lock.acquire()
        await lock.release()


async def test_release_unknown_lock_raises_an_error(_setup_database: None) -> None:
    async with database.create_session() as session:
        lock = engine_lock.EngineLock(session, "some_lock")

        with pytest.raises(
            engine_lock.EngineLockReleaseError,
            match='lock "some_lock" already released',
        ):
            await lock.release()


async def test_release_twice_raises_an_error(_setup_database: None) -> None:
    async with database.create_session() as session:
        lock = engine_lock.EngineLock(session, "some_lock")
        await lock.acquire()
        await lock.release()

        with pytest.raises(engine_lock.EngineLockReleaseError):
            await lock.release()


async def test_context_manager(_setup_database: None) -> None:
    async with database.create_session() as session1, database.create_session() as session2:
        conflicting_lock = engine_lock.EngineLock(session2, "some_lock")

        async with engine_lock.EngineLock(session1, "some_lock"):
            with pytest.raises(
                engine_lock.EngineLockAcquireError,
                match='lock "some_lock" already acquired',
            ):
                await conflicting_lock.acquire()

        # Lock should be released
        await conflicting_lock.acquire()


async def test_get_lock_helper(_setup_database: None) -> None:
    async with engine_lock.get_lock("some_lock"):
        with pytest.raises(
            engine_lock.EngineLockAcquireError,
            match='lock "some_lock" already acquired',
        ):
            async with engine_lock.get_lock("some_lock"):
                pass

    # Lock should be released
    async with engine_lock.get_lock("some_lock"):
        pass
