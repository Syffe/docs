import asyncio
from unittest import mock

import pytest

from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine import redis_utils
from mergify_engine.worker import gitter_service
from mergify_engine.worker import task


OWNER = github_types.GitHubLogin("owner")


async def test_gitter_service_lifecycle(
    request: pytest.FixtureRequest,
    redis_links: redis_utils.RedisLinks,
) -> None:
    logs.setup_logging()
    service = gitter_service.GitterService(
        redis_links,
        gitter_concurrent_jobs=1,
        idle_time=0,
        gitter_worker_idle_time=0.01,
    )
    assert gitter_service.GitterService._instance is not None
    event_loop = asyncio.get_event_loop()
    request.addfinalizer(
        lambda: event_loop.run_until_complete(task.stop_and_wait(service.tasks)),
    )
    method = mock.AsyncMock(return_value="result")
    callback = mock.AsyncMock()

    job = gitter_service.GitterJob[str](OWNER, mock.Mock(), method, callback)

    assert gitter_service.get_job(job.id) is None
    gitter_service.send_job(job)
    assert gitter_service.get_job(job.id) is job

    while job.task is None or not job.task.done():
        await asyncio.sleep(0.001)

    method.assert_awaited()
    callback.assert_awaited()
    assert gitter_service.get_job(job.id) is job

    assert job.result() == "result"
    assert gitter_service.get_job(job.id) is None
    assert gitter_service.GitterService._instance._jobs == {}


async def test_gitter_service_exception(
    request: pytest.FixtureRequest,
    redis_links: redis_utils.RedisLinks,
) -> None:
    logs.setup_logging()
    service = gitter_service.GitterService(
        redis_links,
        gitter_concurrent_jobs=1,
        idle_time=0,
        gitter_worker_idle_time=0.01,
    )
    assert gitter_service.GitterService._instance is not None
    event_loop = asyncio.get_event_loop()
    request.addfinalizer(
        lambda: event_loop.run_until_complete(task.stop_and_wait(service.tasks)),
    )
    method = mock.AsyncMock(side_effect=Exception("boom"))
    callback = mock.AsyncMock()

    job = gitter_service.GitterJob[str](OWNER, mock.Mock(), method, callback)

    assert gitter_service.get_job(job.id) is None
    gitter_service.send_job(job)
    assert gitter_service.get_job(job.id) is job

    while job.task is None or not job.task.done():
        await asyncio.sleep(0.001)

    method.assert_awaited()
    callback.assert_awaited()
    assert gitter_service.get_job(job.id) is job

    with pytest.raises(Exception, match="boom"):
        job.result()

    assert gitter_service.get_job(job.id) is None
    assert gitter_service.GitterService._instance._jobs == {}


async def test_gitter_service_concurrency(
    request: pytest.FixtureRequest,
    redis_links: redis_utils.RedisLinks,
) -> None:
    logs.setup_logging()
    service = gitter_service.GitterService(
        redis_links,
        gitter_concurrent_jobs=4,
        gitter_worker_idle_time=0.01,
        idle_time=0,
    )
    event_loop = asyncio.get_event_loop()
    request.addfinalizer(
        lambda: event_loop.run_until_complete(task.stop_and_wait(service.tasks)),
    )
    waiter_started = asyncio.Event()
    job_1_start = asyncio.Event()
    job_2_start = asyncio.Event()
    q = asyncio.Queue[str]()
    q.put_nowait("foo")
    q.put_nowait("foo")
    q.put_nowait("bar")

    async def waiter() -> str:
        waiter_started.set()
        await q.join()
        return "finished"

    async def join() -> str:
        # Process the queue only if job one and two have started
        await job_1_start.wait()
        await job_2_start.wait()
        res = await q.get()
        q.task_done()
        return res

    async def one() -> str:
        # Start only if waiter as started
        await waiter_started.wait()
        job_1_start.set()
        res = await q.get()
        q.task_done()
        return res

    async def two() -> str:
        # Start only if waiter as started
        await waiter_started.wait()
        job_2_start.set()
        res = await q.get()
        q.task_done()
        return res

    job_1 = gitter_service.GitterJob[str](OWNER, mock.Mock(), one)
    job_2 = gitter_service.GitterJob[str](OWNER, mock.Mock(), two)
    job_join = gitter_service.GitterJob[str](OWNER, mock.Mock(), join)
    job_waiter = gitter_service.GitterJob[str](OWNER, mock.Mock(), waiter)
    gitter_service.send_job(job_1)
    gitter_service.send_job(job_2)
    gitter_service.send_job(job_join)
    gitter_service.send_job(job_waiter)

    assert gitter_service.GitterService._instance is not None
    for job in gitter_service.GitterService._instance._jobs.values():
        while job.task is None or not job.task.done():
            await asyncio.sleep(0.001)

    assert waiter_started.is_set()
    assert q.empty()
    assert job_1.result() == "foo"
    assert job_2.result() == "foo"
    assert job_join.result() == "bar"
    assert job_waiter.result() == "finished"
    assert gitter_service.GitterService._instance._jobs == {}
