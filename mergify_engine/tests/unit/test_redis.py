from unittest import mock

import anys
import pytest

from mergify_engine import redis_utils


async def test_redis_connection_error_working_behavior(
    redis_links: redis_utils.RedisLinks,
) -> None:
    conn = await redis_links.cache.connection_pool.get_connection("whatever")
    assert (
        f"Error connecting to {conn.host}:{conn.port}. foobar."
        == conn._error_message(ConnectionResetError("foobar"))
    )


async def test_redis_connection_error_workaround_for_broken_behavior(
    redis_links: redis_utils.RedisLinks,
) -> None:
    conn = await redis_links.cache.connection_pool.get_connection("whatever")
    assert (
        f"Error connecting to {conn.host}:{conn.port}. Connection reset by peer"
        == conn._error_message(ConnectionResetError())
    )


async def test_redis_stream_processor_ok(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await redis_links.stream.xadd("test", {"foo": "bar1"})
    await redis_links.stream.xadd("test", {"foo": "bar2"})
    await redis_links.stream.xadd("test", {"foo": "bar3"})
    await redis_links.stream.xadd("test", {"foo": "bar4"})

    processor = mock.AsyncMock()
    await redis_utils.process_stream(
        "awesome-task",
        redis_links.stream,
        "test",
        batch_size=3,
        event_processor=processor,
    )

    assert await redis_links.stream.xlen("test") == 1
    assert processor.call_count == 3
    assert processor.mock_calls == [
        mock.call(anys.ANY_BYTES, {b"foo": b"bar1"}),
        mock.call(anys.ANY_BYTES, {b"foo": b"bar2"}),
        mock.call(anys.ANY_BYTES, {b"foo": b"bar3"}),
    ]


@pytest.mark.ignored_logging_errors("unprocessable awesome-task event")
async def test_redis_stream_processor_error(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await redis_links.stream.xadd("test", {"foo": "bar1"})
    await redis_links.stream.xadd("test", {"foo": "bar2"})
    await redis_links.stream.xadd("test", {"foo": "bar3"})

    processor = mock.AsyncMock(
        side_effect=[mock.Mock(), Exception("boom"), Exception("boom")],
    )
    await redis_utils.process_stream(
        "awesome-task",
        redis_links.stream,
        "test",
        batch_size=3,
        event_processor=processor,
    )

    assert processor.call_count == 3
    assert processor.mock_calls == [
        mock.call(anys.ANY_BYTES, {b"foo": b"bar1"}),
        mock.call(anys.ANY_BYTES, {b"foo": b"bar2"}),
        mock.call(anys.ANY_BYTES, {b"foo": b"bar3"}),
    ]

    # Failed event are still there
    assert await redis_links.stream.xlen("test") == 2
