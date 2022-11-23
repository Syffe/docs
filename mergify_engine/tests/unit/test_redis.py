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
