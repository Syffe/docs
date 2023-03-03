import asyncio

import async_timeout


# NOTE(sileht): workaround async_timeout CancellationError bug
# https://github.com/redis/redis-py/pull/2602
# https://github.com/aio-libs/async-timeout/issues/229
async_timeout.Timeout = asyncio.Timeout  # type: ignore[assignment,misc]
async_timeout.timeout = asyncio.timeout  # type: ignore[assignment]
