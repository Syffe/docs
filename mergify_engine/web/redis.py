import typing

import daiquiri
import fastapi
from redis import asyncio as redispy

from mergify_engine import config
from mergify_engine import redis_utils


_REDIS_LINKS: redis_utils.RedisLinks

LOG = daiquiri.getLogger(__name__)


async def startup() -> None:
    global _REDIS_LINKS
    _REDIS_LINKS = redis_utils.RedisLinks(
        name="web",
        # Heroku H12 timeout is 30s, so half should be good
        connection_pool_cls=redispy.connection.BlockingConnectionPool,
        connection_pool_kwargs={"timeout": 15},
        cache_max_connections=config.REDIS_CACHE_WEB_MAX_CONNECTIONS,
        stream_max_connections=config.REDIS_STREAM_WEB_MAX_CONNECTIONS,
        queue_max_connections=config.REDIS_QUEUE_WEB_MAX_CONNECTIONS,
        eventlogs_max_connections=config.REDIS_EVENTLOGS_WEB_MAX_CONNECTIONS,
        stats_max_connections=config.REDIS_STATS_WEB_MAX_CONNECTIONS,
        authentication_max_connections=config.REDIS_AUTHENTICATION_WEB_MAX_CONNECTIONS,
        active_users_max_connections=config.REDIS_ACTIVE_USERS_WEB_MAX_CONNECTIONS,
    )


async def shutdown() -> None:
    LOG.info("asgi: starting redis shutdown")
    await _REDIS_LINKS.shutdown_all()
    LOG.info("asgi: finished redis shutdown")


def get_redis_links() -> redis_utils.RedisLinks:
    global _REDIS_LINKS
    return _REDIS_LINKS


RedisLinks = typing.Annotated[redis_utils.RedisLinks, fastapi.Depends(get_redis_links)]
