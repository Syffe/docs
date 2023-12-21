from __future__ import annotations

import dataclasses
import functools
import hashlib
import logging
import typing
import uuid

import daiquiri
import ddtrace
import redis.asyncio as redispy
from redis.asyncio import retry

from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import service
from mergify_engine import settings


if typing.TYPE_CHECKING:
    from collections import abc
    import datetime
    import types

    from mergify_engine.config import types as config_types


LOG = daiquiri.getLogger(__name__)


RedisCache = typing.NewType("RedisCache", "redispy.Redis[bytes]")
RedisAuthentication = typing.NewType("RedisAuthentication", "redispy.Redis[bytes]")
RedisStream = typing.NewType("RedisStream", "redispy.Redis[bytes]")
PipelineStream = typing.NewType("PipelineStream", "redispy.client.Pipeline[bytes]")
RedisQueue = typing.NewType("RedisQueue", "redispy.Redis[bytes]")
RedisActiveUsers = typing.NewType("RedisActiveUsers", "redispy.Redis[bytes]")
RedisUserPermissionsCache = typing.NewType(
    "RedisUserPermissionsCache",
    "redispy.Redis[bytes]",
)
RedisTeamPermissionsCache = typing.NewType(
    "RedisTeamPermissionsCache",
    "redispy.Redis[bytes]",
)
RedisTeamMembersCache = typing.NewType("RedisTeamMembersCache", "redispy.Redis[bytes]")
RedisStats = typing.NewType("RedisStats", "redispy.Redis[bytes]")

ScriptIdT = typing.NewType("ScriptIdT", uuid.UUID)

SCRIPTS: dict[ScriptIdT, tuple[bytes, str]] = {}


# TODO(sileht): Redis script management can be moved back to Redis.register_script() mechanism
def register_script(script: str) -> ScriptIdT:
    global SCRIPTS
    # NOTE(sileht): We don't use sha, in case of something server side change the script sha
    script_id = ScriptIdT(uuid.uuid4())
    SCRIPTS[script_id] = (
        # NOTE(sileht): SHA1 is imposed by Redis itself
        hashlib.sha1(  # nosemgrep contrib.dlint.dlint-equivalent.insecure-hashlib-use, python.lang.security.insecure-hash-algorithms.insecure-hash-algorithm-sha1
            script.encode("utf8"),
        )
        .hexdigest()
        .encode(),
        script,
    )
    return script_id


# FIXME(sileht): We store Cache and Stream script into the same global object
# it works but if a script is loaded into two redis, this won't works as expected
# as the app will think it's already loaded while it's not...
async def load_script(
    connection: redispy.connection.Connection,
    script_id: ScriptIdT,
) -> None:
    global SCRIPTS
    sha, script = SCRIPTS[script_id]
    await connection.send_command("SCRIPT LOAD", script)
    newsha = await connection.read_response()
    if newsha != sha:
        LOG.error(
            "wrong redis script sha cached",
            script_id=script_id,
            sha=sha,
            newsha=newsha,
        )
        SCRIPTS[script_id] = (newsha, script)


async def load_stream_scripts(connection: redispy.connection.Connection) -> None:
    # TODO(sileht): cleanup unused script, this is tricky, because during
    # deployment we have running in parallel due to the rolling upgrade:
    # * an old version of the asgi server
    # * a new version of the asgi server
    # * a new version of the backend
    global SCRIPTS
    scripts = list(SCRIPTS.items())  # order matter for zip bellow
    shas = [sha for _, (sha, _) in scripts]
    ids = [_id for _id, _ in scripts]
    await connection.on_connect()
    await connection.send_command("SCRIPT EXISTS", *shas)

    # exists is a list of 0 and/or 1, notifying the existence of each script
    exists = await connection.read_response()
    for script_id, exist in zip(ids, exists, strict=True):
        if exist == 0:
            await load_script(connection, script_id)


async def run_script(
    redis: RedisCache | RedisStream | PipelineStream,
    script_id: ScriptIdT,
    keys: tuple[str, ...],
    args: tuple[str, ...] | None = None,
) -> typing.Any:
    global SCRIPTS
    sha, script = SCRIPTS[script_id]
    args = keys if args is None else keys + args
    return await redis.evalsha(sha, len(keys), *args)  # type: ignore[no-untyped-call]


@dataclasses.dataclass
class RedisLinks:
    name: str
    connection_pool_cls: type[
        redispy.connection.ConnectionPool
    ] = redispy.connection.ConnectionPool

    # NOTE(sileht): This is used, only to limit connection on webserver side.
    # The worker open only one connection per asyncio tasks per worker.
    cache_max_connections: int | None = None
    stream_max_connections: int | None = None
    queue_max_connections: int | None = None
    stats_max_connections: int | None = None
    authentication_max_connections: int | None = None
    active_users_max_connections: int | None = None

    @functools.cached_property
    def queue(self) -> RedisQueue:
        client = self.redis_from_url(
            "queue",
            settings.QUEUE_URL,
            max_connections=self.queue_max_connections,
        )
        return RedisQueue(client)

    @functools.cached_property
    def stream(self) -> RedisStream:
        client = self.redis_from_url(
            "stream",
            settings.STREAM_URL,
            max_connections=self.stream_max_connections,
            redis_connect_func=load_stream_scripts,
        )
        return RedisStream(client)

    @functools.cached_property
    def team_members_cache(self) -> RedisTeamMembersCache:
        client = self.redis_from_url(
            "team_members_cache",
            settings.TEAM_MEMBERS_CACHE_URL,
            max_connections=self.cache_max_connections,
        )
        return RedisTeamMembersCache(client)

    @functools.cached_property
    def team_permissions_cache(self) -> RedisTeamPermissionsCache:
        client = self.redis_from_url(
            "team_permissions_cache",
            settings.TEAM_PERMISSIONS_CACHE_URL,
            max_connections=self.cache_max_connections,
        )
        return RedisTeamPermissionsCache(client)

    @functools.cached_property
    def user_permissions_cache(self) -> RedisUserPermissionsCache:
        client = self.redis_from_url(
            "user_permissions_cache",
            settings.USER_PERMISSIONS_CACHE_URL,
            max_connections=self.cache_max_connections,
        )
        return RedisUserPermissionsCache(client)

    @functools.cached_property
    def stats(self) -> RedisStats:
        client = self.redis_from_url(
            "stats",
            settings.STATISTICS_URL,
            max_connections=self.stats_max_connections,
        )
        return RedisStats(client)

    @functools.cached_property
    def active_users(self) -> RedisActiveUsers:
        client = self.redis_from_url(
            "active_users",
            settings.ACTIVE_USERS_URL,
            max_connections=self.active_users_max_connections,
        )
        return RedisActiveUsers(client)

    @functools.cached_property
    def authentication(self) -> RedisAuthentication:
        client = self.redis_from_url(
            "authentication",
            settings.AUTHENTICATION_URL,
            max_connections=self.authentication_max_connections,
        )
        return RedisAuthentication(client)

    @functools.cached_property
    def cache(self) -> RedisCache:
        client = self.redis_from_url(
            "cache",
            settings.CACHE_URL,
            max_connections=self.cache_max_connections,
        )
        return RedisCache(client)

    def redis_from_url(
        self,
        name: str,
        url: config_types.RedisDSN,
        max_connections: int | None = None,
        redis_connect_func: redispy.connection.ConnectCallbackT | None = None,
    ) -> redispy.Redis[bytes]:
        options: dict[str, typing.Any] = {}

        if settings.REDIS_SSL_VERIFY_MODE_CERT_NONE and url.scheme == "rediss":
            options["ssl_check_hostname"] = False
            options["ssl_cert_reqs"] = None

        pool = self.connection_pool_cls.from_url(
            url.geturl(),
            max_connections=max_connections,
            decode_responses=False,
            client_name=f"{service.SERVICE_NAME}/{self.name}/{name}",
            redis_connect_func=redis_connect_func,
            health_check_interval=10,
            retry=retry.Retry(redispy.default_backoff(), retries=3),
            retry_on_timeout=True,
            # Heroku H12 timeout is 30s and we retry 3 times
            socket_timeout=5,
            socket_connect_timeout=5,
            socket_keepalive=True,
            **options,
        )

        client = redispy.Redis(connection_pool=pool)
        ddtrace.Pin.override(client, service=f"engine-redis-{name}")
        return client

    async def __aenter__(self) -> RedisLinks:
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_value: Exception | None,
        traceback: types.TracebackType | None,
    ) -> None:
        await self.shutdown_all()

    async def shutdown_all(self) -> None:
        for db in (
            "cache",
            "stream",
            "queue",
            "team_members_cache",
            "team_permissions_cache",
            "user_permissions_cache",
            "active_users",
            "stats",
            "authentication",
        ):
            if db in self.__dict__:
                await self.__dict__[db].close(close_connection_pool=True)

    async def flushall(self) -> None:
        await self.cache.flushdb()
        await self.stream.flushdb()
        await self.queue.flushdb()
        await self.team_members_cache.flushdb()
        await self.team_permissions_cache.flushdb()
        await self.user_permissions_cache.flushdb()
        await self.active_users.flushdb()
        await self.stats.flushdb()
        await self.authentication.flushdb()


def get_expiration_minid(retention: datetime.timedelta) -> int:
    return int((date.utcnow() - retention).timestamp() * 1000)


async def iter_stream(
    stream: RedisStream,
    stream_key: str,
    batch_size: int,
) -> abc.AsyncGenerator[tuple[bytes, dict[bytes, bytes]], None]:
    min_stream_event_id = "-"

    while stream_entry := await stream.xrange(
        stream_key,
        min=min_stream_event_id,
        count=batch_size,
    ):
        for entry_id, entry_data in stream_entry:
            yield entry_id, entry_data

        min_stream_event_id = f"({entry_id.decode()}"


async def iter_stream_reverse(
    stream: RedisStream,
    stream_key: str,
    batch_size: int,
) -> abc.AsyncGenerator[tuple[bytes, dict[bytes, bytes]], None]:
    max_stream_event_id = "+"

    while stream_entry := await stream.xrevrange(
        stream_key,
        max=max_stream_event_id,
        count=batch_size,
    ):
        for entry_id, entry_data in stream_entry:
            yield entry_id, entry_data

        max_stream_event_id = f"({entry_id.decode()}"


async def process_stream(
    event_name: str,
    redis_stream: RedisStream,
    redis_key: str,
    batch_size: int,
    event_processor: abc.Callable[[bytes, dict[bytes, bytes]], abc.Awaitable[None]],
    redis_payload_data_key: bytes = b"data",
) -> bool:
    events_count = 0
    ids_to_delete: set[bytes] = set()
    try:
        async for event_id, event in iter_stream(
            redis_stream,
            redis_key,
            batch_size=batch_size,
        ):
            events_count += 1
            try:
                await event_processor(event_id, event)
            except Exception as e:
                if not exceptions.should_be_ignored(e):
                    log_level = (
                        logging.ERROR
                        if exceptions.need_retry_in(e) is None
                        else logging.INFO
                    )
                    LOG.log(
                        log_level,
                        "unprocessable %s event",
                        event_name,
                        stream_event=event,
                        stream_event_id=event_id,
                        exc_info=True,
                    )
                    continue

            ids_to_delete.add(event_id)

    finally:
        if ids_to_delete:
            await redis_stream.xdel(redis_key, *ids_to_delete)

    return events_count == batch_size
