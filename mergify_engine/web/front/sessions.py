import typing

import imia
import starlette
from starsessions.stores.base import SessionStore

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models import github_user
from mergify_engine.web import redis


class RedisStore(SessionStore):
    def get_redis_key(
        self, session_id: str, purpose: typing.Literal["data", "invalidation"]
    ) -> str:
        return f"fastapi-session/{session_id}/{purpose}"

    async def read(self, session_id: str, lifetime: int) -> bytes:
        redis_links = redis.get_redis_links()
        pipe = await redis_links.authentication.pipeline()
        await pipe.get(self.get_redis_key(session_id, purpose="data"))
        await pipe.exists(self.get_redis_key(session_id, purpose="invalidation"))
        value, invalid = typing.cast(tuple[bytes | None, bool], await pipe.execute())
        if value is None or invalid:
            return b""
        return value

    async def write(self, session_id: str, data: bytes, lifetime: int, ttl: int) -> str:
        # NOTE(sileht): ttl is when the session expire and lifetime is how long
        # the session is. Our Redis backend doesn't need it as lifetime is
        # always greater that ttl. 0 as a special meaning and we can support it with Redis.
        if lifetime == 0:
            raise RuntimeError("lifetime must be greater than zero")

        ttl = max(1, ttl)
        redis_links = redis.get_redis_links()
        await redis_links.authentication.set(
            self.get_redis_key(session_id, "data"), data, ex=ttl
        )
        return session_id

    async def remove(self, session_id: str) -> None:
        # NOTE(sileht): as concurrent request can be done with the same cookie,
        # if one clear the session, maybe other add stuff to the session
        # so we can't remove the redis key, instead we add a new key to mark this session as invalid
        # so future read will know that even if we have session data attached to this id, we must not use it
        # we also ensure this key always expire after the "data" one
        redis_links = redis.get_redis_links()
        await redis_links.authentication.set(
            self.get_redis_key(session_id, "invalidation"),
            b"",
            ex=3600 * settings.DASHBOARD_UI_SESSION_EXPIRATION_HOURS * 2,
        )

    async def exists(self, session_id: str) -> bool:
        redis_links = redis.get_redis_links()
        pipe = await redis_links.authentication.pipeline()
        await pipe.exists(self.get_redis_key(session_id, "data"))
        await pipe.exists(self.get_redis_key(session_id, "invalidation"))
        has_data, has_invalidation = await pipe.execute()
        return has_data and not has_invalidation


class ImiaUserProvider(imia.user_providers.UserProvider):  # type: ignore[misc]
    async def find_by_id(
        self, connection: starlette.requests.HTTPConnection, identifier: typing.Any
    ) -> imia.protocols.UserLike | None:
        async with database.create_session() as session:
            return await github_user.GitHubUser.get_by_id(
                session, github_types.GitHubAccountIdType(int(identifier))
            )

    async def find_by_username(
        self, connection: starlette.requests.HTTPConnection, username_or_email: str
    ) -> imia.protocols.UserLike | None:
        raise NotImplementedError()

    async def find_by_token(
        self, connection: starlette.requests.HTTPConnection, token: str
    ) -> imia.protocols.UserLike | None:
        raise NotImplementedError()
