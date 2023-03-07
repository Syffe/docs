from __future__ import annotations

import dataclasses
import json
import typing

from mergify_engine import config
from mergify_engine import crypto
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import dashboard
from mergify_engine.clients import http


class ApplicationAccountScope(typing.TypedDict):
    id: github_types.GitHubAccountIdType
    login: github_types.GitHubLogin


class ApplicationDashboardJSON(typing.TypedDict):
    id: int
    name: str
    github_account: github_types.GitHubAccount


class CachedApplication(typing.TypedDict):
    id: int
    name: str
    api_access_key: str
    api_secret_key: str
    account_scope: ApplicationAccountScope


class ApplicationUserNotFound(Exception):
    pass


ApplicationClassT = typing.TypeVar("ApplicationClassT", bound="ApplicationBase")


@dataclasses.dataclass
class ApplicationBase:
    redis: redis_utils.RedisCache
    id: int
    name: str
    api_access_key: str
    api_secret_key: str
    account_scope: ApplicationAccountScope

    @classmethod
    async def delete(cls, redis: redis_utils.RedisCache, api_access_key: str) -> None:
        raise NotImplementedError

    @classmethod
    async def get(
        cls: type[ApplicationClassT],
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationClassT:
        raise NotImplementedError

    @classmethod
    async def update(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
        app: ApplicationDashboardJSON,
    ) -> None:
        raise NotImplementedError


@dataclasses.dataclass
class ApplicationSaas(ApplicationBase):
    ttl: int = -2

    RETENTION_SECONDS = 60 * 60 * 24 * 3  # 3 days
    VALIDITY_SECONDS = 3600

    @staticmethod
    def _cache_key(api_access_key: str) -> str:
        return f"api-key-cache~{api_access_key}"

    def _has_expired(self) -> bool:
        if self.ttl < 0:  # not cached
            return True
        elapsed_since_stored = self.RETENTION_SECONDS - self.ttl
        return elapsed_since_stored > self.VALIDITY_SECONDS

    @classmethod
    async def delete(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
    ) -> None:
        await redis.delete(cls._cache_key(api_access_key))

    @classmethod
    async def get(
        cls: type[ApplicationClassT],
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationClassT:
        return typing.cast(
            ApplicationClassT,
            await typing.cast(ApplicationSaas, cls)._get(
                redis, api_access_key, api_secret_key
            ),
        )

    @classmethod
    async def _get(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> "ApplicationSaas":
        cached_application = await cls._retrieve_from_cache(
            redis, api_access_key, api_secret_key
        )
        if cached_application is None or cached_application._has_expired():
            try:
                db_application = await cls._retrieve_from_db(
                    redis, api_access_key, api_secret_key
                )
            except http.HTTPForbidden:
                # api key is valid, but not the scope
                raise ApplicationUserNotFound()
            except http.HTTPNotFound:
                raise ApplicationUserNotFound()
            except Exception as exc:
                if cached_application is not None and (
                    exceptions.should_be_ignored(exc) or exceptions.need_retry(exc)
                ):
                    # NOTE(sileht): return the cached application, instead of
                    # retrying the stream, just because the dashboard has a
                    # connectivity issue.
                    return cached_application
                raise

            await db_application.save_to_cache()
            return db_application
        return cached_application

    async def save_to_cache(self) -> None:
        """Save an application to the cache."""
        await self.redis.setex(
            self._cache_key(self.api_access_key),
            self.RETENTION_SECONDS,
            crypto.encrypt(
                json.dumps(
                    CachedApplication(
                        {
                            "id": self.id,
                            "name": self.name,
                            "api_access_key": self.api_access_key,
                            "api_secret_key": self.api_secret_key,
                            "account_scope": self.account_scope,
                        }
                    )
                ).encode()
            ),
        )
        self.ttl = self.RETENTION_SECONDS

    @classmethod
    async def update(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
        data: ApplicationDashboardJSON,
    ) -> None:
        encrypted_application = await redis.get(cls._cache_key(api_access_key))
        if encrypted_application is not None:
            decrypted_application = typing.cast(
                CachedApplication,
                json.loads(crypto.decrypt(encrypted_application).decode()),
            )
            if "account_scope" not in decrypted_application:
                # TODO(sileht): Backward compat, delete me <= 7.2.1
                return  # type: ignore[unreachable]

            if data["github_account"] is None:
                # TODO(sileht): Backward compat, delete me <= 7.2.1
                return  # type: ignore[unreachable]

            app = cls(
                redis,
                data["id"],
                data["name"],
                decrypted_application["api_access_key"],
                decrypted_application["api_secret_key"],
                ApplicationAccountScope(
                    {
                        "id": data["github_account"]["id"],
                        "login": data["github_account"]["login"],
                    }
                ),
            )
            await app.save_to_cache()
        return None

    @classmethod
    async def _retrieve_from_cache(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationSaas | None:
        async with await redis.pipeline() as pipe:
            await pipe.get(cls._cache_key(api_access_key))
            await pipe.ttl(cls._cache_key(api_access_key))
            encrypted_application, ttl = typing.cast(
                tuple[bytes, int], await pipe.execute()
            )
        if encrypted_application:
            decrypted_application = typing.cast(
                CachedApplication,
                json.loads(crypto.decrypt(encrypted_application).decode()),
            )
            if decrypted_application["api_secret_key"] != api_secret_key:
                # Don't raise ApplicationUserNotFound yet, check the database first
                return None

            if "account_scope" not in decrypted_application:
                # TODO(sileht): Backward compat, delete me
                return  # type: ignore[unreachable]

            if decrypted_application["account_scope"] is None:
                # TODO(sileht): Backward compat, delete me
                return  # type: ignore[unreachable]

            if "id" not in decrypted_application:
                # TODO(sileht): Backward compat, delete me
                return  # type: ignore[unreachable]

            return cls(
                redis,
                decrypted_application["id"],
                decrypted_application["name"],
                decrypted_application["api_access_key"],
                decrypted_application["api_secret_key"],
                decrypted_application["account_scope"],
                ttl,
            )
        return None

    @classmethod
    async def _retrieve_from_db(
        cls,
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> "ApplicationSaas":
        async with dashboard.AsyncDashboardSaasClient() as client:
            resp = await client.post(
                "/engine/applications",
                json={"token": f"{api_access_key}{api_secret_key}"},
            )
            data = typing.cast(ApplicationDashboardJSON, resp.json())
            return cls(
                redis,
                data["id"],
                data["name"],
                api_access_key,
                api_secret_key,
                ApplicationAccountScope(
                    {
                        "id": data["github_account"]["id"],
                        "login": data["github_account"]["login"],
                    }
                ),
            )


@dataclasses.dataclass
class ApplicationOnPremise(ApplicationBase):
    @classmethod
    async def delete(cls, redis: redis_utils.RedisCache, api_access_key: str) -> None:
        pass

    @classmethod
    async def get(
        cls: type[ApplicationClassT],
        redis: redis_utils.RedisCache,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationClassT:
        data = config.APPLICATION_APIKEYS.get(api_access_key)
        if data is None or data["api_secret_key"] != api_secret_key:
            raise ApplicationUserNotFound()
        return cls(
            redis,
            0,
            "on-premise-app",
            api_access_key,
            api_secret_key,
            ApplicationAccountScope(
                {
                    "id": github_types.GitHubAccountIdType(data["account_id"]),
                    "login": github_types.GitHubLogin(data["account_login"]),
                }
            ),
        )


if config.SAAS_MODE:

    @dataclasses.dataclass
    class Application(ApplicationSaas):
        pass

else:

    @dataclasses.dataclass
    class Application(ApplicationOnPremise):  # type: ignore [no-redef]
        pass
