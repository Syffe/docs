import asyncio
from collections import abc
import dataclasses
import typing

import fastapi
import pydantic
from redis import exceptions as redis_exceptions
import sqlalchemy

from mergify_engine import database
from mergify_engine import settings
from mergify_engine.web import redis


class HealthCheckAuth(fastapi.security.http.HTTPBearer):
    async def __call__(
        self,
        request: fastapi.requests.Request,
    ) -> fastapi.security.http.HTTPAuthorizationCredentials | None:
        if settings.HEALTHCHECK_SHARED_TOKEN is None:
            raise fastapi.HTTPException(status_code=404)

        http_auth = await super().__call__(request)
        if (
            http_auth is None
            or http_auth.credentials
            != settings.HEALTHCHECK_SHARED_TOKEN.get_secret_value()
        ):
            raise fastapi.HTTPException(status_code=403)
        return http_auth


check_healthcheck_auth = HealthCheckAuth()

router = fastapi.APIRouter(
    tags=["monitoring"],
    dependencies=[fastapi.Security(check_healthcheck_auth)],
)


@pydantic.dataclasses.dataclass
class ServiceStatus:
    status: typing.Literal["ok", "failure"] = "failure"


def default_service_status() -> ServiceStatus:
    return ServiceStatus(status="failure")


@pydantic.dataclasses.dataclass
class HealthCheckResult:
    redis_queue: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_stream: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_team_members_cache: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_team_permissions_cache: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_user_permissions_cache: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_stats: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_active_users: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_authentication: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    redis_cache: ServiceStatus = dataclasses.field(
        default_factory=default_service_status,
    )
    postgres: ServiceStatus = dataclasses.field(default_factory=default_service_status)


@router.get("/healthcheck")
async def get_healthcheck(
    redis_links: redis.RedisLinks,
    response: fastapi.responses.Response,
    session: sqlalchemy.ext.asyncio.AsyncSession = fastapi.Depends(  # noqa: B008
        database.get_session,
    ),
) -> HealthCheckResult:
    result = HealthCheckResult()

    healthchecks: list[asyncio.Task[None]] = []

    def healthcheck(
        name: str,
        coro: abc.Coroutine[typing.Any, typing.Any, typing.Any],
        exceptions: tuple[type[Exception], ...],
    ) -> None:
        def check_result(future: asyncio.Future[None]) -> None:
            # FIXME(sileht): mypy didn't get it, and return Any, so...
            service: ServiceStatus = getattr(result, name)
            try:
                future.result()
            except exceptions:
                service.status = "failure"
            else:
                service.status = "ok"

        task = asyncio.create_task(coro)
        task.add_done_callback(check_result)
        healthchecks.append(task)

    redis_excs = (
        redis_exceptions.ConnectionError,
        redis_exceptions.TimeoutError,
    )
    postgres_exceptions = (sqlalchemy.exc.DisconnectionError,)

    healthcheck("redis_cache", redis_links.cache.ping(), redis_excs)
    healthcheck("redis_queue", redis_links.queue.ping(), redis_excs)
    healthcheck("redis_stream", redis_links.stream.ping(), redis_excs)
    healthcheck(
        "redis_team_members_cache",
        redis_links.team_members_cache.ping(),
        redis_excs,
    )
    healthcheck(
        "redis_team_permissions_cache",
        redis_links.team_permissions_cache.ping(),
        redis_excs,
    )
    healthcheck(
        "redis_user_permissions_cache",
        redis_links.user_permissions_cache.ping(),
        redis_excs,
    )
    healthcheck("redis_stats", redis_links.stats.ping(), redis_excs)
    healthcheck("redis_active_users", redis_links.active_users.ping(), redis_excs)
    healthcheck("redis_authentication", redis_links.authentication.ping(), redis_excs)
    healthcheck(
        "postgres",
        session.execute(sqlalchemy.select(1)),
        postgres_exceptions,
    )

    await asyncio.wait(healthchecks)

    for field in dataclasses.fields(result):
        # FIXME(sileht): mypy didn't get it, and return Any, so...
        service: ServiceStatus = getattr(result, field.name)
        if service.status == "failure":
            response.status_code = 502
            break
    return result
