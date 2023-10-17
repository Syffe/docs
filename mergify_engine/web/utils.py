import datetime
import enum
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import fastapi
from redis import exceptions as redis_exceptions
import sqlalchemy
from starlette import requests
from starlette import responses

from mergify_engine import exceptions as engine_exceptions
from mergify_engine import pagination


LOG = daiquiri.getLogger(__name__)


def setup_exception_handlers(app: fastapi.FastAPI) -> None:
    @app.exception_handler(sqlalchemy.exc.DBAPIError)
    async def postgres_errors(
        request: requests.Request,
        exc: sqlalchemy.exc.DBAPIError,
    ) -> responses.Response:
        if exc.connection_invalidated:
            statsd.increment("postgres.client.connection.errors")
            LOG.warning("FastAPI lost Postgres connection", exc_info=exc)
            return responses.Response(status_code=503)
        raise exc

    async def redis_errors(
        request: requests.Request,
        exc: redis_exceptions.ConnectionError | redis_exceptions.TimeoutError,
    ) -> responses.Response:
        statsd.increment("redis.client.connection.errors")
        LOG.warning("FastAPI lost Redis connection", exc_info=exc)
        return responses.Response(status_code=503)

    app.exception_handler(redis_exceptions.ConnectionError)(redis_errors)
    app.exception_handler(redis_exceptions.TimeoutError)(redis_errors)

    @app.exception_handler(engine_exceptions.RateLimited)
    async def rate_limited_handler(
        request: requests.Request, exc: engine_exceptions.RateLimited
    ) -> responses.JSONResponse:
        return responses.JSONResponse(
            status_code=403,
            content={"message": "Organization or user has hit GitHub API rate limit"},
        )

    @app.exception_handler(engine_exceptions.MergifyNotInstalled)
    async def mergify_not_installed_handler(
        request: requests.Request, exc: engine_exceptions.MergifyNotInstalled
    ) -> responses.JSONResponse:
        return responses.JSONResponse(
            status_code=403,
            content={
                "message": "Mergify is not installed or suspended on this organization or repository"
            },
        )

    @app.exception_handler(pagination.InvalidCursor)
    async def pagination_handler(
        request: requests.Request, exc: pagination.InvalidCursor
    ) -> responses.JSONResponse:
        return responses.JSONResponse(
            status_code=422,
            content={"message": "Invalid cursor", "cursor": exc.cursor},
        )


def serialize_query_parameters(val: typing.Any) -> typing.Any:
    if isinstance(val, dict):
        return {
            k: serialize_query_parameters(v) for k, v in val.items() if v is not None
        }
    if isinstance(val, list):
        return [serialize_query_parameters(v) for v in val]
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, datetime.datetime):
        return val.isoformat()
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int | str | float):
        return val
    raise ValueError(f"Unsupported type {type(val)}")
