import daiquiri
from datadog import statsd
import fastapi
from redis import exceptions as redis_exceptions
from starlette import requests
from starlette import responses

from mergify_engine import exceptions as engine_exceptions
from mergify_engine import pagination


LOG = daiquiri.getLogger(__name__)


def setup_exception_handlers(app: fastapi.FastAPI) -> None:
    @app.exception_handler(redis_exceptions.ConnectionError)
    async def redis_errors(
        request: requests.Request, exc: redis_exceptions.ConnectionError
    ) -> responses.Response:
        statsd.increment("redis.client.connection.errors")
        LOG.warning("FastAPI lost Redis connection", exc_info=exc)
        return responses.Response(status_code=503)

    @app.exception_handler(engine_exceptions.RateLimited)
    async def rate_limited_handler(
        request: requests.Request, exc: engine_exceptions.RateLimited
    ) -> responses.JSONResponse:
        return responses.JSONResponse(
            status_code=403,
            content={"message": "Organization or user has hit GitHub API rate limit"},
        )

    @app.exception_handler(pagination.InvalidCursor)
    async def pagination_handler(
        request: requests.Request, exc: pagination.InvalidCursor
    ) -> responses.JSONResponse:
        return responses.JSONResponse(
            status_code=422,
            content={"message": "Invalid cursor", "cursor": exc.cursor},
        )
