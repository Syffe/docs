import daiquiri
import starlette
from starlette.middleware import base


LOG = daiquiri.getLogger(__name__)

# NOTE(sileht): related to issue introduced by 0.15.0 in june 2021
# not a big deal, but this is polluting logs and sentry
# https://github.com/encode/starlette/discussions/1527
# https://github.com/encode/starlette/pull/1706


class StarletteWorkaroundMiddleware(base.BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: starlette.requests.Request,
        call_next: base.RequestResponseEndpoint,
    ) -> starlette.responses.Response:
        return await call_next(request)

    async def __call__(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ) -> None:
        try:
            await super().__call__(scope, receive, send)
        except RuntimeError:
            if scope["type"] == "http":
                request = starlette.requests.Request(scope, receive, send)
                if not await request.is_disconnected():
                    raise
                LOG.debug("remote disconnected")
            else:
                raise
