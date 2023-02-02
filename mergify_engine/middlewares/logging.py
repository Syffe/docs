import daiquiri
import starlette
from starlette.middleware import base
import starlette.requests
import starlette.responses


LOG = daiquiri.getLogger(__name__)


class LoggingMiddleware(base.BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: starlette.requests.Request,
        call_next: base.RequestResponseEndpoint,
    ) -> starlette.responses.Response:
        try:
            response = await call_next(request)
        except Exception:
            LOG.error(
                "request",
                request={
                    "method": request.method,
                    "url": request.url,
                    "headers": request.headers,
                },
                exc_info=True,
            )
            raise
        else:
            LOG.info(
                "request",
                request={
                    "method": request.method,
                    "url": request.url,
                    "headers": request.headers,
                },
                response={
                    "status_code": response.status_code,
                    "headers": response.headers,
                },
            )
        return response
