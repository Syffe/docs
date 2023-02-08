import dataclasses

import starlette
from starlette import types
from starlette.middleware import base


@dataclasses.dataclass
class SudoMiddleware(base.BaseHTTPMiddleware):
    def __init__(self, app: types.ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(
        self,
        request: starlette.requests.Request,
        call_next: base.RequestResponseEndpoint,
    ) -> starlette.responses.Response:
        response = await call_next(request)
        if "sudoGrantedTo" in request.session:
            response.headers["Mergify-Sudo-Granted-To"] = request.session[
                "sudoGrantedTo"
            ]
        return response
