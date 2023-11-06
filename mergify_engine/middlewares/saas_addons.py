import dataclasses

import daiquiri
from starlette import requests
from starlette import types
import starlette.exceptions

from mergify_engine import settings


"""
This middleware is used to check the source of the incoming requests.

For unexpected connection we will raise 542 error code to easily track issue related to this in Datadog

Hackers may connect directly to the Heroku servers IPs and set the Host header manually.

This middleware must be used after the TrustedHostMiddleware to validate the Host header first.

Heroku handles these domains:
* api.mergify.com
* engine-api-for-dashboard.mergify.com
* engine-api-for-shadow-office.mergify.com
* github-webhook.mergify.com

HTTP_CF_MERGIFY_HOSTS proxied by Cloudflare and must contains a secret header:
* api.mergify.com
* engine-api-for-dashboard.mergify.com
* engine-api-for-shadow-office.mergify.com

(to continue to receive events in case of Cloudflare outage)
HTTP_GITHUB_TO_MERGIFY_HOST GitHub directly talks to Heroku via:
* github-webhook.mergify.com

"""

LOG = daiquiri.getLogger(__name__)


class UnexpectedConnection(Exception):
    pass


@dataclasses.dataclass
class SaasSecurityMiddleware:
    app: types.ASGIApp

    async def __call__(
        self, scope: types.Scope, receive: types.Receive, send: types.Send
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = requests.Request(scope=scope)

        try:
            self._check_source(request)
        except UnexpectedConnection:
            if settings.HTTP_SAAS_SECURITY_ENFORCE:
                response = starlette.responses.Response(
                    status_code=542, content="Unexpected downstream servers"
                )
                await response(scope, receive, send)
                return
            LOG.warning("Unexpected downstream servers", request=request)

        await self.app(scope, receive, send)

    def _check_source(self, request: requests.Request) -> None:
        host = request.headers.get("host", "").split(":")[0]
        if host in settings.HTTP_CF_TO_MERGIFY_HOSTS:
            secret = request.headers.get("X-Mergify-CF-Secret")
            if secret == settings.HTTP_CF_TO_MERGIFY_SECRET:
                return

        elif host == settings.HTTP_GITHUB_TO_MERGIFY_HOST:
            # The token will be checked by the endpoint itself
            if request.headers.get("X-Hub-Signature", "").startswith("sha1="):
                return

        raise UnexpectedConnection()
