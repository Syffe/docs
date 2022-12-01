import hmac

import daiquiri
import fastapi
from starlette import requests

from mergify_engine import config
from mergify_engine import utils


LOG = daiquiri.getLogger(__name__)


async def github_webhook_signature(request: requests.Request) -> None:
    # Only SHA1 is supported
    header_signature = request.headers.get("X-Hub-Signature")
    if header_signature is None:
        LOG.warning("Webhook without signature")
        raise fastapi.HTTPException(status_code=403)

    try:
        sha_name, signature = header_signature.split("=")
    except ValueError:
        sha_name = None

    if sha_name != "sha1":
        LOG.warning("Webhook signature malformed")
        raise fastapi.HTTPException(status_code=403)

    body = await request.body()

    current_hmac = utils.compute_hmac(body, config.WEBHOOK_SECRET)
    if hmac.compare_digest(current_hmac, str(signature)):
        return

    if config.WEBHOOK_SECRET_PRE_ROTATION is not None:
        future_hmac = utils.compute_hmac(body, config.WEBHOOK_SECRET_PRE_ROTATION)
        if hmac.compare_digest(future_hmac, str(signature)):
            return

    LOG.warning("Webhook signature invalid")
    raise fastapi.HTTPException(status_code=403)


async def dashboard(request: requests.Request) -> None:
    authorization = request.headers.get("Authorization")
    if authorization:
        if authorization.lower().startswith("bearer "):
            token = authorization[7:]
            if token == config.DASHBOARD_TO_ENGINE_API_KEY:
                return

            if (
                config.DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION is not None
                and token == config.DASHBOARD_TO_ENGINE_API_KEY_PRE_ROTATION
            ):
                return

    raise fastapi.HTTPException(status_code=403)
