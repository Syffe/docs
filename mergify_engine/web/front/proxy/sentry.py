import json
from urllib import parse

import fastapi
import httpx


ALLOWED_PROJECT_IDS = (1506419,)  # mergify-react
SENTRY_HOST = "o162266.ingest.sentry.io"

router = fastapi.APIRouter()


@router.post("/sentry")
async def sentry_tunnel(request: fastapi.Request) -> fastapi.Response:
    # Inspired from reference implementation:
    # https://docs.sentry.io/platforms/javascript/troubleshooting/#using-the-tunnel-option
    # https://github.com/getsentry/examples/blob/master/tunneling/python/app.py
    body = await request.body()

    try:
        # NOTE(sileht): we don't use splitlines() to follow Sentry
        # documentation and keep \r in messages
        piece = body.decode().split("\n")[0]
    except UnicodeDecodeError:
        raise fastapi.HTTPException(403)

    try:
        header = json.loads(piece)
    except json.JSONDecodeError:
        raise fastapi.HTTPException(403)

    try:
        dsn = parse.urlparse(header.get("dsn"))
    except AttributeError:
        raise fastapi.HTTPException(403)

    try:
        project_id = int(dsn.path.strip("/"))
    except (ValueError, TypeError):
        raise fastapi.HTTPException(403)

    # required to not become an open sentry tunnel
    if dsn.hostname != SENTRY_HOST:
        raise fastapi.HTTPException(403, detail="Unauthorized sentry host")

    if project_id not in ALLOWED_PROJECT_IDS:
        raise fastapi.HTTPException(403, detail="Unauthorized project id")

    # nosemgrep: python.django.security.injection.tainted-url-host.tainted-url-host
    url = f"https://{SENTRY_HOST}/api/{project_id}/envelope/"

    async with httpx.AsyncClient() as client:
        proxy_request: httpx.Request | None = None
        try:
            resp = await client.request(
                method="POST",
                url=url,
                content=body,
                follow_redirects=True,
            )
        except httpx.HTTPStatusError as e:
            resp = e.response
            proxy_request = e.request
        except httpx.RequestError as e:
            resp = None
            proxy_request = e.request

        if resp is None or resp.status_code >= 500:
            resp = httpx.Response(
                status_code=502,
                content="Bad Gateway",
                request=proxy_request,
                headers=dict[str, str](),
            )

        return fastapi.Response(status_code=resp.status_code, content=resp.content)
