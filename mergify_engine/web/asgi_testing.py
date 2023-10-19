import contextlib
import typing

import daiquiri
import fastapi

from mergify_engine import service
from mergify_engine.models import manage
from mergify_engine.web import root


__all__ = ["application"]

service.setup("web")

LOG = daiquiri.getLogger(__name__)
LOG.warning("create database tables")


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI) -> typing.AsyncGenerator[None, None]:
    async with root.lifespan(app):
        await manage.create_all()
        yield


application = root.create_app(https_only=False)
application.router.lifespan_context = lifespan
