import daiquiri

from mergify_engine.models import manage
from mergify_engine.web.asgi import application


__all__ = ["application"]

LOG = daiquiri.getLogger(__name__)
LOG.warning("create database tables")

application.on_event("startup")(manage.create_all)
