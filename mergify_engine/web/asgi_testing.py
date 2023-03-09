import daiquiri

from mergify_engine import service
from mergify_engine.models import manage
from mergify_engine.web import root


__all__ = ["application"]

service.setup("web", pg_pool_size=55)

LOG = daiquiri.getLogger(__name__)
LOG.warning("create database tables")


application = root.create_app(https_only=False)
application.on_event("startup")(manage.create_all)
