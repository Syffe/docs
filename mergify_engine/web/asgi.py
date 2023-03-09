from mergify_engine import service
from mergify_engine.web import root


__all__ = ["application"]

service.setup("web", pg_pool_size=55)


application = root.create_app()
