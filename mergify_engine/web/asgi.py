from mergify_engine import service
from mergify_engine.web import root


__all__ = ["application"]

service.setup("web")


application = root.create_app()
