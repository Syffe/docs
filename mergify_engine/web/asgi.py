from mergify_engine import service
from mergify_engine.web import root


service.setup("web")


application = root.create_app()
