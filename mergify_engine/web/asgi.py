from mergify_engine import service
from mergify_engine.web.root import app as application  # noqa


service.setup("web")
