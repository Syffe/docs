import fastapi
from fastapi.staticfiles import StaticFiles
import pkg_resources


pkg_resources.resource_filename(__name__, "build")

application = fastapi.FastAPI()
application.mount(
    "/",
    StaticFiles(
        directory=pkg_resources.resource_filename(__name__, "build"), html=True
    ),
    name="installer",
)
