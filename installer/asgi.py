import fastapi
from fastapi.staticfiles import StaticFiles


application = fastapi.FastAPI()
application.mount(
    "/",
    StaticFiles(directory="build", html=True),
    name="installer",
)
