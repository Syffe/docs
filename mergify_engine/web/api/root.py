import argparse
import json
import os

import fastapi
from starlette.middleware import cors

from mergify_engine import config
from mergify_engine.web import api
from mergify_engine.web import utils as web_utils
from mergify_engine.web.api import applications
from mergify_engine.web.api import badges
from mergify_engine.web.api import eventlogs
from mergify_engine.web.api import queues
from mergify_engine.web.api import simulator
from mergify_engine.web.api import statistics


def api_enabled() -> None:
    if not config.API_ENABLE:
        raise fastapi.HTTPException(status_code=404)


app = fastapi.FastAPI(
    title="Mergify API",
    description="Faster & safer code merge",
    version="v1",
    terms_of_service="https://mergify.com/tos",
    contact={
        "name": "Mergify",
        "url": "https://mergify.com",
        "email": "support@mergify.com",
    },
    openapi_url=None,
    redoc_url=None,
    docs_url=None,
    openapi_tags=[
        {
            "name": "applications",
            "description": "Operations with applications.",
        },
        {
            "name": "badges",
            "description": "Operations with badges.",
        },
        {
            "name": "eventlogs",
            "description": "Operations with event logs.",
        },
        {
            "name": "queues",
            "description": "Operations with queues.",
        },
        {
            "name": "simulator",
            "description": "Mergify configuration simulator.",
        },
        {
            "name": "statistics",
            "description": "Operations with statistics.",
        },
    ],
    servers=[{"url": "https://api.mergify.com/v1", "description": "default"}],
    dependencies=[fastapi.Depends(api_enabled)],
    reponses=api.default_responses,
)
app.add_middleware(
    cors.CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(applications.router)
app.include_router(queues.router)
app.include_router(badges.router)
app.include_router(simulator.router)
app.include_router(eventlogs.router)
app.include_router(statistics.router)

web_utils.setup_exception_handlers(app)


def generate_openapi_spec() -> None:
    parser = argparse.ArgumentParser(description="Generate OpenAPI spec file")
    parser.add_argument("output", help="output file")
    args = parser.parse_args()

    path = os.path.dirname(args.output)
    if path:
        os.makedirs(path, exist_ok=True)

    with open(args.output, "w") as f:
        json.dump(fp=f, obj=app.openapi())
