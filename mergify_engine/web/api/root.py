import fastapi
from starlette.middleware import cors

from mergify_engine import settings
from mergify_engine.web import api
from mergify_engine.web import utils as web_utils
from mergify_engine.web.api import applications
from mergify_engine.web.api import badges
from mergify_engine.web.api import eventlogs
from mergify_engine.web.api import events
from mergify_engine.web.api import gha_failed_jobs
from mergify_engine.web.api import partitions
from mergify_engine.web.api import pulls
from mergify_engine.web.api import queues
from mergify_engine.web.api import simulator
from mergify_engine.web.api import statistics


def api_enabled() -> None:
    if not settings.API_ENABLE:
        raise fastapi.HTTPException(status_code=404)


def include_api_routes(router: fastapi.APIRouter | fastapi.FastAPI) -> None:
    router.include_router(applications.router)
    router.include_router(queues.router)
    router.include_router(badges.router)
    router.include_router(simulator.router)
    router.include_router(eventlogs.router)
    router.include_router(statistics.router)
    router.include_router(partitions.router)
    router.include_router(events.router)
    router.include_router(gha_failed_jobs.router)
    router.include_router(pulls.router)


def create_app(cors_enabled: bool, debug: bool = False) -> fastapi.FastAPI:
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
                "name": "events",
                "description": "Enhanced operations with events.",
            },
            {
                "name": "gha_failed_jobs",
                "description": "Operations with failed workflow jobs.",
            },
            {
                "name": "pull_requests",
                "description": "Operations with pull requests.",
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
        debug=debug,
    )
    if cors_enabled:
        app.add_middleware(
            cors.CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    include_api_routes(app)

    web_utils.setup_exception_handlers(app)
    return app
