import fastapi

from mergify_engine.web.api.ci import index


router = fastapi.APIRouter(
    prefix="/ci",  # noqa: [FS003]
)
router.include_router(index.router)
