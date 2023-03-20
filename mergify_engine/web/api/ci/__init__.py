import fastapi

from mergify_engine.web.api.ci import index


router = fastapi.APIRouter(
    prefix="/ci",
)
router.include_router(index.router)
