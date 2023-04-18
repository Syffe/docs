import fastapi

from mergify_engine.web.api.partitions import index


router = fastapi.APIRouter(
    prefix="/repos/{owner}/{repository}",
)
router.include_router(index.router)
