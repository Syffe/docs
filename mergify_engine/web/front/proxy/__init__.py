import fastapi

from mergify_engine.web.front.proxy import saas


router = fastapi.APIRouter()


router.include_router(saas.router)
