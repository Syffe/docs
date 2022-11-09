import fastapi

from mergify_engine.web.front.proxy import github
from mergify_engine.web.front.proxy import saas


router = fastapi.APIRouter()


router.include_router(saas.router)
router.include_router(github.router)
