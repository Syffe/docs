import fastapi

from mergify_engine.web.front.proxy import engine
from mergify_engine.web.front.proxy import github
from mergify_engine.web.front.proxy import saas
from mergify_engine.web.front.proxy import sentry


router = fastapi.APIRouter(tags=["front"])


router.include_router(engine.router, prefix="/engine/v1")
router.include_router(saas.router)
router.include_router(github.router)
router.include_router(sentry.router)
