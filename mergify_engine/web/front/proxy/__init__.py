import fastapi

from mergify_engine.web.front.proxy import github
from mergify_engine.web.front.proxy import saas
from mergify_engine.web.front.proxy import sentry


router = fastapi.APIRouter()


router.include_router(saas.router)
router.include_router(github.router)
router.include_router(sentry.router)
