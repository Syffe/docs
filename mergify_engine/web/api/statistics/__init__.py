import fastapi

from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import checks_duration
from mergify_engine.web.api.statistics import queue_checks_outcome
from mergify_engine.web.api.statistics import time_to_merge


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)

router.include_router(time_to_merge.router)
router.include_router(queue_checks_outcome.router)
router.include_router(checks_duration.router)
