import fastapi

from mergify_engine.web.api import security
from mergify_engine.web.api.statistics import average_ci_runtime
from mergify_engine.web.api.statistics import checks_duration
from mergify_engine.web.api.statistics import pull_requests_merged
from mergify_engine.web.api.statistics import queue_checks_outcome
from mergify_engine.web.api.statistics import time_to_merge


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Security(security.require_authentication),
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats),
    ],
)

router.include_router(time_to_merge.router)
router.include_router(queue_checks_outcome.router)
router.include_router(checks_duration.router)
router.include_router(pull_requests_merged.router)
router.include_router(average_ci_runtime.router)
