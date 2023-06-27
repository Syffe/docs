from __future__ import annotations

import daiquiri
import fastapi

from mergify_engine.web.api.queues import configuration
from mergify_engine.web.api.queues import details
from mergify_engine.web.api.queues import freeze
from mergify_engine.web.api.queues import index
from mergify_engine.web.api.queues import new_details
from mergify_engine.web.api.queues import pause


LOG = daiquiri.getLogger(__name__)

router = fastapi.APIRouter(prefix="/repos/{owner}/{repository}")
router.include_router(configuration.router)
router.include_router(details.router)
router.include_router(freeze.router)
router.include_router(pause.router)
router.include_router(index.router)
router.include_router(new_details.router)
