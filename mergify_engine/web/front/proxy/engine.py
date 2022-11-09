import fastapi

from mergify_engine.web.api import root


router = fastapi.APIRouter()
root.include_api_routes(router)
