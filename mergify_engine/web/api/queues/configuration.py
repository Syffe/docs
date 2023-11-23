from __future__ import annotations

import dataclasses

import fastapi
import pydantic

from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.queues import types


router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class QueuesConfig:
    configuration: list[types.QueueRule] = dataclasses.field(
        metadata={"description": "The queues configuration of the repository"},
    )


@router.get(
    "/queues/configuration",
    summary="Get merge queues configuration",
    description="Get the list of all merge queues configuration sorted by processing order",
    response_model=QueuesConfig,
    responses={
        **api.default_responses,  # type: ignore
        422: {"description": "The configuration file is invalid."},
    },
)
async def repository_queues_configuration(
    repository_ctxt: security.RepositoryWithConfig,
) -> QueuesConfig:
    return QueuesConfig(
        [
            types.QueueRule(
                config=rule.config,
                name=rule.name,
            )
            for rule in repository_ctxt.mergify_config["queue_rules"]
        ],
    )
