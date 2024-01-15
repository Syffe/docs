from __future__ import annotations

import dataclasses
import datetime  # noqa: TCH003
import typing

import daiquiri
import fastapi
import pydantic
from starlette.status import HTTP_204_NO_CONTENT

from mergify_engine import date
from mergify_engine import signals
from mergify_engine.queue import freeze
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)

router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class QueueFreeze:
    name: str = dataclasses.field(metadata={"description": "Queue name"})
    reason: str = dataclasses.field(
        metadata={"description": "The reason of the queue freeze"},
    )
    freeze_date: datetime.datetime = dataclasses.field(
        metadata={"description": "The date and time of the freeze"},
    )
    cascading: bool = dataclasses.field(
        default=True,
        metadata={"description": "The active status of the cascading effect"},
    )


@pydantic.dataclasses.dataclass
class QueueFreezeResponse:
    queue_freezes: list[QueueFreeze] = dataclasses.field(
        metadata={"description": "The frozen queues of the repository"},
    )


class QueueFreezePayload(pydantic.BaseModel):
    reason: str = pydantic.Field(
        max_length=255,
        description="The reason of the queue freeze",
    )
    cascading: bool = pydantic.Field(
        default=True,
        description="The active status of the cascading effect",
    )


@router.put(
    "/queue/{queue_name}/freeze",
    summary="Freezes merge queue",
    description="Freezes the merge of the requested queue and the queues following it",
    response_model=QueueFreezeResponse,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "The queue does not exist"},
    },
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_queue_freeze),
        fastapi.Depends(security.check_logged_user_has_write_access),
    ],
)
async def create_queue_freeze(
    queue_freeze_payload: QueueFreezePayload,
    queue_rule: security.QueueRuleByNameFromPath,
    authentication_actor: security.AuthenticatedActor,
    repository_ctxt: security.RepositoryWithConfig,
) -> QueueFreezeResponse:
    if not queue_freeze_payload.reason:
        queue_freeze_payload.reason = "No freeze reason was specified."

    queue_freeze = await freeze.QueueFreeze.get(repository_ctxt, queue_rule)
    if queue_freeze is None:
        queue_freeze = freeze.QueueFreeze(
            repository=repository_ctxt,
            queue_rule=queue_rule,
            name=queue_rule.name,
            reason=queue_freeze_payload.reason,
            freeze_date=date.utcnow(),
            cascading=queue_freeze_payload.cascading,
        )

        await signals.send(
            repository_ctxt,
            None,
            None,
            "queue.freeze.create",
            signals.EventQueueFreezeCreateMetadata(
                {
                    "queue_name": queue_rule.name,
                    "reason": queue_freeze.reason,
                    "cascading": queue_freeze.cascading,
                    "created_by": typing.cast(
                        signals.Actor,
                        authentication_actor.actor,
                    ),
                },
            ),
            "Create queue freeze",
        )

        LOG.info(
            "Create queue freeze",
            queue_name=queue_rule.name,
            reason=queue_freeze.reason,
            cascading=queue_freeze.cascading,
            created_by=authentication_actor.actor,
        )

    has_been_updated = False
    if queue_freeze.reason != queue_freeze_payload.reason:
        has_been_updated = True
        queue_freeze.reason = queue_freeze_payload.reason

    if queue_freeze.cascading != queue_freeze_payload.cascading:
        has_been_updated = True
        queue_freeze.cascading = queue_freeze_payload.cascading

    if has_been_updated:
        await signals.send(
            repository_ctxt,
            None,
            None,
            "queue.freeze.update",
            signals.EventQueueFreezeUpdateMetadata(
                {
                    "queue_name": queue_rule.name,
                    "reason": queue_freeze.reason,
                    "cascading": queue_freeze.cascading,
                    "updated_by": typing.cast(
                        signals.Actor,
                        authentication_actor.actor,
                    ),
                },
            ),
            "Update queue freeze",
        )

        LOG.info(
            "Update queue freeze",
            queue_name=queue_rule.name,
            reason=queue_freeze.reason,
            cascading=queue_freeze.cascading,
            updated_by=authentication_actor.actor,
        )

    await queue_freeze.save()
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=queue_freeze.name,
                reason=queue_freeze.reason,
                freeze_date=queue_freeze.freeze_date,
                cascading=queue_freeze.cascading,
            ),
        ],
    )


@router.delete(
    "/queue/{queue_name}/freeze",
    summary="Unfreeze merge queue",
    description="Unfreeze the specified merge queue",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_queue_freeze),
        fastapi.Depends(security.check_logged_user_has_write_access),
    ],
    status_code=204,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def delete_queue_freeze(
    authentication_actor: security.AuthenticatedActor,
    repository_ctxt: security.RepositoryWithConfig,
    queue_rule: security.QueueRuleByNameFromPath,
) -> fastapi.Response:
    queue_freeze = freeze.QueueFreeze(
        repository=repository_ctxt,
        queue_rule=queue_rule,
        name=queue_rule.name,
    )
    if not await queue_freeze.delete():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_rule.name}" is not currently frozen.',
        )

    await signals.send(
        repository_ctxt,
        None,
        None,
        "queue.freeze.delete",
        signals.EventQueueFreezeDeleteMetadata(
            {
                "queue_name": queue_rule.name,
                "deleted_by": typing.cast(signals.Actor, authentication_actor.actor),
            },
        ),
        "Delete queue freeze",
    )

    LOG.info(
        "Delete queue freeze",
        queue_name=queue_rule.name,
        deleted_by=authentication_actor.actor,
    )

    return fastapi.Response(status_code=HTTP_204_NO_CONTENT)


@router.get(
    "/queue/{queue_name}/freeze",
    summary="Get queue freeze data",
    description="Checks if the queue is frozen and get the queue freeze data",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def get_queue_freeze(
    repository_ctxt: security.RepositoryWithConfig,
    queue_rule: security.QueueRuleByNameFromPath,
) -> QueueFreezeResponse:
    queue_freeze = await freeze.QueueFreeze.get(repository_ctxt, queue_rule)
    if queue_freeze is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_rule.name}" is not currently frozen.',
        )

    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=queue_freeze.name,
                reason=queue_freeze.reason,
                freeze_date=queue_freeze.freeze_date,
                cascading=queue_freeze.cascading,
            ),
        ],
    )


@router.get(
    "/queues/freezes",
    summary="Get the list of frozen queues",
    description="Get the list of frozen queues inside the requested repository",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses=api.default_responses,
)
async def get_list_queue_freeze(
    repository_ctxt: security.RepositoryWithConfig,
) -> QueueFreezeResponse:
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=queue_freeze.name,
                reason=queue_freeze.reason,
                freeze_date=queue_freeze.freeze_date,
                cascading=queue_freeze.cascading,
            )
            async for queue_freeze in freeze.QueueFreeze.get_all(repository_ctxt)
        ],
    )
