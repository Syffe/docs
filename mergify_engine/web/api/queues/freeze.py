from __future__ import annotations

import dataclasses
import datetime
import typing

import fastapi
import pydantic
from starlette.status import HTTP_204_NO_CONTENT

from mergify_engine import date
from mergify_engine import signals
from mergify_engine.queue import freeze
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class QueueFreeze:
    name: str = dataclasses.field(
        default_factory=str, metadata={"description": "Queue name"}
    )
    reason: str = dataclasses.field(
        default_factory=str, metadata={"description": "The reason of the queue freeze"}
    )
    freeze_date: datetime.datetime = dataclasses.field(
        default_factory=date.utcnow,
        metadata={"description": "The date and time of the freeze"},
    )
    cascading: bool = dataclasses.field(
        default=True,
        metadata={"description": "The active status of the cascading effect"},
    )


@pydantic.dataclasses.dataclass
class QueueFreezeResponse:
    queue_freezes: list[QueueFreeze] = dataclasses.field(
        default_factory=list,
        metadata={"description": "The frozen queues of the repository"},
    )


# FIXME(sileht): reuse dataclasses variante once
# https://github.com/tiangolo/fastapi/issues/4679 is fixed
class QueueFreezePayload(pydantic.BaseModel):
    reason: str = pydantic.Field(
        max_length=255, description="The reason of the queue freeze"
    )
    cascading: bool = pydantic.Field(
        default=True, description="The active status of the cascading effect"
    )


@router.put(
    "/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Freezes merge queue",
    description="Freezes the merge of the requested queue and the queues following it",
    response_model=QueueFreezeResponse,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist"},
    },
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
)
async def create_queue_freeze(
    queue_freeze_payload: QueueFreezePayload,
    queue_name: typing.Annotated[
        qr_config.QueueName, fastapi.Path(description="The name of the queue")
    ],
    auth: security.HttpAuth,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
) -> QueueFreezeResponse:
    if queue_freeze_payload.reason == "":
        queue_freeze_payload.reason = "No freeze reason was specified."

    try:
        queue_rule = queue_rules[queue_name]
    except KeyError:
        raise fastapi.HTTPException(
            status_code=404, detail=f'The queue "{queue_name}" does not exist.'
        )

    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_rule)
    if qf is None:
        qf = freeze.QueueFreeze(
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
            "queue.freeze.create",
            signals.EventQueueFreezeCreateMetadata(
                {
                    "queue_name": queue_name,
                    "reason": qf.reason,
                    "cascading": qf.cascading,
                    "created_by": auth.actor,
                }
            ),
            "Create queue freeze",
        )

    has_been_updated = False
    if qf.reason != queue_freeze_payload.reason:
        has_been_updated = True
        qf.reason = queue_freeze_payload.reason

    if qf.cascading != queue_freeze_payload.cascading:
        has_been_updated = True
        qf.cascading = queue_freeze_payload.cascading

    if has_been_updated:
        await signals.send(
            repository_ctxt,
            None,
            "queue.freeze.update",
            signals.EventQueueFreezeUpdateMetadata(
                {
                    "queue_name": queue_name,
                    "reason": qf.reason,
                    "cascading": qf.cascading,
                    "updated_by": auth.actor,
                }
            ),
            "Update queue freeze",
        )

    await qf.save(queue_rules)
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
        ],
    )


@router.delete(
    "/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Unfreeze merge queue",
    description="Unfreeze the specified merge queue",
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    status_code=204,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def delete_queue_freeze(
    queue_name: typing.Annotated[
        qr_config.QueueName, fastapi.Path(description="The name of the queue")
    ],
    auth: security.HttpAuth,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
) -> fastapi.Response:
    try:
        queue_rule = queue_rules[queue_name]
    except KeyError:
        raise fastapi.HTTPException(
            status_code=404, detail=f'The queue "{queue_name}" does not exist.'
        )

    qf = freeze.QueueFreeze(
        repository=repository_ctxt,
        queue_rule=queue_rule,
        name=queue_name,
    )
    if not await qf.delete(queue_rules):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" is not currently frozen.',
        )

    await signals.send(
        repository_ctxt,
        None,
        "queue.freeze.delete",
        signals.EventQueueFreezeDeleteMetadata(
            {"queue_name": queue_name, "deleted_by": auth.actor}
        ),
        "Delete queue freeze",
    )

    return fastapi.Response(status_code=HTTP_204_NO_CONTENT)


@router.get(
    "/queue/{queue_name}/freeze",  # noqa: FS003
    summary="Get queue freeze data",
    description="Checks if the queue is frozen and get the queue freeze data",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The queue does not exist or is not currently frozen"},
    },
)
async def get_queue_freeze(
    queue_name: typing.Annotated[
        qr_config.QueueName, fastapi.Path(description="The name of the queue")
    ],
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
) -> QueueFreezeResponse:
    try:
        queue_rule = queue_rules[queue_name]
    except KeyError:
        raise fastapi.HTTPException(
            status_code=404, detail=f'The queue "{queue_name}" does not exist.'
        )

    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_rule)
    if qf is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" is not currently frozen.',
        )

    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
        ],
    )


@router.get(
    "/queues/freezes",
    summary="Get the list of frozen queues",
    description="Get the list of frozen queues inside the requested repository",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def get_list_queue_freeze(
    repository_ctxt: security.Repository, queue_rules: security.QueueRules
) -> QueueFreezeResponse:
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
            async for qf in freeze.QueueFreeze.get_all(repository_ctxt, queue_rules)
        ]
    )
