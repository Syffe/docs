from __future__ import annotations

import dataclasses
import datetime

import fastapi
import pydantic
from starlette.status import HTTP_204_NO_CONTENT

from mergify_engine import context
from mergify_engine import date
from mergify_engine.dashboard import application as application_mod
from mergify_engine.queue import freeze
from mergify_engine.rules.config import mergify as mergify_conf
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
    application_name: str = dataclasses.field(
        metadata={"description": "Application name responsible for the freeze"},
    )
    application_id: int = dataclasses.field(
        metadata={"description": "Application ID responsible for the freeze"},
    )
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
    application: application_mod.Application = fastapi.Security(  # noqa: B008
        security.get_application
    ),
    queue_name: qr_config.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:
    if queue_freeze_payload.reason == "":
        queue_freeze_payload.reason = "No freeze reason was specified."

    try:
        config = await repository_ctxt.get_mergify_config()
    except mergify_conf.InvalidRules:
        raise fastapi.HTTPException(
            status_code=422,
            detail="The configuration file is invalid.",
        )

    queue_rules = config["queue_rules"]
    if all(queue_name != rule.name for rule in queue_rules):
        raise fastapi.HTTPException(
            status_code=404, detail=f'The queue "{queue_name}" does not exist.'
        )

    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_name)
    if qf is None:
        qf = freeze.QueueFreeze(
            repository=repository_ctxt,
            name=queue_name,
            reason=queue_freeze_payload.reason,
            application_name=application.name,
            application_id=application.id,
            freeze_date=date.utcnow(),
            cascading=queue_freeze_payload.cascading,
        )

    if qf.reason != queue_freeze_payload.reason:
        qf.reason = queue_freeze_payload.reason

    if qf.cascading != queue_freeze_payload.cascading:
        qf.cascading = queue_freeze_payload.cascading

    await qf.save()
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
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
    application: application_mod.Application = fastapi.Security(  # noqa: B008
        security.get_application
    ),
    queue_name: qr_config.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> fastapi.Response:
    qf = freeze.QueueFreeze(
        repository=repository_ctxt,
        name=queue_name,
        application_name=application.name,
        application_id=application.id,
    )
    if not await qf.delete():
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" does not exist or is not currently frozen.',
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
    queue_name: qr_config.QueueName = fastapi.Path(  # noqa: B008
        ..., description="The name of the queue"
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:
    qf = await freeze.QueueFreeze.get(repository_ctxt, queue_name)
    if qf is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f'The queue "{queue_name}" does not exist or is not currently frozen.',
        )

    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
        ],
    )


@router.get(
    "/queues/freezes",  # noqa: FS003
    summary="Get the list of frozen queues",
    description="Get the list of frozen queues inside the requested repository",
    response_model=QueueFreezeResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_freeze)],
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def get_list_queue_freeze(
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> QueueFreezeResponse:
    return QueueFreezeResponse(
        queue_freezes=[
            QueueFreeze(
                name=qf.name,
                reason=qf.reason,
                application_name=qf.application_name,
                application_id=qf.application_id,
                freeze_date=qf.freeze_date,
                cascading=qf.cascading,
            )
            async for qf in freeze.QueueFreeze.get_all(repository_ctxt)
        ]
    )
