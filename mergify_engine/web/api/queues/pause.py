from __future__ import annotations

import dataclasses
import datetime  # noqa: TCH003
import typing

import fastapi
import pydantic
from starlette.status import HTTP_204_NO_CONTENT

from mergify_engine import date
from mergify_engine import signals
from mergify_engine.queue import pause
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class QueuePause:
    reason: str = dataclasses.field(
        metadata={"description": "The reason of the queue pause"},
    )
    pause_date: datetime.datetime = dataclasses.field(
        metadata={"description": "The date and time of the freeze"},
    )


@pydantic.dataclasses.dataclass
class QueuePauseResponse:
    queue_pause: list[QueuePause] = dataclasses.field(
        metadata={"description": "The pause of the repository"},
    )


# FIXME(sileht): reuse dataclasses variante once
# https://github.com/tiangolo/fastapi/issues/4679 is fixed
class QueuePausePayload(pydantic.BaseModel):
    reason: str = pydantic.Field(
        max_length=255,
        description="The reason of the queue pause",
    )


@router.put(
    "/queue/pause",
    summary="Pause the merge queue",
    description="Pause the merge of the requested repository",
    response_model=QueuePauseResponse,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The pause does not exist"},
    },
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_queue_pause),
        fastapi.Depends(security.check_logged_user_has_write_access),
    ],
)
async def create_queue_pause(
    queue_pause_payload: QueuePausePayload,
    authentication_actor: security.AuthenticatedActor,
    repository_ctxt: security.RepositoryWithConfig,
) -> QueuePauseResponse:
    if queue_pause_payload.reason == "":
        queue_pause_payload.reason = "No pause reason was specified."

    queue_pause = await pause.QueuePause.get(repository_ctxt)
    if queue_pause is None:
        queue_pause = pause.QueuePause(
            repository=repository_ctxt,
            reason=queue_pause_payload.reason,
            pause_date=date.utcnow(),
        )

        await signals.send(
            repository_ctxt,
            None,
            None,
            "queue.pause.create",
            signals.EventQueuePauseCreateMetadata(
                {
                    "reason": queue_pause.reason,
                    "created_by": typing.cast(
                        signals.Actor,
                        authentication_actor.actor,
                    ),
                },
            ),
            "Create queue pause",
        )

    has_been_updated = False
    if queue_pause.reason != queue_pause_payload.reason:
        has_been_updated = True
        queue_pause.reason = queue_pause_payload.reason

    if has_been_updated:
        await signals.send(
            repository_ctxt,
            None,
            None,
            "queue.pause.update",
            signals.EventQueuePauseUpdateMetadata(
                {
                    "reason": queue_pause.reason,
                    "updated_by": typing.cast(
                        signals.Actor,
                        authentication_actor.actor,
                    ),
                },
            ),
            "Update queue pause",
        )

    await queue_pause.save()
    return QueuePauseResponse(
        queue_pause=[
            QueuePause(
                reason=queue_pause.reason,
                pause_date=queue_pause.pause_date,
            ),
        ],
    )


@router.delete(
    "/queue/pause",
    summary="Unpause merge queue",
    description="Unpause the merge queue of the requested repository",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_queue_pause),
        fastapi.Depends(security.check_logged_user_has_write_access),
    ],
    status_code=204,
    responses={
        **api.default_responses,  # type: ignore
        404: {
            "description": "The merge queue is not currently paused on this repository",
        },
    },
)
async def delete_queue_pause(
    authentication_actor: security.AuthenticatedActor,
    repository_ctxt: security.RepositoryWithConfig,
) -> fastapi.Response:
    queue_pause = pause.QueuePause(
        repository=repository_ctxt,
    )
    if not await queue_pause.delete():
        raise fastapi.HTTPException(
            status_code=404,
            detail="The merge queue is not currently paused on this repository",
        )

    await signals.send(
        repository_ctxt,
        None,
        None,
        "queue.pause.delete",
        signals.EventQueuePauseDeleteMetadata(
            {"deleted_by": typing.cast(signals.Actor, authentication_actor.actor)},
        ),
        "Delete queue pause",
    )

    return fastapi.Response(status_code=HTTP_204_NO_CONTENT)


@router.get(
    "/queue/pause",
    summary="Get queue pause data",
    description="Checks if the merge queue is paused and get the queue pause data",
    response_model=QueuePauseResponse,
    dependencies=[fastapi.Depends(security.check_subscription_feature_queue_pause)],
    responses={
        **api.default_responses,  # type: ignore
        404: {
            "description": "The merge queue is not currently paused on this repository",
        },
    },
)
async def get_queue_pause(
    repository_ctxt: security.RepositoryWithConfig,
) -> QueuePauseResponse:
    queue_pause = await pause.QueuePause.get(repository_ctxt)
    if queue_pause is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail="The merge queue is not currently paused on this repository",
        )

    return QueuePauseResponse(
        queue_pause=[
            QueuePause(
                reason=queue_pause.reason,
                pause_date=queue_pause.pause_date,
            ),
        ],
    )
