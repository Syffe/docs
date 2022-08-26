# -*- encoding: utf-8 -*-
#
# Copyright © 2022 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import statistics
import typing

import fastapi

from mergify_engine import context
from mergify_engine.queue import statistics as queue_statistics
from mergify_engine.web.api import security


router = fastapi.APIRouter(
    tags=["statistics"],
    dependencies=[
        fastapi.Depends(security.require_authentication),
    ],
)


class TimeToMergeResponse(typing.TypedDict):
    mean: float


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/time_to_merge",  # noqa: FS003
    summary="Get the average time to merge statistics for the specified queue name",
    description="Get the average time to merge statistics for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=TimeToMergeResponse,
)
async def get_average_time_to_merge_stats(
    queue_name: str = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the average time to merge after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the average time to merge before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> TimeToMergeResponse:
    stats = await queue_statistics.get_time_to_merge_stats(
        repository_ctxt,
        queue_name,
        start_at,
        end_at,
    )
    return TimeToMergeResponse(mean=statistics.fmean(stats))


class ChecksDurationResponse(typing.TypedDict):
    mean: float


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/checks_duration",  # noqa: FS003
    summary="Get the average checks duration statistics for the specified queue name",
    description="Get the average checks duration statistics for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=ChecksDurationResponse,
)
async def get_checks_duration_stats(
    queue_name: str = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> ChecksDurationResponse:
    stats = await queue_statistics.get_checks_duration_stats(
        repository_ctxt, queue_name, start_at, end_at
    )
    return ChecksDurationResponse(mean=statistics.fmean(stats))


@router.get(
    "/repos/{owner}/{repository}/queues/{queue_name}/stats/failure_by_reason",  # noqa: FS003
    summary="Get the failure by reason statistics for the specified queue name",
    description="Get the failure by reason statistics for the specified queue name",
    dependencies=[
        fastapi.Depends(security.check_subscription_feature_merge_queue_stats)
    ],
    response_model=queue_statistics.FailureByReasonT,
)
async def get_failure_by_reason_stats(
    queue_name: str = fastapi.Path(  # noqa: B008
        ...,
        description="Name of the queue",
    ),
    start_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened after this timestamp",
    ),
    end_at: int
    | None = fastapi.Query(  # noqa: B008
        default=None,
        description="Retrieve the stats that happened before this timestamp",
    ),
    repository_ctxt: context.Repository = fastapi.Depends(  # noqa: B008
        security.get_repository_context
    ),
) -> queue_statistics.FailureByReasonT:
    return await queue_statistics.get_failure_by_reason_stats(
        repository_ctxt, queue_name, start_at, end_at
    )
