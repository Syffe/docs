import dataclasses
import datetime
import enum

import fastapi
import pydantic
import typing_extensions

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import github_actions
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(tags=["gha_failed_jobs"])


class Owner(typing_extensions.TypedDict):
    id: int
    login: str


class Repository(typing_extensions.TypedDict):
    id: int
    name: str
    owner: Owner


class FlakyStatus(enum.Enum):
    FLAKY = "yes"
    NOT_FLAKY = "no"
    UNKNOWN = "unknown"


class WorkflowJob(typing_extensions.TypedDict):
    name: str
    error_description: str | None
    id: int
    run_id: int
    steps: list[github_types.GitHubWorkflowJobStep]
    started_at: github_types.ISODateTimeType
    completed_at: github_types.ISODateTimeType
    flaky: FlakyStatus
    run_attempt: int


class WorkflowJobGroup(typing_extensions.TypedDict):
    workflow_jobs: list[WorkflowJob]


@pydantic.dataclasses.dataclass
class FailedJobResponse:
    repository: Repository
    start_at: datetime.date | None
    min_similarity: float
    workflow_job_groups: list[WorkflowJobGroup] = dataclasses.field(
        default_factory=list,
        metadata={"description": "Failed jobs of the repository"},
    )


@router.get(
    "/repos/{owner}/{repository}/gha-failed-jobs",
    summary="Get failed jobs and related potential failed jobs",
    description="Get failed jobs and related potential failed jobs",
    response_model=FailedJobResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "The repository is not found"},
    },
)
async def get_gha_failed_jobs(
    session: database.Session,
    repository_ctxt: security.Repository,
    start_at: datetime.date | None = None,
    neighbour_cosine_similarity_threshold: float = 0.01,
) -> FailedJobResponse:
    results = await github_actions.WorkflowJob.get_failed_jobs(
        session,
        repository_ctxt.repo["id"],
        start_at,
        neighbour_cosine_similarity_threshold,
    )

    wfj_groups = {}  # type: ignore[var-annotated]
    for failed_job in results:
        for group_id in [failed_job.id, *failed_job.neighbour_job_ids]:
            if group_id in wfj_groups:
                wfj_group = wfj_groups[group_id]
                break
        else:
            wfj_group = wfj_groups.setdefault(failed_job.id, {"workflow_jobs": []})

        wfj_group["workflow_jobs"].append(
            WorkflowJob(
                name=failed_job.name,
                error_description=failed_job.embedded_log_error_title,
                id=failed_job.id,
                run_id=failed_job.workflow_run_id,
                steps=failed_job.steps or [],
                started_at=github_types.ISODateTimeType(str(failed_job.started_at)),
                completed_at=github_types.ISODateTimeType(str(failed_job.completed_at)),
                flaky=FlakyStatus.FLAKY if failed_job.flaky else FlakyStatus.UNKNOWN,
                run_attempt=failed_job.run_attempt,
            )
        )

    return FailedJobResponse(
        repository=Repository(
            id=repository_ctxt.repo["id"],
            name=repository_ctxt.repo["name"],
            owner=Owner(
                id=repository_ctxt.repo["owner"]["id"],
                login=repository_ctxt.repo["owner"]["login"],
            ),
        ),
        start_at=start_at,
        min_similarity=neighbour_cosine_similarity_threshold,
        workflow_job_groups=list(wfj_groups.values()),
    )
