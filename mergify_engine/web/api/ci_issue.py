from collections import OrderedDict
import dataclasses

import fastapi
import pydantic
import sqlalchemy
import typing_extensions

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.models.views.workflows import FlakyStatus
from mergify_engine.models.views.workflows import WorkflowJobEnhanced
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(tags=["ci_issues"])


class CiIssueEvent(typing_extensions.TypedDict):
    id: int
    run_id: int
    started_at: github_types.ISODateTimeType
    flaky: FlakyStatus
    failed_run_count: int


@pydantic.dataclasses.dataclass
class CiIssueResponse:
    id: int
    short_id: str
    name: str
    job_name: str
    events: list[CiIssueEvent] = dataclasses.field(
        default_factory=list,
        metadata={"description": "Event link to this CiIssue"},
    )


@pydantic.dataclasses.dataclass
class CiIssuesResponse:
    issues: list[CiIssueResponse] = dataclasses.field(
        default_factory=list,
        metadata={"description": "List of CiIssue"},
    )


async def query_issues(
    session: database.Session,
    repository_id: github_types.GitHubRepositoryIdType,
    issue_id: int | None = None,
) -> OrderedDict[int, CiIssueResponse]:
    stmt = (
        sqlalchemy.select(
            CiIssue.id,
            CiIssue.short_id,
            CiIssue.name,
            WorkflowJobEnhanced.id.label("job_id"),
            WorkflowJobEnhanced.name_without_matrix.label("job_name"),
            WorkflowJobEnhanced.workflow_run_id,
            WorkflowJobEnhanced.started_at,
            WorkflowJobEnhanced.flaky,
            WorkflowJobEnhanced.failed_run_count,
        )
        .join(
            gh_models.GitHubRepository,
            gh_models.GitHubRepository.id == CiIssue.repository_id,
        )
        .join(WorkflowJobEnhanced, WorkflowJobEnhanced.ci_issue_id == CiIssue.id)
        .where(CiIssue.repository_id == repository_id)
        .order_by(sqlalchemy.desc(WorkflowJobEnhanced.started_at))
    )

    if issue_id is not None:
        stmt = stmt.where(CiIssue.id == issue_id)

    # NOTE(Kontrolix): We use an OrderedDict here to be sure to keep the sorting of
    # the SQL query
    reponses: OrderedDict[int, CiIssueResponse] = OrderedDict()
    for result in await session.execute(stmt):
        if result.id in reponses:
            reponse = reponses[result.id]
        else:
            reponse = CiIssueResponse(
                id=result.id,
                short_id=result.short_id,
                name=result.name or f"Failure of {result.job_name}",
                job_name=result.job_name,
            )
            reponses[result.id] = reponse

        reponse.events.append(
            CiIssueEvent(
                id=result.job_id,
                run_id=result.workflow_run_id,
                started_at=github_types.ISODateTimeType(result.started_at.isoformat()),
                flaky=FlakyStatus(result.flaky),
                failed_run_count=result.failed_run_count,
            )
        )

    if issue_id is not None and len(reponses) > 1:
        raise RuntimeError("This should never happens")

    return reponses


@router.get(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Get CI issues",
    description="Get CI issues",
    response_model=CiIssuesResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def get_ci_issues(
    session: database.Session,
    repository_ctxt: security.Repository,
) -> CiIssuesResponse:
    return CiIssuesResponse(
        issues=list((await query_issues(session, repository_ctxt.repo["id"])).values())
    )


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_id}",
    summary="Get a CI issue",
    description="Get a CI issue",
    response_model=CiIssueResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "CI issue not found"},
    },
)
async def get_ci_issue(
    session: database.Session, repository_ctxt: security.Repository, ci_issue_id: int
) -> CiIssueResponse:
    reponses = await query_issues(session, repository_ctxt.repo["id"], ci_issue_id)
    if len(reponses) == 0:
        raise fastapi.HTTPException(404)
    return reponses[ci_issue_id]
