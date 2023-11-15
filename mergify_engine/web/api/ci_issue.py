from collections import OrderedDict
import dataclasses
import typing

import fastapi
import pydantic
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.models.views.workflows import FlakyStatus
from mergify_engine.models.views.workflows import WorkflowJobEnhanced
from mergify_engine.web import api
from mergify_engine.web.api import security


router = fastapi.APIRouter(tags=["ci_issues"])


@pydantic.dataclasses.dataclass
class CiIssueEvent:
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
        metadata={"description": "Event link to this CiIssue"},
    )


@pydantic.dataclasses.dataclass
class CiIssuesResponse:
    issues: list[CiIssueResponse] = dataclasses.field(
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
        .join(
            WorkflowJobEnhanced,
            sqlalchemy.and_(
                WorkflowJobEnhanced.ci_issue_id == CiIssue.id,
                WorkflowJobEnhanced.repository_id == CiIssue.repository_id,
            ),
        )
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
                events=[],
            )
            reponses[result.id] = reponse

        reponse.events.append(
            CiIssueEvent(
                id=result.job_id,
                run_id=result.workflow_run_id,
                started_at=github_types.ISODateTimeType(result.started_at.isoformat()),
                flaky=FlakyStatus(result.flaky),
                failed_run_count=result.failed_run_count,
            ),
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
        issues=list((await query_issues(session, repository_ctxt.repo["id"])).values()),
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
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_id: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue"),
    ],
) -> CiIssueResponse:
    reponses = await query_issues(session, repository_ctxt.repo["id"], ci_issue_id)
    if len(reponses) == 0:
        raise fastapi.HTTPException(404)
    return reponses[ci_issue_id]


@pydantic.dataclasses.dataclass
class CiIssueEventDetailResponse(CiIssueEvent):
    completed_at: github_types.ISODateTimeType
    embedded_log: str
    failed_step_number: int | None
    name: str
    run_attempt: int
    steps: list[github_types.GitHubWorkflowJobStep]


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_id}/events/{event_id}",
    summary="Get a detailed event of a CI issue",
    description="Get a detailed event of a CI issue",
    response_model=CiIssueEventDetailResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "CI issue event not found"},
    },
)
async def get_ci_issue_event_detail(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_id: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue"),
    ],
    event_id: typing.Annotated[int, fastapi.Path(description="The ID of the Event")],
) -> CiIssueEventDetailResponse:
    stmt = sqlalchemy.select(
        WorkflowJobEnhanced.id,
        WorkflowJobEnhanced.name_without_matrix.label("name"),
        WorkflowJobEnhanced.workflow_run_id,
        WorkflowJobEnhanced.started_at,
        WorkflowJobEnhanced.completed_at,
        WorkflowJobEnhanced.steps,
        WorkflowJobEnhanced.failed_step_number,
        WorkflowJobEnhanced.flaky,
        WorkflowJobEnhanced.failed_run_count,
        WorkflowJobEnhanced.embedded_log,
        WorkflowJobEnhanced.ci_issue_id,
        WorkflowJobEnhanced.run_attempt,
    ).where(
        WorkflowJobEnhanced.repository_id == repository_ctxt.repo["id"],
        WorkflowJobEnhanced.ci_issue_id == ci_issue_id,
        WorkflowJobEnhanced.id == event_id,
    )

    event_detail = (await session.execute(stmt)).one_or_none()

    if event_detail is None:
        raise fastapi.HTTPException(404)

    return CiIssueEventDetailResponse(
        id=event_detail.id,
        run_id=event_detail.workflow_run_id,
        steps=event_detail.steps,
        failed_step_number=event_detail.failed_step_number,
        completed_at=github_types.ISODateTimeType(
            event_detail.completed_at.isoformat(),
        ),
        started_at=github_types.ISODateTimeType(event_detail.started_at.isoformat()),
        flaky=FlakyStatus(event_detail.flaky),
        failed_run_count=event_detail.failed_run_count,
        embedded_log=event_detail.embedded_log,
        run_attempt=event_detail.run_attempt,
        name=event_detail.name,
    )
