from collections import OrderedDict
import dataclasses
import typing

import fastapi
import pydantic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.models.ci_issue import CiIssueStatus
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
    status: CiIssueStatus
    events: list[CiIssueEvent] = dataclasses.field(
        metadata={"description": "Event link to this CiIssue"},
    )


class CiIssuesResponse(pagination.PageResponse[CiIssueResponse]):
    items_key: typing.ClassVar[str] = "issues"
    issues: list[CiIssueResponse] = dataclasses.field(
        metadata={"description": "List of CiIssue"},
    )


class CiIssueBody(pydantic.BaseModel):
    status: CiIssueStatus


async def query_issues(
    session: database.Session,
    repository_id: github_types.GitHubRepositoryIdType,
    limit: int,
    issue_id: int | None = None,
    statuses: tuple[CiIssueStatus, ...] | None = None,
    cursor: pagination.Cursor | None = None,
) -> list[CiIssueResponse]:
    if cursor is None:
        cursor = pagination.Cursor("")

    # NOTE(charly): subquery to filter and limit issues, for pagination mainly
    filtered_issues = (
        sqlalchemy.select(CiIssue)
        .where(CiIssue.repository_id == repository_id)
        .order_by(
            CiIssue.id.desc() if cursor.forward else CiIssue.id.asc(),
        )
        .limit(limit)
    )

    if issue_id is not None:
        filtered_issues = filtered_issues.where(CiIssue.id == issue_id)
    if statuses is not None:
        filtered_issues = filtered_issues.where(CiIssue.status.in_(statuses))
    if cursor.value:
        try:
            cursor_issue_id = int(cursor.value)
        except ValueError:
            raise pagination.InvalidCursor(cursor)

        if cursor.forward:
            filtered_issues = filtered_issues.where(CiIssue.id < cursor_issue_id)
        else:
            filtered_issues = filtered_issues.where(CiIssue.id > cursor_issue_id)

    filtered_issues_cte = orm.aliased(
        CiIssue,
        filtered_issues.cte("filtered_issues"),
    )

    stmt = (
        sqlalchemy.select(
            filtered_issues_cte.id,
            filtered_issues_cte.short_id,
            filtered_issues_cte.name,
            filtered_issues_cte.status,
            WorkflowJobEnhanced.id.label("job_id"),
            WorkflowJobEnhanced.name_without_matrix.label("job_name"),
            WorkflowJobEnhanced.workflow_run_id,
            WorkflowJobEnhanced.started_at,
            WorkflowJobEnhanced.flaky,
            WorkflowJobEnhanced.failed_run_count,
        )
        .join(
            gh_models.GitHubRepository,
            gh_models.GitHubRepository.id == filtered_issues_cte.repository_id,
        )
        .join(
            WorkflowJobEnhanced,
            sqlalchemy.and_(
                WorkflowJobEnhanced.ci_issue_id == filtered_issues_cte.id,
                WorkflowJobEnhanced.repository_id == repository_id,
            ),
        )
        .order_by(WorkflowJobEnhanced.started_at.desc())
    )

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
                status=result.status,
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

    return list(reponses.values())


@router.get(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Get CI issues",
    description="Get CI issues",
    response_model=CiIssuesResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
    },
)
async def get_ci_issues(
    session: database.Session,
    repository_ctxt: security.Repository,
    page: pagination.CurrentPage,
    status: typing.Annotated[
        tuple[CiIssueStatus, ...],
        fastapi.Query(description="CI issue status"),
    ] = (CiIssueStatus.UNRESOLVED,),
) -> CiIssuesResponse:
    issues = await query_issues(
        session,
        repository_ctxt.repo["id"],
        limit=page.per_page,
        statuses=status,
        cursor=page.cursor,
    )

    first_issue_id = issues[0].id if issues else None
    last_issue_id = issues[-1].id if issues else None

    page_response: pagination.Page[CiIssueResponse] = pagination.Page(
        items=issues,
        current=page,
        cursor_prev=page.cursor.previous(first_issue_id, last_issue_id),
        cursor_next=page.cursor.next(first_issue_id, last_issue_id),
    )
    return CiIssuesResponse(page=page_response)  # type: ignore[call-arg]


@router.patch(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Partially update CI issues",
    description="Update some properties of several CI issues",
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
    },
)
async def patch_ci_issues(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_ids: typing.Annotated[
        list[int],
        fastapi.Query(description="IDs of CI Issues", default_factory=list, alias="id"),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssue)
        .values(status=json.status)
        .where(CiIssue.id.in_(ci_issue_ids))
    )
    await session.execute(stmt)
    await session.commit()


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_id}",
    summary="Get a CI issue",
    description="Get a CI issue",
    response_model=CiIssueResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
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
    reponses = await query_issues(
        session,
        repository_ctxt.repo["id"],
        limit=1,
        issue_id=ci_issue_id,
    )
    if len(reponses) == 0:
        raise fastapi.HTTPException(404)
    return reponses[0]


@router.patch(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_id}",
    summary="Partially update a CI issue",
    description="Update some properties of a CI issue",
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "CI issue not found"},
    },
)
async def patch_ci_issue(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_id: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue"),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssue)
        .values(status=json.status)
        .where(CiIssue.id == ci_issue_id)
        .returning(CiIssue.id)
    )
    result = await session.execute(stmt)
    await session.commit()

    updated_ci_issue_id = result.scalar_one_or_none()
    if updated_ci_issue_id is None:
        raise fastapi.HTTPException(404)


@pydantic.dataclasses.dataclass
class CiIssueEventDetailResponse(CiIssueEvent):
    completed_at: github_types.ISODateTimeType
    log_extract: str
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
        **api.default_responses,  # type: ignore[dict-item]
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
        WorkflowJobEnhanced.log_extract,
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
        log_extract=event_detail.log_extract,
        run_attempt=event_detail.run_attempt,
        name=event_detail.name,
    )
