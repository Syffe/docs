from collections import OrderedDict
import dataclasses
import typing

import daiquiri
import fastapi
import pydantic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssueGPT
from mergify_engine.models.ci_issue import CiIssueStatus
from mergify_engine.models.github.pull_request import PullRequestHeadShaHistory
from mergify_engine.models.github.workflows import FlakyStatus
from mergify_engine.models.views.workflows import WorkflowJobEnhanced
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)

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


async def get_filtered_issues_cte(
    repository_id: github_types.GitHubRepositoryIdType,
    limit: int,
    issue_id: int | None = None,
    statuses: tuple[CiIssueStatus, ...] | None = None,
    cursor: pagination.Cursor | None = None,
) -> type[CiIssueGPT]:
    if cursor is None:
        cursor = pagination.Cursor("")

    # NOTE(charly): subquery to filter and limit issues, for pagination mainly
    filtered_issues = (
        sqlalchemy.select(CiIssueGPT)
        .where(CiIssueGPT.repository_id == repository_id)
        .order_by(
            CiIssueGPT.id.desc() if cursor.forward else CiIssueGPT.id.asc(),
        )
        .limit(limit)
    )

    if issue_id is not None:
        filtered_issues = filtered_issues.where(CiIssueGPT.id == issue_id)
    if statuses is not None:
        filtered_issues = filtered_issues.where(CiIssueGPT.status.in_(statuses))

    if cursor.value:
        try:
            cursor_issue_id = int(cursor.value)
        except ValueError:
            raise pagination.InvalidCursor(cursor)

        if cursor.forward:
            filtered_issues = filtered_issues.where(CiIssueGPT.id < cursor_issue_id)
        else:
            filtered_issues = filtered_issues.where(CiIssueGPT.id > cursor_issue_id)

    return orm.aliased(
        CiIssueGPT,
        filtered_issues.cte("filtered_issues"),
    )


async def query_issues(
    session: database.Session,
    repository_id: github_types.GitHubRepositoryIdType,
    limit: int,
    issue_id: int | None = None,
    statuses: tuple[CiIssueStatus, ...] | None = None,
    cursor: pagination.Cursor | None = None,
) -> list[CiIssueResponse]:
    filtered_issues_cte = await get_filtered_issues_cte(
        repository_id,
        limit,
        issue_id,
        statuses,
        cursor,
    )

    stmt = (
        sqlalchemy.select(
            filtered_issues_cte.id,
            filtered_issues_cte.short_id,
            filtered_issues_cte.name,
            filtered_issues_cte.status,
            gh_models.WorkflowJobLogMetadata.id.label("event_id"),
            WorkflowJobEnhanced.name_without_matrix.label("job_name"),
            WorkflowJobEnhanced.workflow_run_id,
            WorkflowJobEnhanced.started_at,
            WorkflowJobEnhanced.flaky,
            WorkflowJobEnhanced.failed_run_count,
            sqlalchemy.func.array_remove(
                sqlalchemy.func.array_agg(gh_models.PullRequest.number.distinct()),
                None,
            ).label("pull_requests"),
        )
        .join(
            gh_models.GitHubRepository,
            gh_models.GitHubRepository.id == filtered_issues_cte.repository_id,
        )
        .join(
            gh_models.WorkflowJobLogMetadata,
            gh_models.WorkflowJobLogMetadata.ci_issue_id == filtered_issues_cte.id,
        )
        .join(
            WorkflowJobEnhanced,
            sqlalchemy.and_(
                gh_models.WorkflowJobLogMetadata.workflow_job_id
                == WorkflowJobEnhanced.id,
                WorkflowJobEnhanced.repository_id == repository_id,
            ),
        )
        .outerjoin(
            gh_models.PullRequest,
            sqlalchemy.and_(
                gh_models.PullRequest.head_sha_history.any(
                    PullRequestHeadShaHistory.head_sha == WorkflowJobEnhanced.head_sha,
                ),
                gh_models.PullRequest.base_repository_id
                == WorkflowJobEnhanced.repository_id,
            ),
        )
        .group_by(
            filtered_issues_cte.id,
            filtered_issues_cte.short_id_suffix,
            filtered_issues_cte.name,
            filtered_issues_cte.status,
            WorkflowJobEnhanced.name_without_matrix,
            WorkflowJobEnhanced.workflow_run_id,
            WorkflowJobEnhanced.started_at,
            WorkflowJobEnhanced.flaky,
            WorkflowJobEnhanced.failed_run_count,
            gh_models.WorkflowJobLogMetadata.id,
            gh_models.GitHubRepository.name,
        )
        .order_by(WorkflowJobEnhanced.started_at.desc())
    )

    # NOTE(Kontrolix): We use an OrderedDict here to be sure to keep the sorting of
    # the SQL query
    responses: OrderedDict[int, CiIssueResponse] = OrderedDict()
    pr_linked_to_ci_issues: OrderedDict[int, set[int]] = OrderedDict()
    for result in await session.execute(stmt):
        if result.id in responses:
            response = responses[result.id]
            pr_linked_to_ci_issues[result.id].update(result.pull_requests)
        else:
            response = CiIssueResponse(
                id=result.id,
                short_id=result.short_id,
                name=result.name or f"Failure of {result.job_name}",
                job_name=result.job_name,
                status=result.status,
                events=[],
            )
            responses[result.id] = response
            pr_linked_to_ci_issues[result.id] = set(result.pull_requests)

        response.events.append(
            CiIssueEvent(
                id=result.event_id,
                run_id=result.workflow_run_id,
                started_at=github_types.ISODateTimeType(result.started_at.isoformat()),
                flaky=result.flaky,
                failed_run_count=result.failed_run_count,
            ),
        )

    if issue_id is not None and len(responses) > 1:
        raise RuntimeError("This should never happens")

    # NOTE(Kontrolix): Filter out ci_issue linked to only one PR
    return [
        responses[ci_issue_id]
        for ci_issue_id, prs_linked in pr_linked_to_ci_issues.items()
        if len(prs_linked) != 1
    ]


@router.get(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Get CI issues",
    description="Get CI issues",
    response_model=CiIssuesResponse,
    include_in_schema=False,
    responses=api.default_responses,
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
    responses=api.default_responses,
)
async def patch_ci_issues(
    session: database.Session,
    _repository_ctxt: security.Repository,
    ci_issue_ids: typing.Annotated[
        list[int],
        fastapi.Query(description="IDs of CI Issues", default_factory=list, alias="id"),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssueGPT)
        .values(status=json.status)
        .where(CiIssueGPT.id.in_(ci_issue_ids))
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
    stmt = (
        sqlalchemy.select(CiIssueGPT)
        .options(
            sqlalchemy.orm.joinedload(CiIssueGPT.log_metadata)
            .load_only(gh_models.WorkflowJobLogMetadata.id)
            .options(
                sqlalchemy.orm.joinedload(
                    gh_models.WorkflowJobLogMetadata.workflow_job,
                )
                .load_only(
                    # NOTE(sileht): Only what needed as some columns are quite big
                    gh_models.WorkflowJob.id,
                    gh_models.WorkflowJob.started_at,
                    gh_models.WorkflowJob.workflow_run_id,
                    gh_models.WorkflowJob.name_without_matrix,
                    gh_models.WorkflowJob.matrix,
                )
                .options(
                    gh_models.WorkflowJob.with_flaky_column(),
                    gh_models.WorkflowJob.with_failed_run_count_column(),
                ),
            ),
        )
        .where(
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
            CiIssueGPT.id == ci_issue_id,
        )
    )

    result = await session.execute(stmt)
    ci_issue = result.unique().scalar_one_or_none()
    if ci_issue is None:
        raise fastapi.HTTPException(404)

    events = []
    for log_metadata in ci_issue.log_metadata:
        job = log_metadata.workflow_job
        events.append(
            CiIssueEvent(
                id=log_metadata.id,
                run_id=job.workflow_run_id,
                started_at=github_types.ISODateTimeType(
                    job.started_at.isoformat(),
                ),
                flaky=FlakyStatus(job.flaky),
                failed_run_count=job.failed_run_count,
            ),
        )

    return CiIssueResponse(
        id=ci_issue.id,
        short_id=ci_issue.short_id,
        name=ci_issue.name or "<unknown>",
        job_name=ci_issue.log_metadata[0].workflow_job.name_without_matrix,
        status=ci_issue.status,
        # TODO(sileht): make the order by with ORM
        events=sorted(events, key=lambda e: e.started_at, reverse=True),
    )


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
    _repository_ctxt: security.Repository,
    ci_issue_id: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue"),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssueGPT)
        .values(status=json.status)
        .where(CiIssueGPT.id == ci_issue_id)
        .returning(CiIssueGPT.id)
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
    stmt = (
        sqlalchemy.select(gh_models.WorkflowJobLogMetadata)
        .join(gh_models.WorkflowJobLogMetadata.workflow_job)
        .options(
            sqlalchemy.orm.joinedload(
                gh_models.WorkflowJobLogMetadata.workflow_job,
            ).options(
                gh_models.WorkflowJob.with_flaky_column(),
                gh_models.WorkflowJob.with_failed_run_count_column(),
            ),
        )
        .where(
            gh_models.WorkflowJobLogMetadata.id == event_id,
            # NOTE(sileht): For security purpose:
            gh_models.WorkflowJob.repository_id == repository_ctxt.repo["id"],
            gh_models.WorkflowJobLogMetadata.ci_issue_id == ci_issue_id,
        )
    )

    result = await session.execute(stmt)
    log_metadata = result.unique().scalar_one_or_none()

    if log_metadata is None:
        raise fastapi.HTTPException(404)

    return CiIssueEventDetailResponse(
        id=log_metadata.id,
        run_id=log_metadata.workflow_job.workflow_run_id,
        steps=log_metadata.workflow_job.steps or [],
        failed_step_number=log_metadata.workflow_job.failed_step_number,
        completed_at=github_types.ISODateTimeType(
            log_metadata.workflow_job.completed_at.isoformat(),
        ),
        started_at=github_types.ISODateTimeType(
            log_metadata.workflow_job.started_at.isoformat(),
        ),
        flaky=FlakyStatus(log_metadata.workflow_job.flaky),
        failed_run_count=log_metadata.workflow_job.failed_run_count,
        log_extract=log_metadata.workflow_job.log_extract or "",
        run_attempt=log_metadata.workflow_job.run_attempt,
        name=log_metadata.workflow_job.name_without_matrix,
    )
