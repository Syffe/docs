import dataclasses
import typing

import daiquiri
import fastapi
import pydantic
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssueGPT
from mergify_engine.models.ci_issue import CiIssueStatus
from mergify_engine.models.github.workflows import FlakyStatus
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
    stmt = (
        sqlalchemy.select(CiIssueGPT)
        .options(
            sqlalchemy.orm.joinedload(CiIssueGPT.log_metadata)
            .load_only(gh_models.WorkflowJobLogMetadata.id)
            .options(
                sqlalchemy.orm.joinedload(
                    gh_models.WorkflowJobLogMetadata.workflow_job.of_type(
                        gh_models.WorkflowJob,
                    ),
                )
                .load_only(
                    # NOTE(sileht): Only what needed as some columns are quite big
                    gh_models.WorkflowJob.id,
                    gh_models.WorkflowJob.started_at,
                    gh_models.WorkflowJob.workflow_run_id,
                    gh_models.WorkflowJob.name_without_matrix,
                    gh_models.WorkflowJob.matrix,
                    # NOTE(sileht): required of pull_requests
                    # WorkflowsJobForEvents.repository_id,
                    gh_models.WorkflowJob.head_sha,
                )
                .options(
                    gh_models.WorkflowJob.with_pull_requests_column(),
                    gh_models.WorkflowJob.with_flaky_column(),
                    gh_models.WorkflowJob.with_failed_run_count_column(),
                ),
            ),
        )
        .where(
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
            CiIssueGPT.log_metadata.any(),
        )
    )

    if status is not None:
        stmt = stmt.where(CiIssueGPT.status.in_(status))

    if page.cursor.value:
        try:
            cursor_issue_id = int(page.cursor.value)
        except ValueError:
            raise pagination.InvalidCursor(page.cursor)

        if page.cursor.forward:
            stmt = stmt.where(CiIssueGPT.id > cursor_issue_id)
        else:
            stmt = stmt.where(CiIssueGPT.id < cursor_issue_id)

    # FIXME(sileht): default order should be the most recent issue first
    stmt = stmt.order_by(
        CiIssueGPT.id.asc() if page.cursor.forward else CiIssueGPT.id.desc(),
    ).limit(page.per_page)

    result = await session.execute(stmt)
    issues = []
    for ci_issue in result.unique().scalars():
        events = []
        pull_requests = set()
        for log_metadata in ci_issue.log_metadata:
            job = log_metadata.workflow_job
            if job.pull_requests:
                pull_requests.update(job.pull_requests)
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

        # FIXME(sileht): This should be filterout in the SQL query, otherwise the pagination is wrong
        if len(pull_requests) == 1:
            continue

        issues.append(
            CiIssueResponse(
                id=ci_issue.id,
                short_id=ci_issue.short_id,
                name=ci_issue.name or "<unknown>",
                job_name=ci_issue.log_metadata[0].workflow_job.name_without_matrix,
                status=ci_issue.status,
                # TODO(sileht): make the order by with ORM
                events=sorted(events, key=lambda e: e.started_at, reverse=True),
            ),
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
