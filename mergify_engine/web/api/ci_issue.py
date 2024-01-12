import dataclasses
import datetime
import typing

import daiquiri
import fastapi
import pydantic
import sqlalchemy
import sqlalchemy.orm

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import pagination
from mergify_engine import settings
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssueGPT
from mergify_engine.models.ci_issue import CiIssueStatus
from mergify_engine.web import api
from mergify_engine.web.api import security


LOG = daiquiri.getLogger(__name__)

FlakyT = typing.Literal["flaky", "unknown"]


async def has_log_embedder_enabled(
    repository_ctxt: security.Repository,
) -> None:
    if repository_ctxt.installation.owner_login in settings.LOG_EMBEDDER_ENABLED_ORGS:
        return
    raise fastapi.HTTPException(403)


router = fastapi.APIRouter(
    tags=["ci_issues"],
    dependencies=[fastapi.Security(has_log_embedder_enabled)],
)


@pydantic.dataclasses.dataclass
class CiIssueEventDeprecated:
    id: int
    run_id: int
    started_at: datetime.datetime
    flaky: typing.Literal["flaky", "unknown"]
    failed_run_count: int


@pydantic.dataclasses.dataclass
class CiIssuePullRequestData:
    number: int
    title: str
    author: str
    events_count: int


@pydantic.dataclasses.dataclass
class CiIssueDetailResponse:
    id: int
    short_id: str
    name: str
    job_name: str
    events_count: int
    status: CiIssueStatus
    events: list[CiIssueEventDeprecated] = dataclasses.field(
        metadata={"description": "List of CI issue events"},
    )
    pull_requests_impacted: list[CiIssuePullRequestData] = dataclasses.field(
        metadata={"description": "List of pull requests impacted by this CiIssue"},
    )
    flaky: FlakyT
    first_seen: datetime.datetime
    last_seen: datetime.datetime
    first_event_id: int


@pydantic.dataclasses.dataclass
class CiIssueListResponse:
    id: int
    short_id: str
    name: str
    job_name: str
    status: CiIssueStatus
    events_count: int
    events: list[CiIssueEventDeprecated] = dataclasses.field(
        metadata={"description": "List of CI issue events"},
    )
    flaky: FlakyT
    first_event_id: int
    first_seen: datetime.datetime
    last_seen: datetime.datetime
    pull_requests_count: int


class CiIssuesListResponse(pagination.PageResponse[CiIssueListResponse]):
    items_key: typing.ClassVar[str] = "issues"
    issues: list[CiIssueListResponse] = dataclasses.field(
        metadata={"description": "List of CI issues"},
    )


class CiIssueBody(pydantic.BaseModel):
    status: CiIssueStatus


@router.get(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Get CI issues",
    description="Get CI issues",
    response_model=CiIssuesListResponse,
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
) -> CiIssuesListResponse:
    stmt = (
        sqlalchemy.select(CiIssueGPT)
        .options(
            sqlalchemy.orm.undefer(CiIssueGPT.events_count),
            CiIssueGPT.with_flaky_column(),
            CiIssueGPT.with_first_event_id_column(),
            CiIssueGPT.with_first_seen_column(),
            CiIssueGPT.with_last_seen_column(),
            CiIssueGPT.with_pull_requests_count_column(),
            CiIssueGPT.with_job_name_column(),
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
            CiIssueGPT.pull_requests_count_subquery() != 1,
        )
    )

    if status is not None:
        stmt = stmt.where(CiIssueGPT.status.in_(status))

    if page.cursor.value:
        try:
            cursor_issue_id = int(page.cursor.value)
        except ValueError:
            raise pagination.InvalidCursorError(page.cursor)

        if page.cursor.forward:
            stmt = stmt.where(CiIssueGPT.short_id_suffix > cursor_issue_id)
        else:
            stmt = stmt.where(CiIssueGPT.short_id_suffix < cursor_issue_id)

    # FIXME(sileht): default order should be the most recent issue first
    stmt = stmt.order_by(
        CiIssueGPT.short_id_suffix.asc()
        if page.cursor.forward
        else CiIssueGPT.short_id_suffix.desc(),
    ).limit(page.per_page)

    result = await session.execute(stmt)
    issues = []
    for ci_issue in result.unique().scalars():
        events = []
        for log_metadata in ci_issue.log_metadata:
            job = log_metadata.workflow_job
            events.append(
                CiIssueEventDeprecated(
                    id=log_metadata.id,
                    run_id=job.workflow_run_id,
                    started_at=job.started_at,
                    flaky=job.flaky,
                    failed_run_count=job.failed_run_count,
                ),
            )

        issues.append(
            CiIssueListResponse(
                id=ci_issue.short_id_suffix,
                short_id=ci_issue.short_id,
                name=ci_issue.name or "<unknown>",
                job_name=ci_issue.job_name,
                status=ci_issue.status,
                events_count=ci_issue.events_count,
                # TODO(sileht): make the order by with ORM
                events=sorted(events, key=lambda e: e.started_at, reverse=True),
                flaky=ci_issue.flaky,
                first_event_id=ci_issue.first_event_id,
                first_seen=ci_issue.first_seen,
                last_seen=ci_issue.last_seen,
                pull_requests_count=ci_issue.pull_requests_count,
            ),
        )

    first_issue_id = str(issues[0].id) if issues else None
    last_issue_id = str(issues[-1].id) if issues else None

    page_response: pagination.Page[CiIssueListResponse] = pagination.Page(
        items=issues,
        current=page,
        cursor_prev=page.cursor.previous(first_issue_id, last_issue_id),
        cursor_next=page.cursor.next(first_issue_id, last_issue_id),
    )
    return CiIssuesListResponse(page=page_response)  # type: ignore[call-arg]


@router.patch(
    "/repos/{owner}/{repository}/ci_issues",
    summary="Partially update CI issues",
    description="Update some properties of several CI issues",
    include_in_schema=False,
    responses=api.default_responses,
)
async def patch_ci_issues(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_short_ids_suffixes: typing.Annotated[
        list[int],
        fastapi.Query(
            description="IDs of CI Issues in this repository",
            default_factory=list,
            alias="id",
        ),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssueGPT)
        .values(status=json.status)
        .where(
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
            CiIssueGPT.short_id_suffix.in_(ci_issue_short_ids_suffixes),
        )
    )

    await session.execute(stmt)
    await session.commit()


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_short_id_suffix}",
    summary="Get a CI issue",
    description="Get a CI issue",
    response_model=CiIssueDetailResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "CI issue not found"},
    },
)
async def get_ci_issue(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_short_id_suffix: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue in this repository"),
    ],
) -> CiIssueDetailResponse:
    stmt = (
        sqlalchemy.select(CiIssueGPT)
        .options(
            sqlalchemy.orm.undefer(CiIssueGPT.events_count),
            CiIssueGPT.with_flaky_column(),
            CiIssueGPT.with_first_event_id_column(),
            CiIssueGPT.with_first_seen_column(),
            CiIssueGPT.with_last_seen_column(),
            CiIssueGPT.with_pull_requests_impacted_column(),
            CiIssueGPT.with_job_name_column(),
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
            CiIssueGPT.short_id_suffix == ci_issue_short_id_suffix,
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
            CiIssueEventDeprecated(
                id=log_metadata.id,
                run_id=job.workflow_run_id,
                started_at=job.started_at,
                flaky=job.flaky,
                failed_run_count=job.failed_run_count,
            ),
        )

    return CiIssueDetailResponse(
        id=ci_issue.short_id_suffix,
        short_id=ci_issue.short_id,
        name=ci_issue.name or "<unknown>",
        job_name=ci_issue.job_name,
        status=ci_issue.status,
        events_count=ci_issue.events_count,
        # TODO(sileht): make the order by with ORM
        events=sorted(events, key=lambda e: e.started_at, reverse=True),
        pull_requests_impacted=[
            CiIssuePullRequestData(**pull_request_api_data)
            for pull_request_api_data in ci_issue.pull_requests_impacted
        ]
        if ci_issue.pull_requests_impacted
        else [],
        flaky=ci_issue.flaky,
        first_event_id=ci_issue.first_event_id,
        first_seen=ci_issue.first_seen,
        last_seen=ci_issue.last_seen,
    )


@router.patch(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_short_id_suffix}",
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
    ci_issue_short_id_suffix: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue in this repository"),
    ],
    json: CiIssueBody,
) -> None:
    stmt = (
        sqlalchemy.update(CiIssueGPT)
        .values(status=json.status)
        .where(
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
            CiIssueGPT.short_id_suffix == ci_issue_short_id_suffix,
        )
        .returning(CiIssueGPT.id)
    )
    result = await session.execute(stmt)
    await session.commit()

    updated_ci_issue_id = result.scalar_one_or_none()
    if updated_ci_issue_id is None:
        raise fastapi.HTTPException(404)


@pydantic.dataclasses.dataclass
class CiIssueEvent:
    id: int
    run_id: int
    started_at: datetime.datetime
    completed_at: datetime.datetime
    flaky: typing.Literal["flaky", "unknown"]
    failed_run_count: int
    failed_step_number: int | None
    name: str
    run_attempt: int
    steps: list[github_types.GitHubWorkflowJobStep]


@pydantic.dataclasses.dataclass
class CiIssueEventDetailResponse(CiIssueEvent):
    log_extract: str


class CiIssueEventsResponse(pagination.PageResponse[CiIssueEvent]):
    items_key: typing.ClassVar[str] = "events"
    events: list[CiIssueEvent] = dataclasses.field(
        metadata={"description": "List of CI issue events"},
    )


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_short_id_suffix}/events",
    summary="Get detailed events of a CI issue",
    description="Get detailed events of a CI issue",
    response_model=CiIssueEventsResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "CI issue event not found"},
    },
)
async def get_ci_issue_events(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_short_id_suffix: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue in this repository"),
    ],
    page: pagination.CurrentPage,
) -> CiIssueEventsResponse:
    sub_q_ci_issue_id = (
        sqlalchemy.select(CiIssueGPT.id)
        .where(
            CiIssueGPT.short_id_suffix == ci_issue_short_id_suffix,
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
        )
        .scalar_subquery()
    )

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
            gh_models.WorkflowJobLogMetadata.ci_issue_id == sub_q_ci_issue_id,
            # NOTE(sileht): For security purpose:
            gh_models.WorkflowJob.repository_id == repository_ctxt.repo["id"],
        )
    )

    if page.cursor.value:
        try:
            cursor_event_id = int(page.cursor.value)
        except ValueError:
            raise pagination.InvalidCursorError(page.cursor)

        if page.cursor.forward:
            stmt = stmt.where(gh_models.WorkflowJobLogMetadata.id < cursor_event_id)
        else:
            stmt = stmt.where(gh_models.WorkflowJobLogMetadata.id > cursor_event_id)

    # NOTE(sileht): As IDs are always growing, to sort them from the most recent to the
    # the oldest, we can use them.
    stmt = stmt.order_by(
        gh_models.WorkflowJobLogMetadata.id.desc()
        if page.cursor.forward
        else gh_models.WorkflowJobLogMetadata.id.asc(),
    ).limit(page.per_page)

    result = await session.execute(stmt)
    logs_metadata = result.unique().scalars()

    events = [
        CiIssueEvent(
            id=event.id,
            run_id=event.workflow_job.workflow_run_id,
            steps=event.workflow_job.steps or [],
            failed_step_number=event.workflow_job.failed_step_number,
            completed_at=event.workflow_job.completed_at,
            started_at=event.workflow_job.started_at,
            flaky=event.workflow_job.flaky,
            failed_run_count=event.workflow_job.failed_run_count,
            run_attempt=event.workflow_job.run_attempt,
            name=event.workflow_job.name_without_matrix,
        )
        for event in logs_metadata
    ]

    first_event_id = str(events[0].id) if events else None
    last_event_id = str(events[-1].id) if events else None

    page_response: pagination.Page[CiIssueEvent] = pagination.Page(
        items=events,
        current=page,
        cursor_prev=page.cursor.previous(first_event_id, last_event_id),
        cursor_next=page.cursor.next(first_event_id, last_event_id),
    )
    return CiIssueEventsResponse(page=page_response)  # type: ignore[call-arg]


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_short_id_suffix}/events/{event_id}",
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
    ci_issue_short_id_suffix: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue in this repository"),
    ],
    event_id: typing.Annotated[int, fastapi.Path(description="The ID of the Event")],
) -> CiIssueEventDetailResponse:
    sub_q_ci_issue_id = (
        sqlalchemy.select(CiIssueGPT.id)
        .where(
            CiIssueGPT.short_id_suffix == ci_issue_short_id_suffix,
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
        )
        .scalar_subquery()
    )

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
            gh_models.WorkflowJobLogMetadata.ci_issue_id == sub_q_ci_issue_id,
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
        completed_at=log_metadata.workflow_job.completed_at,
        started_at=log_metadata.workflow_job.started_at,
        flaky=log_metadata.workflow_job.flaky,
        failed_run_count=log_metadata.workflow_job.failed_run_count,
        log_extract=log_metadata.workflow_job.log_extract or "",
        run_attempt=log_metadata.workflow_job.run_attempt,
        name=log_metadata.workflow_job.name_without_matrix,
    )


@router.get(
    "/repos/{owner}/{repository}/ci_issues/{ci_issue_short_id_suffix}/events/{event_id}/log",
    summary="Get log of an event of a CI issue",
    description="Get log of a event of a CI issue",
    response_class=fastapi.responses.PlainTextResponse,
    include_in_schema=False,
    responses={
        **api.default_responses,  # type: ignore[dict-item]
        404: {"description": "CI issue event log not found"},
    },
)
async def get_ci_issue_event_log(
    session: database.Session,
    repository_ctxt: security.Repository,
    ci_issue_short_id_suffix: typing.Annotated[
        int,
        fastapi.Path(description="The ID of the CI Issue in this repository"),
    ],
    event_id: typing.Annotated[int, fastapi.Path(description="The ID of the Event")],
) -> str:
    sub_q_ci_issue_id = (
        sqlalchemy.select(CiIssueGPT.id)
        .where(
            CiIssueGPT.short_id_suffix == ci_issue_short_id_suffix,
            CiIssueGPT.repository_id == repository_ctxt.repo["id"],
        )
        .scalar_subquery()
    )

    stmt = (
        sqlalchemy.select(gh_models.WorkflowJob.log_extract)
        .select_from(gh_models.WorkflowJobLogMetadata)
        .join(gh_models.WorkflowJobLogMetadata.workflow_job)
        .where(
            gh_models.WorkflowJobLogMetadata.id == event_id,
            # NOTE(sileht): For security purpose:
            gh_models.WorkflowJob.repository_id == repository_ctxt.repo["id"],
            gh_models.WorkflowJobLogMetadata.ci_issue_id == sub_q_ci_issue_id,
        )
    )

    result = await session.execute(stmt)
    try:
        log = result.scalar_one()
    except sqlalchemy.exc.NoResultFound:
        raise fastapi.HTTPException(404)

    if log is None:
        raise RuntimeError(
            "log_extract is None on a WorkflowJob attached to a CI issue",
        )

    return log
