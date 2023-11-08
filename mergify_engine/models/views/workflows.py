import enum
import typing

import sqlalchemy
import sqlalchemy.orm as orm

from mergify_engine.models import github as gh_models


class FlakyStatus(enum.Enum):
    FLAKY = "flaky"
    UNKNOWN = "unknown"


class WorkflowJobEnhanced(gh_models.WorkflowJob):
    cte_run_info = (
        sqlalchemy.select(
            gh_models.WorkflowJob.workflow_run_id,
            gh_models.WorkflowJob.name_without_matrix,
            gh_models.WorkflowJob.matrix,
            gh_models.WorkflowJob.repository_id,
            sqlalchemy.case(
                (
                    sqlalchemy.and_(
                        sqlalchemy.func.bool_or(
                            gh_models.WorkflowJob.conclusion
                            == gh_models.WorkflowJobConclusion.SUCCESS
                        ),
                        sqlalchemy.func.bool_or(
                            gh_models.WorkflowJob.conclusion
                            == gh_models.WorkflowJobConclusion.FAILURE
                        ),
                    ),
                    FlakyStatus.FLAKY.value,
                ),
                else_=FlakyStatus.UNKNOWN.value,
            ).label("flaky"),
            sqlalchemy.func.sum(
                sqlalchemy.case(
                    (
                        gh_models.WorkflowJob.conclusion
                        == gh_models.WorkflowJobConclusion.FAILURE,
                        1,
                    ),
                    else_=0,
                )
            ).label("failed_run_count"),
        )
        .group_by(
            gh_models.WorkflowJob.workflow_run_id,
            gh_models.WorkflowJob.name_without_matrix,
            gh_models.WorkflowJob.matrix,
            gh_models.WorkflowJob.repository_id,
        )
        .cte("job_run_info_aggregate")
    )

    __table__ = (
        sqlalchemy.select(gh_models.WorkflowJob)
        .add_columns(
            cte_run_info.c.flaky,
            cte_run_info.c.failed_run_count,
        )
        .outerjoin(
            cte_run_info,
            sqlalchemy.and_(
                cte_run_info.c.workflow_run_id == gh_models.WorkflowJob.workflow_run_id,
                cte_run_info.c.name_without_matrix
                == gh_models.WorkflowJob.name_without_matrix,
                cte_run_info.c.repository_id == gh_models.WorkflowJob.repository_id,
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        cte_run_info.c.matrix.is_(None),
                        gh_models.WorkflowJob.matrix.is_(None),
                    ),
                    cte_run_info.c.matrix == gh_models.WorkflowJob.matrix,
                ),
            ),
        )
        .cte("workflow_job_enhanced")
    )

    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore
        "inherit_condition": gh_models.WorkflowJob.id == __table__.c.id
    }

    flaky: orm.Mapped[FlakyStatus]
    failed_run_count: orm.Mapped[int]
