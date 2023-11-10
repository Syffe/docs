import enum

import sqlalchemy
import sqlalchemy.orm as orm

from mergify_engine import models
from mergify_engine.models.github import workflows as wf


class FlakyStatus(enum.Enum):
    FLAKY = "flaky"
    UNKNOWN = "unknown"


class WorkflowJobEnhanced(models.Base, wf.WorkflowJobColumnMixin):
    cte_run_info = (
        sqlalchemy.select(
            wf.WorkflowJob.workflow_run_id,
            wf.WorkflowJob.name_without_matrix,
            wf.WorkflowJob.matrix,
            wf.WorkflowJob.repository_id,
            sqlalchemy.case(
                (
                    sqlalchemy.and_(
                        sqlalchemy.func.bool_or(
                            wf.WorkflowJob.conclusion
                            == wf.WorkflowJobConclusion.SUCCESS
                        ),
                        sqlalchemy.func.bool_or(
                            wf.WorkflowJob.conclusion
                            == wf.WorkflowJobConclusion.FAILURE
                        ),
                    ),
                    FlakyStatus.FLAKY.value,
                ),
                else_=FlakyStatus.UNKNOWN.value,
            ).label("flaky"),
            sqlalchemy.func.sum(
                sqlalchemy.case(
                    (
                        wf.WorkflowJob.conclusion == wf.WorkflowJobConclusion.FAILURE,
                        1,
                    ),
                    else_=0,
                )
            ).label("failed_run_count"),
        )
        .group_by(
            wf.WorkflowJob.workflow_run_id,
            wf.WorkflowJob.name_without_matrix,
            wf.WorkflowJob.matrix,
            wf.WorkflowJob.repository_id,
        )
        .cte("job_run_info_aggregate")
    )

    __table__ = (
        sqlalchemy.select(wf.WorkflowJob)
        .add_columns(
            cte_run_info.c.flaky,
            cte_run_info.c.failed_run_count,
        )
        .outerjoin(
            cte_run_info,
            sqlalchemy.and_(
                cte_run_info.c.workflow_run_id == wf.WorkflowJob.workflow_run_id,
                cte_run_info.c.name_without_matrix
                == wf.WorkflowJob.name_without_matrix,
                cte_run_info.c.repository_id == wf.WorkflowJob.repository_id,
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        cte_run_info.c.matrix.is_(None),
                        wf.WorkflowJob.matrix.is_(None),
                    ),
                    cte_run_info.c.matrix == wf.WorkflowJob.matrix,
                ),
            ),
        )
        .cte("workflow_job_enhanced")
    )

    flaky: orm.Mapped[FlakyStatus]
    failed_run_count: orm.Mapped[int]
