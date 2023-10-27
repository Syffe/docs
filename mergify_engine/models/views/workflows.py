import enum
import typing

import sqlalchemy
import sqlalchemy.orm as orm

from mergify_engine.models import github as gh_models


class FlakyStatus(enum.Enum):
    FLAKY = "flaky"
    UNKNOWN = "unknown"


class WorkflowJobEnhanced(gh_models.WorkflowJob):
    jobs = orm.aliased(gh_models.WorkflowJob, name="jobs")

    __table__ = (
        sqlalchemy.select(gh_models.WorkflowJob)
        .add_columns(
            sqlalchemy.case(
                (
                    sqlalchemy.and_(
                        sqlalchemy.func.bool_or(
                            jobs.conclusion == gh_models.WorkflowJobConclusion.SUCCESS
                        ),
                        sqlalchemy.func.bool_or(
                            jobs.conclusion == gh_models.WorkflowJobConclusion.FAILURE
                        ),
                    ),
                    FlakyStatus.FLAKY.value,
                ),
                else_=FlakyStatus.UNKNOWN.value,
            ).label("flaky"),
            sqlalchemy.func.sum(
                sqlalchemy.case(
                    (jobs.conclusion == gh_models.WorkflowJobConclusion.FAILURE, 1),
                    else_=0,
                )
            ).label("failed_run_count"),
        )
        .join(
            jobs,
            sqlalchemy.and_(
                jobs.workflow_run_id == gh_models.WorkflowJob.workflow_run_id,
                jobs.github_name == gh_models.WorkflowJob.github_name,
                jobs.repository_id == gh_models.WorkflowJob.repository_id,
            ),
            isouter=True,
        )
        .group_by(gh_models.WorkflowJob.id)
        .subquery()
    )

    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {  # type: ignore
        "inherit_condition": gh_models.WorkflowJob.id == __table__.c.id
    }

    flaky: orm.Mapped[FlakyStatus]
    failed_run_count: orm.Mapped[int]
