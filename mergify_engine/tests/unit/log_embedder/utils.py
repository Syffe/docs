import datetime
import typing

import sqlalchemy.ext.asyncio

from mergify_engine.models import github_actions


async def add_job(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job_data: dict[str, typing.Any],
) -> github_actions.WorkflowJob:
    job = github_actions.WorkflowJob(
        id=job_data["id"],
        repository=job_data["repository"],
        log_embedding=job_data.get("log_embedding"),
        workflow_run_id=job_data.get("workflow_run_id", 1),
        name=job_data.get("name", "job_name"),
        started_at=job_data.get("started_at", datetime.datetime.now()),
        completed_at=job_data.get("completed_at", datetime.datetime.now()),
        conclusion=job_data.get(
            "conclusion", github_actions.WorkflowJobConclusion.SUCCESS
        ),
        labels=job_data.get("labels", []),
        run_attempt=job_data.get("run_attempt", 1),
    )
    session.add(job)
    return job


async def get_cosine_similarity_for_job(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job: github_actions.WorkflowJob,
) -> typing.Sequence[github_actions.WorkflowJobLogNeighbours]:
    return (
        await session.scalars(
            sqlalchemy.select(github_actions.WorkflowJobLogNeighbours)
            .where(github_actions.WorkflowJobLogNeighbours.job_id == job.id)
            .order_by(github_actions.WorkflowJobLogNeighbours.neighbour_job_id)
        )
    ).all()
