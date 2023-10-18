import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine.flaky_check.utils import NeedRerunStatus
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


@pytest.mark.parametrize(
    "input_name,expected_name,expected_matrix",
    [
        ("job-name", "job-name", None),
        ("job-name (lvl1)", "job-name", "lvl1"),
        ("job-name (lvl1, lvl2)", "job-name", "lvl1, lvl2"),
        ("job-name (lv(,l1, l,)()vl2)", "job-name", "lv(,l1, l,)()vl2"),
    ],
)
async def test_get_job_name_and_matrix(
    input_name: str,
    expected_name: str,
    expected_matrix: str | None,
) -> None:
    assert gh_models.WorkflowJob.get_job_name_and_matrix(input_name) == (
        expected_name,
        expected_matrix,
    )


@pytest.mark.parametrize(
    "job_ref, max_rerun, first_rerun_needed, second_rerun_needed, final_rerun_needed",
    [
        (
            "failed_job_with_flaky_nghb",
            3,
            NeedRerunStatus.NEED_RERUN,
            NeedRerunStatus.NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
        ),
        (
            "failed_job_with_flaky_nghb",
            2,
            NeedRerunStatus.NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
        ),
        (
            "failed_job_with_no_flaky_nghb",
            3,
            NeedRerunStatus.NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
        ),
        (
            "failed_job_uncomputed",
            3,
            NeedRerunStatus.UNKONWN,
            NeedRerunStatus.UNKONWN,
            NeedRerunStatus.UNKONWN,
        ),
        (
            "successful_flaky_job",
            3,
            NeedRerunStatus.DONT_NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
            NeedRerunStatus.DONT_NEED_RERUN,
        ),
    ],
)
@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_is_rerun_needed(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    job_ref: str,
    max_rerun: int,
    first_rerun_needed: NeedRerunStatus,
    second_rerun_needed: NeedRerunStatus,
    final_rerun_needed: NeedRerunStatus,
) -> None:
    # TODO: To remove before commit
    await populated_db.commit()

    job = (
        await populated_db.execute(
            sqlalchemy.select(gh_models.WorkflowJob).where(
                gh_models.WorkflowJob.id == DbPopulator.internal_ref[job_ref]
            )
        )
    ).scalar_one()

    job.run_attempt = 1

    assert (
        await gh_models.WorkflowJob.is_rerun_needed(populated_db, job.id, max_rerun)
    ) is first_rerun_needed

    job.run_attempt = 2

    assert (
        await gh_models.WorkflowJob.is_rerun_needed(populated_db, job.id, max_rerun)
    ) is second_rerun_needed

    job.run_attempt = max_rerun

    assert (
        await gh_models.WorkflowJob.is_rerun_needed(populated_db, job.id, max_rerun)
    ) is final_rerun_needed
