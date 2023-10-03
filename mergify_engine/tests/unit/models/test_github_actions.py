import pytest

from mergify_engine.models import github as gh_models


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
