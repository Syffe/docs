from collections import Counter
from collections import abc
import datetime

from mergify_engine import github_types
from mergify_engine.ci import models
from mergify_engine.ci import reports
from mergify_engine.tests.unit.ci import utils


class FakeJobRegistry:
    def __init__(self) -> None:
        self._job_run_timing = 10_000
        self._pull_id = 1
        self._positions: Counter[int] = Counter()

    async def search(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> abc.AsyncIterator[models.JobRun]:
        # Pull request, initial push
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="pull_request",
            triggering_actor="someone",
            pull_id=self._pull_id,
            run_attempt=1,
        )
        # Pull request, update
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="pull_request",
            triggering_actor="somebody",
            pull_id=self._pull_id,
            run_attempt=1,
        )
        # Pull request, manual retry
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="pull_request",
            triggering_actor="somebody",
            pull_id=self._pull_id,
            run_attempt=2,
        )
        # Deployment
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="push",
            pull_id=self._pull_id,
        )
        # Scheduled job
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="schedule",
            pull_id=self._pull_id,
        )

        # Increment timing delta to have a difference
        self._job_run_timing += 1000
        # Change pull request ID, to prevent side effects on the lifecycle
        self._pull_id += 1

    def get_job_running_order(self, pull_id: int, job_run: models.JobRun) -> int:
        self._positions[pull_id] += 1
        return self._positions[pull_id]


async def test_category_report() -> None:
    report = reports.CategoryReport(
        FakeJobRegistry(),
        reports.CategoryQuery(
            owner=github_types.GitHubLogin("mergifyio"),
            repository=github_types.GitHubRepositoryName("engine"),
            start_at=datetime.date(2023, 2, 1),
            end_at=datetime.date(2023, 2, 1),
        ),
    )
    result = await report.run()

    assert result.total_costs == reports.Money.from_decimal("6.68")
    assert result.total_difference == reports.Money.from_decimal("-0.680")

    deployments = result.categories.deployments
    assert deployments == reports.Category(
        type="deployments",
        total_cost=reports.Money.from_decimal("1.336"),
        difference=reports.Money.from_decimal("-0.136"),
        dimensions=reports.Dimensions(
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="engine",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
        ),
    )
    scheduled_jobs = result.categories.scheduled_jobs
    assert scheduled_jobs == reports.Category(
        type="scheduled_jobs",
        total_cost=reports.Money.from_decimal("1.336"),
        difference=reports.Money.from_decimal("-0.136"),
        dimensions=reports.Dimensions(
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="engine",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
        ),
    )
    pull_requests = result.categories.pull_requests
    assert pull_requests == reports.Category(
        type="pull_requests",
        total_cost=reports.Money.from_decimal("4.008"),
        difference=reports.Money.from_decimal("-0.408"),
        dimensions=reports.Dimensions(
            actors=reports.Dimension(
                type="actors",
                items=[
                    reports.DimensionItem(
                        name="someone",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="somebody",
                        cost=reports.Money.from_decimal("2.672"),
                        difference=reports.Money.from_decimal("-0.272"),
                    ),
                ],
            ),
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
            lifecycles=reports.Dimension(
                type="lifecycles",
                items=[
                    reports.DimensionItem(
                        name="Initial push",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="Update",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="Manual retry",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="engine",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
        ),
    )


async def test_category_report_for_all_repos() -> None:
    report = reports.CategoryReport(
        FakeJobRegistry(),
        reports.CategoryQuery(
            owner=github_types.GitHubLogin("mergifyio"),
            start_at=datetime.date(2023, 2, 1),
            end_at=datetime.date(2023, 2, 1),
        ),
    )
    result = await report.run()

    assert result.total_costs == reports.Money.from_decimal("6.68")
    assert result.total_difference == reports.Money.from_decimal("-0.680")

    deployments = result.categories.deployments
    assert deployments == reports.Category(
        type="deployments",
        total_cost=reports.Money.from_decimal("1.336"),
        difference=reports.Money.from_decimal("-0.136"),
        dimensions=reports.Dimensions(
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="fake_repo",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
        ),
    )
    scheduled_jobs = result.categories.scheduled_jobs
    assert scheduled_jobs == reports.Category(
        type="scheduled_jobs",
        total_cost=reports.Money.from_decimal("1.336"),
        difference=reports.Money.from_decimal("-0.136"),
        dimensions=reports.Dimensions(
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="fake_repo",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    )
                ],
            ),
        ),
    )
    pull_requests = result.categories.pull_requests
    assert pull_requests == reports.Category(
        type="pull_requests",
        total_cost=reports.Money.from_decimal("4.008"),
        difference=reports.Money.from_decimal("-0.408"),
        dimensions=reports.Dimensions(
            actors=reports.Dimension(
                type="actors",
                items=[
                    reports.DimensionItem(
                        name="someone",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="somebody",
                        cost=reports.Money.from_decimal("2.672"),
                        difference=reports.Money.from_decimal("-0.272"),
                    ),
                ],
            ),
            jobs=reports.Dimension(
                type="jobs",
                items=[
                    reports.DimensionItem(
                        name="hello",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
            lifecycles=reports.Dimension(
                type="lifecycles",
                items=[
                    reports.DimensionItem(
                        name="Initial push",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="Update",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                    reports.DimensionItem(
                        name="Manual retry",
                        cost=reports.Money.from_decimal("1.336"),
                        difference=reports.Money.from_decimal("-0.136"),
                    ),
                ],
            ),
            conclusions=reports.Dimension(
                type="conclusions",
                items=[
                    reports.DimensionItem(
                        name="success",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
            repositories=reports.Dimension(
                type="repositories",
                items=[
                    reports.DimensionItem(
                        name="fake_repo",
                        cost=reports.Money.from_decimal("4.008"),
                        difference=reports.Money.from_decimal("-0.408"),
                    )
                ],
            ),
        ),
    )


def test_category_query_compute_date_range() -> None:
    query = reports.CategoryQuery(
        github_types.GitHubLogin("some-owner"),
        github_types.GitHubRepositoryName("some-repo"),
        datetime.date(2023, 2, 5),
        datetime.date(2023, 2, 8),
    )

    assert query.compare_start_at == datetime.date(2023, 2, 1)
    assert query.compare_end_at == datetime.date(2023, 2, 4)
