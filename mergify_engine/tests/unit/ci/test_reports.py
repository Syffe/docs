from collections import Counter
from collections import abc
import datetime

from mergify_engine import github_types
from mergify_engine.ci import models
from mergify_engine.ci import reports
from mergify_engine.tests.unit.ci import utils


class FakePullRegistry:
    def __init__(self) -> None:
        self._positions: Counter[int] = Counter()

    async def get_job_run_position(self, pull_id: int, job_run: models.JobRun) -> int:
        position = self._positions[pull_id]
        self._positions[pull_id] += 1
        return position


class FakeJobRegistry:
    def __init__(self) -> None:
        self._job_run_timing = 10_000
        self._pull_id = 1

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


async def test_report() -> None:
    report = reports.Report(
        FakeJobRegistry(),
        FakePullRegistry(),
        reports.Query(
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
        ),
    )


async def test_report_for_whole_owner() -> None:
    report = reports.Report(
        FakeJobRegistry(),
        FakePullRegistry(),
        reports.Query(
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
        ),
    )


def test_get_previous_date_range() -> None:
    start_at, end_at = datetime.date(2023, 2, 5), datetime.date(2023, 2, 8)
    (
        previous_start_at,
        previous_end_at,
    ) = reports.Report._get_previous_date_range(start_at, end_at)

    assert previous_start_at == datetime.date(2023, 2, 1)
    assert previous_end_at == datetime.date(2023, 2, 4)
