from collections import abc
import datetime

from mergify_engine import github_types
from mergify_engine.ci import cost_calculator
from mergify_engine.ci import models
from mergify_engine.ci import reports
from mergify_engine.tests.unit.ci import utils


class FakePullRegistry:
    def __init__(self) -> None:
        self._position = -1

    async def get_job_run_position(self, pull_id: int, job_run: models.JobRun) -> int:
        self._position += 1
        return self._position


class FakeJobRegistry:
    def __init__(self) -> None:
        self._job_run_timing = 10_000

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
            run_attempt=1,
        )
        # Pull request, update
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="pull_request",
            triggering_actor="somebody",
            run_attempt=1,
        )
        # Pull request, manual retry
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="pull_request",
            triggering_actor="somebody",
            run_attempt=2,
        )
        # Deployment
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="push",
        )
        # Scheduled job
        yield utils.create_job(
            owner=owner,
            repository=repository or "fake_repo",
            timing=self._job_run_timing,
            triggering_event="schedule",
        )

        # Increment timing delta to have a difference
        self._job_run_timing += 1000


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

    assert result["total_costs"] == cost_calculator.Money("6.68")
    assert result["total_difference"] == cost_calculator.Money("-0.680")

    deployments = result["categories"]["deployments"]
    assert deployments == {
        "type": "deployments",
        "total_cost": cost_calculator.Money("1.336"),
        "difference": cost_calculator.Money("-0.136"),
        "dimensions": {
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("1.336")}],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("1.336")}],
            },
        },
    }
    scheduled_jobs = result["categories"]["scheduled_jobs"]
    assert scheduled_jobs == {
        "type": "scheduled_jobs",
        "total_cost": cost_calculator.Money("1.336"),
        "difference": cost_calculator.Money("-0.136"),
        "dimensions": {
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("1.336")}],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("1.336")}],
            },
        },
    }
    pull_requests = result["categories"]["pull_requests"]
    assert pull_requests == {
        "type": "pull_requests",
        "total_cost": cost_calculator.Money("4.008"),
        "difference": cost_calculator.Money("-0.408"),
        "dimensions": {
            "actors": {
                "type": "actors",
                "items": [
                    {"name": "someone", "cost": cost_calculator.Money("1.336")},
                    {"name": "somebody", "cost": cost_calculator.Money("2.672")},
                ],
            },
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("4.008")}],
            },
            "lifecycles": {
                "type": "lifecycles",
                "items": [
                    {
                        "name": "Initial push",
                        "cost": cost_calculator.Money("1.336"),
                    },
                    {"name": "Update", "cost": cost_calculator.Money("1.336")},
                    {
                        "name": "Manual retry",
                        "cost": cost_calculator.Money("1.336"),
                    },
                ],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("4.008")}],
            },
        },
    }


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

    assert result["total_costs"] == cost_calculator.Money("6.68")
    assert result["total_difference"] == cost_calculator.Money("-0.680")

    deployments = result["categories"]["deployments"]
    assert deployments == {
        "type": "deployments",
        "total_cost": cost_calculator.Money("1.336"),
        "difference": cost_calculator.Money("-0.136"),
        "dimensions": {
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("1.336")}],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("1.336")}],
            },
        },
    }
    scheduled_jobs = result["categories"]["scheduled_jobs"]
    assert scheduled_jobs == {
        "type": "scheduled_jobs",
        "total_cost": cost_calculator.Money("1.336"),
        "difference": cost_calculator.Money("-0.136"),
        "dimensions": {
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("1.336")}],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("1.336")}],
            },
        },
    }
    pull_requests = result["categories"]["pull_requests"]
    assert pull_requests == {
        "type": "pull_requests",
        "total_cost": cost_calculator.Money("4.008"),
        "difference": cost_calculator.Money("-0.408"),
        "dimensions": {
            "actors": {
                "type": "actors",
                "items": [
                    {"name": "someone", "cost": cost_calculator.Money("1.336")},
                    {"name": "somebody", "cost": cost_calculator.Money("2.672")},
                ],
            },
            "jobs": {
                "type": "jobs",
                "items": [{"name": "hello", "cost": cost_calculator.Money("4.008")}],
            },
            "lifecycles": {
                "type": "lifecycles",
                "items": [
                    {
                        "name": "Initial push",
                        "cost": cost_calculator.Money("1.336"),
                    },
                    {"name": "Update", "cost": cost_calculator.Money("1.336")},
                    {
                        "name": "Manual retry",
                        "cost": cost_calculator.Money("1.336"),
                    },
                ],
            },
            "conclusions": {
                "type": "conclusions",
                "items": [{"name": "success", "cost": cost_calculator.Money("4.008")}],
            },
        },
    }


def test_get_previous_date_range() -> None:
    start_at, end_at = datetime.date(2023, 2, 5), datetime.date(2023, 2, 8)
    (
        previous_start_at,
        previous_end_at,
    ) = reports.Report._get_previous_date_range(start_at, end_at)

    assert previous_start_at == datetime.date(2023, 2, 1)
    assert previous_end_at == datetime.date(2023, 2, 4)
