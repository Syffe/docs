import collections
import dataclasses
import datetime
import typing

from mergify_engine import github_types
from mergify_engine.ci import cost_calculator
from mergify_engine.ci import job_registries
from mergify_engine.ci import models
from mergify_engine.ci import pull_registries


class DimensionItem(typing.TypedDict):
    name: str
    cost: cost_calculator.Money


class Dimension(typing.TypedDict):
    type: typing.Literal["conclusions", "jobs", "actors", "lifecycles"]
    items: list[DimensionItem]


class Dimensions(typing.TypedDict):
    conclusions: Dimension
    jobs: Dimension
    actors: typing.NotRequired[Dimension]
    lifecycles: typing.NotRequired[Dimension]


class Category(typing.TypedDict):
    type: typing.Literal["deployments", "scheduled_jobs", "pull_requests"]
    total_cost: cost_calculator.Money
    difference: typing.NotRequired[cost_calculator.Money]
    dimensions: Dimensions


class Categories(typing.TypedDict):
    deployments: Category
    scheduled_jobs: Category
    pull_requests: Category


class ReportPayload(typing.TypedDict):
    total_difference: typing.NotRequired[cost_calculator.Money]
    total_costs: cost_calculator.Money
    categories: Categories


@dataclasses.dataclass
class Query:
    owner: github_types.GitHubLogin
    repository: github_types.GitHubRepositoryName | None = None
    start_at: datetime.date | None = None
    end_at: datetime.date | None = None


@dataclasses.dataclass
class Report:
    job_registry: job_registries.JobRegistry
    pull_registry: pull_registries.PullRequestWorkflowRunPositionRegistry
    query: Query

    async def run(self) -> ReportPayload:
        current = await self._run_without_differences(
            self.query.owner,
            self.query.repository,
            self.query.start_at,
            self.query.end_at,
        )
        previous_start_at, previous_end_at = self._get_previous_date_range(
            self.query.start_at, self.query.end_at
        )
        previous = await self._run_without_differences(
            self.query.owner,
            self.query.repository,
            previous_start_at,
            previous_end_at,
        )

        current["categories"]["deployments"]["difference"] = cost_calculator.Money(
            current["categories"]["deployments"]["total_cost"]
            - previous["categories"]["deployments"]["total_cost"]
        )
        current["categories"]["scheduled_jobs"]["difference"] = cost_calculator.Money(
            current["categories"]["scheduled_jobs"]["total_cost"]
            - previous["categories"]["scheduled_jobs"]["total_cost"]
        )
        current["categories"]["pull_requests"]["difference"] = cost_calculator.Money(
            current["categories"]["pull_requests"]["total_cost"]
            - previous["categories"]["pull_requests"]["total_cost"]
        )
        current["total_difference"] = cost_calculator.Money(
            current["categories"]["deployments"]["difference"]
            + current["categories"]["scheduled_jobs"]["difference"]
            + current["categories"]["pull_requests"]["difference"]
        )
        return current

    @staticmethod
    def _get_previous_date_range(
        start_at: datetime.date | None, end_at: datetime.date | None
    ) -> tuple[datetime.date | None, datetime.date | None]:
        if start_at is None or end_at is None:
            return start_at, end_at

        delta = end_at - start_at + datetime.timedelta(days=1)
        return start_at - delta, end_at - delta

    async def _run_without_differences(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> ReportPayload:
        job_runs = [
            j
            async for j in self.job_registry.search(owner, repository, start_at, end_at)
        ]
        deployments = self._deployments(*job_runs)
        scheduled_jobs = self._scheduled_jobs(*job_runs)
        pull_requests = await self._pull_requests(*job_runs)
        total_costs = cost_calculator.Money(
            deployments["total_cost"]
            + scheduled_jobs["total_cost"]
            + pull_requests["total_cost"]
        )

        return {
            "total_costs": total_costs,
            "categories": {
                "deployments": deployments,
                "scheduled_jobs": scheduled_jobs,
                "pull_requests": pull_requests,
            },
        }

    def _deployments(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        per_job: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        total_cost = cost_calculator.Money.zero()

        for job_run in job_runs:
            if job_run.triggering_event == "push":
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
                total_cost += job_run.cost

        return {
            "type": "deployments",
            "total_cost": total_cost,
            "dimensions": {
                "conclusions": {
                    "type": "conclusions",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_conclusion.items()
                    ],
                },
                "jobs": {
                    "type": "jobs",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_job.items()
                    ],
                },
            },
        }

    def _scheduled_jobs(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        per_job: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        total_cost = cost_calculator.Money.zero()

        for job_run in job_runs:
            if job_run.triggering_event == "schedule":
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
                total_cost += job_run.cost

        return {
            "type": "scheduled_jobs",
            "total_cost": total_cost,
            "dimensions": {
                "conclusions": {
                    "type": "conclusions",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_conclusion.items()
                    ],
                },
                "jobs": {
                    "type": "jobs",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_job.items()
                    ],
                },
            },
        }

    async def _pull_requests(self, *job_runs: models.JobRun) -> Category:
        per_actor: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        per_job: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        per_lifecycle: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        per_conclusion: dict[str, cost_calculator.Money] = collections.defaultdict(
            cost_calculator.Money.zero
        )
        total_cost = cost_calculator.Money.zero()

        for job_run in job_runs:
            if job_run.triggering_event in ("pull_request", "pull_request_target"):
                per_actor[job_run.triggering_actor.login] += job_run.cost
                per_job[job_run.name] += job_run.cost
                per_conclusion[job_run.conclusion] += job_run.cost

                lifecycle = await self._lifecycle(job_run)
                if lifecycle is not None:
                    per_lifecycle[lifecycle] += job_run.cost

                total_cost += job_run.cost

        return {
            "type": "pull_requests",
            "total_cost": total_cost,
            "dimensions": {
                "actors": {
                    "type": "actors",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_actor.items()
                    ],
                },
                "jobs": {
                    "type": "jobs",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_job.items()
                    ],
                },
                "lifecycles": {
                    "type": "lifecycles",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_lifecycle.items()
                    ],
                },
                "conclusions": {
                    "type": "conclusions",
                    "items": [
                        DimensionItem(name=conclusion, cost=cost)
                        for conclusion, cost in per_conclusion.items()
                    ],
                },
            },
        }

    async def _lifecycle(self, job_run: models.JobRun) -> str | None:
        if job_run.lifecycle is None:
            return None
        elif job_run.lifecycle == "retry":
            return "Manual retry"
        else:
            for pull in job_run.pulls:
                position = await self.pull_registry.get_job_run_position(
                    pull_id=pull.id, job_run=job_run
                )
                if position == 0:
                    return "Initial push"
                else:
                    return "Update"
        return None
