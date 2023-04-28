import collections
import dataclasses
import datetime
import decimal
import typing

import pydantic

from mergify_engine import github_types
from mergify_engine.ci import cost_calculator
from mergify_engine.ci import job_registries
from mergify_engine.ci import models


DEPLOYMENTS_EVENTS: tuple[github_types.GitHubWorkflowTriggerEventType, ...] = ("push",)
SCHEDULED_JOBS_EVENTS: tuple[github_types.GitHubWorkflowTriggerEventType, ...] = (
    "schedule",
)
PULL_REQUESTS_EVENTS: tuple[github_types.GitHubWorkflowTriggerEventType, ...] = (
    "pull_request",
    "pull_request_target",
)


@pydantic.dataclasses.dataclass
class Money:
    amount: cost_calculator.MoneyAmount
    currency: typing.Literal["USD"] = "USD"

    @classmethod
    def from_decimal(cls, v: decimal.Decimal | int | str) -> "Money":
        return cls(cost_calculator.MoneyAmount(v))

    @classmethod
    def zero(cls) -> "Money":
        return cls(cost_calculator.MoneyAmount.zero())


@pydantic.dataclasses.dataclass
class DimensionItem:
    name: str
    cost: Money
    difference: Money = dataclasses.field(default_factory=Money.zero)

    def difference_with(self, other_items: list["DimensionItem"]) -> Money:
        for other_item in other_items:
            if self.name == other_item.name:
                return Money(self.cost.amount - other_item.cost.amount)

        # NOTE(charly): the other list doesn't contain any similar item, so we
        # consider it's zero
        return Money(self.cost.amount)


@pydantic.dataclasses.dataclass
class Dimension:
    type: typing.Literal["conclusions", "jobs", "actors", "lifecycles", "repositories"]
    items: list[DimensionItem]

    def difference_with(self, other: "Dimension") -> "Dimension":
        new_items = []

        for item in self.items:
            new_item = DimensionItem(
                item.name, item.cost, item.difference_with(other.items)
            )
            new_items.append(new_item)

        return self.__class__(type=self.type, items=new_items)


@pydantic.dataclasses.dataclass
class Dimensions:
    conclusions: Dimension
    jobs: Dimension
    repositories: Dimension
    actors: Dimension | None = None
    lifecycles: Dimension | None = None


@pydantic.dataclasses.dataclass
class Category:
    type: typing.Literal["deployments", "scheduled_jobs", "pull_requests"]
    total_cost: Money
    dimensions: Dimensions
    difference: Money = dataclasses.field(default_factory=Money.zero)

    def difference_with(self, other: "Category") -> "Category":
        conclusions = self.dimensions.conclusions.difference_with(
            other.dimensions.conclusions
        )
        jobs = self.dimensions.jobs.difference_with(other.dimensions.jobs)
        repositories = self.dimensions.repositories.difference_with(
            other.dimensions.repositories
        )
        actors = (
            self.dimensions.actors.difference_with(other.dimensions.actors)
            if self.dimensions.actors and other.dimensions.actors
            else None
        )
        lifecycles = (
            self.dimensions.lifecycles.difference_with(other.dimensions.lifecycles)
            if self.dimensions.lifecycles and other.dimensions.lifecycles
            else None
        )

        return self.__class__(
            type=self.type,
            total_cost=self.total_cost,
            dimensions=Dimensions(
                conclusions=conclusions,
                jobs=jobs,
                actors=actors,
                lifecycles=lifecycles,
                repositories=repositories,
            ),
            difference=Money(self.total_cost.amount - other.total_cost.amount),
        )


@pydantic.dataclasses.dataclass
class Categories:
    deployments: Category
    scheduled_jobs: Category
    pull_requests: Category


@pydantic.dataclasses.dataclass
class DateRange:
    start_at: datetime.date | None
    end_at: datetime.date | None


@pydantic.dataclasses.dataclass(
    config=pydantic.ConfigDict(
        json_encoders={cost_calculator.MoneyAmount: lambda v: float(round(v, 2))}
    )
)
class CategoryReportPayload:
    total_costs: Money
    categories: Categories
    date_range: DateRange
    compared_date_range: DateRange | None = None
    total_difference: Money = dataclasses.field(default_factory=Money.zero)

    def difference_with(
        self, other: "CategoryReportPayload"
    ) -> "CategoryReportPayload":
        deployments = self.categories.deployments.difference_with(
            other.categories.deployments
        )
        scheduled_jobs = self.categories.scheduled_jobs.difference_with(
            other.categories.scheduled_jobs
        )
        pull_requests = self.categories.pull_requests.difference_with(
            other.categories.pull_requests
        )
        total_difference = Money(
            deployments.difference.amount
            + scheduled_jobs.difference.amount
            + pull_requests.difference.amount
        )

        return self.__class__(
            total_costs=self.total_costs,
            total_difference=total_difference,
            categories=Categories(
                deployments=deployments,
                scheduled_jobs=scheduled_jobs,
                pull_requests=pull_requests,
            ),
            date_range=self.date_range,
            compared_date_range=other.date_range,
        )


@dataclasses.dataclass
class CategoryQuery:
    owner: github_types.GitHubLogin
    repository: github_types.GitHubRepositoryName | None = None
    start_at: datetime.date | None = None
    end_at: datetime.date | None = None
    compare_start_at: datetime.date | None = None
    compare_end_at: datetime.date | None = None

    def __post_init__(self) -> None:
        # No date range, we don't compute the comparison date range
        if self.start_at is None or self.end_at is None:
            return

        # Comparison date range is specified
        if self.compare_start_at is not None and self.compare_end_at is not None:
            return

        # Compute comparison date range
        delta = self.end_at - self.start_at + datetime.timedelta(days=1)
        self.compare_start_at = self.start_at - delta
        self.compare_end_at = self.end_at - delta


@dataclasses.dataclass
class CategoryReport:
    job_registry: job_registries.JobRegistry
    query: CategoryQuery

    async def run(self) -> CategoryReportPayload:
        report = await self._run_without_differences(
            self.query.owner,
            self.query.repository,
            self.query.start_at,
            self.query.end_at,
        )

        other_report = await self._run_without_differences(
            self.query.owner,
            self.query.repository,
            self.query.compare_start_at,
            self.query.compare_end_at,
        )

        return report.difference_with(other_report)

    async def _run_without_differences(
        self,
        owner: str,
        repository: str | None,
        start_at: datetime.date | None,
        end_at: datetime.date | None,
    ) -> CategoryReportPayload:
        job_runs = [
            j
            async for j in self.job_registry.search(owner, repository, start_at, end_at)
        ]
        deployments = self._deployments(*job_runs)
        scheduled_jobs = self._scheduled_jobs(*job_runs)
        pull_requests = await self._pull_requests(*job_runs)
        total_costs = cost_calculator.MoneyAmount(
            deployments.total_cost.amount
            + scheduled_jobs.total_cost.amount
            + pull_requests.total_cost.amount
        )

        return CategoryReportPayload(
            total_costs=Money(total_costs),
            categories=Categories(
                deployments=deployments,
                scheduled_jobs=scheduled_jobs,
                pull_requests=pull_requests,
            ),
            date_range=DateRange(start_at=start_at, end_at=end_at),
        )

    def _deployments(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        per_job: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        per_repository: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event in DEPLOYMENTS_EVENTS:
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
                per_repository[job_run.repository] += job_run.cost
                total_cost += job_run.cost

        return Category(
            type="deployments",
            total_cost=Money(total_cost),
            dimensions=Dimensions(
                conclusions=Dimension(
                    type="conclusions",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_conclusion.items()
                    ],
                ),
                jobs=Dimension(
                    type="jobs",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_job.items()
                    ],
                ),
                repositories=Dimension(
                    type="repositories",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_repository.items()
                    ],
                ),
            ),
        )

    def _scheduled_jobs(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        per_job: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        per_repository: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event in SCHEDULED_JOBS_EVENTS:
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
                per_repository[job_run.repository] += job_run.cost
                total_cost += job_run.cost

        return Category(
            type="scheduled_jobs",
            total_cost=Money(total_cost),
            dimensions=Dimensions(
                conclusions=Dimension(
                    type="conclusions",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_conclusion.items()
                    ],
                ),
                jobs=Dimension(
                    type="jobs",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_job.items()
                    ],
                ),
                repositories=Dimension(
                    type="repositories",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_repository.items()
                    ],
                ),
            ),
        )

    async def _pull_requests(self, *job_runs: models.JobRun) -> Category:
        per_actor: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        per_job: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        per_lifecycle: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        per_conclusion: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        per_repository: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event in PULL_REQUESTS_EVENTS:
                per_actor[job_run.triggering_actor.login] += job_run.cost
                per_job[job_run.name] += job_run.cost
                per_conclusion[job_run.conclusion] += job_run.cost
                per_repository[job_run.repository] += job_run.cost

                lifecycle = await self._lifecycle(job_run)
                if lifecycle is not None:
                    per_lifecycle[lifecycle] += job_run.cost

                total_cost += job_run.cost

        return Category(
            type="pull_requests",
            total_cost=Money(total_cost),
            dimensions=Dimensions(
                actors=Dimension(
                    type="actors",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_actor.items()
                    ],
                ),
                jobs=Dimension(
                    type="jobs",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_job.items()
                    ],
                ),
                lifecycles=Dimension(
                    type="lifecycles",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_lifecycle.items()
                    ],
                ),
                conclusions=Dimension(
                    type="conclusions",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_conclusion.items()
                    ],
                ),
                repositories=Dimension(
                    type="repositories",
                    items=[
                        DimensionItem(name=conclusion, cost=Money(cost))
                        for conclusion, cost in per_repository.items()
                    ],
                ),
            ),
        )

    async def _lifecycle(self, job_run: models.JobRun) -> str | None:
        if job_run.lifecycle is None:
            return None
        if job_run.lifecycle == "retry":
            return "Manual retry"

        for pull in job_run.pulls:
            position = self.job_registry.get_job_running_order(pull.id, job_run)
            if position == 1:
                return "Initial push"
            return "Update"

        return None


@pydantic.dataclasses.dataclass
class RepositoryCategories:
    deployments: Money
    scheduled_jobs: Money
    pull_requests: Money


@pydantic.dataclasses.dataclass
class Repository:
    name: github_types.GitHubRepositoryName
    total_cost: Money
    categories: RepositoryCategories


@pydantic.dataclasses.dataclass(
    config=pydantic.ConfigDict(
        json_encoders={cost_calculator.MoneyAmount: lambda v: float(round(v, 2))}
    )
)
class RepositoryReportPayload:
    repositories: list[Repository]
    date_range: DateRange


@dataclasses.dataclass
class RepositoryQuery:
    owner: github_types.GitHubLogin
    start_at: datetime.date | None = None
    end_at: datetime.date | None = None


@dataclasses.dataclass
class RepositoryReport:
    job_registry: job_registries.JobRegistry
    query: RepositoryQuery

    async def run(self) -> RepositoryReportPayload:
        per_repository: dict[
            github_types.GitHubRepositoryName, list[models.JobRun]
        ] = collections.defaultdict(list)

        job_runs = self.job_registry.search(
            self.query.owner, None, self.query.start_at, self.query.end_at
        )

        async for job_run in job_runs:
            per_repository[job_run.repository].append(job_run)

        return RepositoryReportPayload(
            repositories=[
                self._create_repository(name, repo_job_runs)
                for name, repo_job_runs in per_repository.items()
            ],
            date_range=DateRange(
                start_at=self.query.start_at, end_at=self.query.end_at
            ),
        )

    def _create_repository(
        self, name: github_types.GitHubRepositoryName, job_runs: list[models.JobRun]
    ) -> Repository:
        total_cost = cost_calculator.MoneyAmount.zero()
        deployments = cost_calculator.MoneyAmount.zero()
        scheduled_jobs = cost_calculator.MoneyAmount.zero()
        pull_requests = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            total_cost += job_run.cost

            if job_run.triggering_event in DEPLOYMENTS_EVENTS:
                deployments += job_run.cost
            elif job_run.triggering_event in SCHEDULED_JOBS_EVENTS:
                scheduled_jobs += job_run.cost
            elif job_run.triggering_event in PULL_REQUESTS_EVENTS:
                pull_requests += job_run.cost

        categories = RepositoryCategories(
            deployments=Money(deployments),
            scheduled_jobs=Money(scheduled_jobs),
            pull_requests=Money(pull_requests),
        )

        return Repository(name, Money(total_cost), categories)
