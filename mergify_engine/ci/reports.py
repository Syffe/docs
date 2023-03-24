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
from mergify_engine.ci import pull_registries


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
    type: typing.Literal["conclusions", "jobs", "actors", "lifecycles"]
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
            ),
            difference=Money(self.total_cost.amount - other.total_cost.amount),
        )


@pydantic.dataclasses.dataclass
class Categories:
    deployments: Category
    scheduled_jobs: Category
    pull_requests: Category


@pydantic.dataclasses.dataclass(
    config=pydantic.ConfigDict(
        json_encoders={cost_calculator.MoneyAmount: lambda v: float(round(v, 2))}
    )
)
class ReportPayload:
    total_costs: Money
    categories: Categories
    total_difference: Money = dataclasses.field(default_factory=Money.zero)

    def difference_with(self, other: "ReportPayload") -> "ReportPayload":
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
        )


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

        return current.difference_with(previous)

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
        total_costs = cost_calculator.MoneyAmount(
            deployments.total_cost.amount
            + scheduled_jobs.total_cost.amount
            + pull_requests.total_cost.amount
        )

        return ReportPayload(
            total_costs=Money(total_costs),
            categories=Categories(
                deployments=deployments,
                scheduled_jobs=scheduled_jobs,
                pull_requests=pull_requests,
            ),
        )

    def _deployments(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        per_job: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event == "push":
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
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
            ),
        )

    def _scheduled_jobs(self, *job_runs: models.JobRun) -> Category:
        per_conclusion: dict[
            str, cost_calculator.MoneyAmount
        ] = collections.defaultdict(cost_calculator.MoneyAmount.zero)
        per_job: dict[str, cost_calculator.MoneyAmount] = collections.defaultdict(
            cost_calculator.MoneyAmount.zero
        )
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event == "schedule":
                per_conclusion[job_run.conclusion] += job_run.cost
                per_job[job_run.name] += job_run.cost
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
        total_cost = cost_calculator.MoneyAmount.zero()

        for job_run in job_runs:
            if job_run.triggering_event in ("pull_request", "pull_request_target"):
                per_actor[job_run.triggering_actor.login] += job_run.cost
                per_job[job_run.name] += job_run.cost
                per_conclusion[job_run.conclusion] += job_run.cost

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
            ),
        )

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
