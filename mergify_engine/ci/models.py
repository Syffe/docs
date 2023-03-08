from __future__ import annotations

import dataclasses
import datetime
import typing

from mergify_engine import github_types
from mergify_engine.ci import cost_calculator


Lifecycle = typing.Literal["push", "retry"]
OperatingSystem = typing.Literal["Linux", "macOS", "Windows"]


@dataclasses.dataclass
class Account:
    id: int
    login: github_types.GitHubLogin


@dataclasses.dataclass(eq=True, frozen=True)
class WorkflowRun:
    id: int
    started_at: datetime.datetime


@dataclasses.dataclass
class PullRequest:
    id: int
    number: int
    title: str


@dataclasses.dataclass
class JobRun:
    id: int
    workflow_run_id: int
    workflow_id: int
    name: str
    owner: Account
    repository: github_types.GitHubRepositoryName
    conclusion: github_types.GitHubJobRunConclusionType
    triggering_event: github_types.GitHubWorkflowTriggerEventType
    triggering_actor: Account
    started_at: datetime.datetime
    completed_at: datetime.datetime
    pulls: list[PullRequest]
    run_attempt: int
    operating_system: OperatingSystem
    cores: int

    @property
    def timing(self) -> datetime.timedelta:
        return self.completed_at - self.started_at

    @property
    def cost(self) -> cost_calculator.Money:
        return cost_calculator.CostCalculator.calculate(
            self.timing, self.operating_system, self.cores
        )

    @property
    def lifecycle(self) -> Lifecycle | None:
        if self.triggering_event not in ("pull_request", "pull_request_target"):
            return None

        if self.run_attempt > 1:
            return "retry"

        return "push"
