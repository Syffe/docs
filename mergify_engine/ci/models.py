from __future__ import annotations

import dataclasses
import datetime
import re
import typing

from mergify_engine import github_types
from mergify_engine.ci import cost_calculator


if typing.TYPE_CHECKING:
    from mergify_engine.ci import pull_registries


Lifecycle = typing.Literal["push", "retry"]
OperatingSystem = typing.Literal["Linux", "macOS", "Windows", "Unknown"]


@dataclasses.dataclass
class Account:
    id: github_types.GitHubAccountIdType
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
    state: github_types.GitHubPullRequestState


class RunnerProperties(typing.NamedTuple):
    operating_system: OperatingSystem
    cores: int

    @classmethod
    def unknown(cls) -> RunnerProperties:
        return cls("Unknown", 0)


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
    def cost(self) -> cost_calculator.MoneyAmount:
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

    @classmethod
    async def create_job(
        cls,
        pull_registry: pull_registries.PullRequestFromCommitRegistry,
        job_payload: github_types.GitHubJobRun,
        run_payload: github_types.GitHubWorkflowRun,
    ) -> JobRun:
        runner_properties = cls._extract_runner_properties(job_payload)

        return cls(
            id=job_payload["id"],
            workflow_run_id=run_payload["id"],
            workflow_id=run_payload["workflow_id"],
            name=job_payload["name"],
            owner=Account(
                id=run_payload["repository"]["owner"]["id"],
                login=run_payload["repository"]["owner"]["login"],
            ),
            repository=run_payload["repository"]["name"],
            conclusion=job_payload["conclusion"],
            triggering_event=run_payload["event"],
            triggering_actor=Account(
                id=run_payload["triggering_actor"]["id"],
                login=run_payload["triggering_actor"]["login"],
            ),
            started_at=datetime.datetime.fromisoformat(job_payload["started_at"]),
            completed_at=datetime.datetime.fromisoformat(job_payload["completed_at"]),
            pulls=await pull_registry.get_from_commit(
                run_payload["repository"]["owner"]["login"],
                run_payload["repository"]["name"],
                run_payload["head_sha"],
            ),
            run_attempt=run_payload["run_attempt"],
            operating_system=runner_properties.operating_system,
            cores=runner_properties.cores,
        )

    @classmethod
    def _extract_runner_properties(
        cls,
        job_payload: github_types.GitHubJobRun,
    ) -> RunnerProperties:
        for label in job_payload["labels"]:
            try:
                return cls._extract_runner_properties_from_label(label)
            except ValueError:
                continue

        return RunnerProperties.unknown()

    @staticmethod
    def _extract_runner_properties_from_label(label: str) -> RunnerProperties:
        # NOTE(charly):
        # https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners#supported-runners-and-hardware-resources
        # https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#choosing-github-hosted-runners
        match = re.match(
            r"(ubuntu|windows|macos)-[\w\.]+(-xl)?(?:-(\d+)-cores)?", label
        )
        if not match:
            raise ValueError(f"Cannot parse label '{label}'")

        raw_os, is_xl, raw_cores = match.groups()

        operating_system: OperatingSystem
        if raw_os == "ubuntu":
            operating_system = "Linux"
        elif raw_os == "windows":
            operating_system = "Windows"
        elif raw_os == "macos":
            operating_system = "macOS"
        else:
            raise ValueError(f"Unknown operating system '{operating_system}'")

        if raw_cores is not None:
            cores = int(raw_cores)
        elif operating_system == "macOS":
            cores = 3 if not is_xl else 12
        else:
            cores = 2

        return RunnerProperties(operating_system, cores)
