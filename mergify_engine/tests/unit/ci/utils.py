import datetime
import typing

from mergify_engine import github_types
from mergify_engine.ci import models


def create_job(
    *,
    job_id: int = 1,
    workflow_run_id: int = 1,
    workflow_id: int = 1,
    name: str = "hello",
    owner: str = "generalkenobi",
    repository: str = "hello-there",
    conclusion: str = "success",
    timing: int = 10,
    triggering_event: str = "pull_request",
    triggering_actor: str = "dependabot",
    pull_id: int = 1,
    run_attempt: int = 1,
    operating_system: str = "Linux",
    cores: int = 2,
) -> models.JobRun:
    started_at = datetime.datetime.now()
    completed_at = started_at + datetime.timedelta(seconds=timing)

    return models.JobRun(
        id=job_id,
        workflow_run_id=workflow_run_id,
        workflow_id=workflow_id,
        name=name,
        owner=models.Account(id=1, login=github_types.GitHubLogin(owner)),
        repository=github_types.GitHubRepositoryName(repository),
        conclusion=typing.cast(github_types.GitHubJobRunConclusionType, conclusion),
        triggering_event=typing.cast(
            github_types.GitHubWorkflowTriggerEventType, triggering_event
        ),
        triggering_actor=models.Account(
            id=2, login=github_types.GitHubLogin(triggering_actor)
        ),
        started_at=started_at,
        completed_at=completed_at,
        pulls=[
            models.PullRequest(
                id=pull_id, number=1234, title="feat: hello", state="open"
            )
        ],
        run_attempt=run_attempt,
        operating_system=typing.cast(models.OperatingSystem, operating_system),
        cores=cores,
    )
