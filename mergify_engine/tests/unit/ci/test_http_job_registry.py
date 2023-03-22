import datetime

import pytest
import respx

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine.ci import job_registries
from mergify_engine.ci import models
from mergify_engine.ci import models as ci_models
from mergify_engine.clients import github


class FakePullRequestRegistry:
    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[models.PullRequest]:
        return [
            models.PullRequest(
                id=5, number=6, title="feat: my awesome feature", state="open"
            )
        ]


@pytest.mark.respx(base_url=config.GITHUB_REST_API_URL)
async def test_search(respx_mock: respx.MockRouter) -> None:
    client = github.AsyncGithubClient(auth=None)  # type: ignore [arg-type]
    registry = job_registries.HTTPJobRegistry(client, FakePullRequestRegistry())
    respx_mock.get("/repos/some-owner/some-repo/actions/runs").respond(
        200,
        json={
            "total_count": 1,
            "workflow_runs": [
                {
                    "id": 1,
                    "workflow_id": 4,
                    "repository": {
                        "name": "some-repo",
                        "owner": {"id": 1, "login": "some-owner"},
                    },
                    "event": "pull_request",
                    "triggering_actor": {"id": 2, "login": "some-user"},
                    "head_sha": "some-sha",
                    "run_attempt": 1,
                    "jobs_url": "https://api.github.com/repos/some-owner/some-repo/actions/runs/1/jobs",
                }
            ],
        },
    )
    respx_mock.get("/repos/some-owner/some-repo/actions/runs/1/jobs").respond(
        200,
        json={
            "total_count": 2,
            "jobs": [
                {
                    "id": 2,
                    "name": "job name",
                    "conclusion": "success",
                    "started_at": "2023-01-24T17:32:02Z",
                    "completed_at": "2023-01-24T17:35:38Z",
                    "labels": ["ubuntu-latest"],
                },
                # Job is not completed, should be ignored
                {
                    "id": 3,
                    "name": "job name",
                    "conclusion": "pending",
                    "started_at": "2023-01-24T17:32:02Z",
                    "completed_at": None,
                    "labels": ["ubuntu-latest"],
                },
                # Job was skipped, should be ignored
                {
                    "id": 4,
                    "name": "job name",
                    "conclusion": "skipped",
                    "started_at": "2023-01-24T17:32:02Z",
                    "completed_at": "2023-01-24T17:32:02Z",
                    "labels": [],
                },
            ],
        },
    )

    jobs = registry.search(
        github_types.GitHubLogin("some-owner"),
        github_types.GitHubRepositoryName("some-repo"),
        datetime.date(2023, 2, 1),
    )

    async for job in jobs:
        assert job.id == 2
        assert job.workflow_run_id == 1
        assert job.workflow_id == 4
        assert job.name == "job name"
        assert job.owner.login == "some-owner"
        assert job.repository == "some-repo"
        assert job.conclusion == "success"
        assert job.triggering_event == "pull_request"
        assert job.triggering_actor.login == "some-user"
        assert job.started_at == datetime.datetime.fromisoformat("2023-01-24T17:32:02Z")
        assert job.completed_at == datetime.datetime.fromisoformat(
            "2023-01-24T17:35:38Z"
        )
        assert job.pulls == [
            models.PullRequest(
                id=5, number=6, title="feat: my awesome feature", state="open"
            )
        ]
        assert job.run_attempt == 1
        assert job.operating_system == "Linux"
        assert job.cores == 2


@pytest.mark.parametrize(
    "job_payload,expected_os,expected_cores",
    (
        ({"labels": ["ubuntu-20.04"]}, "Linux", 2),
        ({"labels": ["ubuntu-latest"]}, "Linux", 2),
        ({"labels": ["ubuntu-latest-8-cores"]}, "Linux", 8),
        ({"labels": ["windows-latest"]}, "Windows", 2),
        ({"labels": ["windows-2022"]}, "Windows", 2),
        ({"labels": ["windows-latest-64-cores"]}, "Windows", 64),
        ({"labels": ["macos-latest"]}, "macOS", 3),
        ({"labels": ["macos-12"]}, "macOS", 3),
        ({"labels": ["macos-latest-xl"]}, "macOS", 3),
        ({"labels": ["macos-12-xl"]}, "macOS", 3),
        ({"labels": ["macos-10.15"]}, "macOS", 3),
    ),
)
async def test_extract_runner_properties(
    job_payload: github_types.GitHubJobRun,
    expected_os: ci_models.OperatingSystem,
    expected_cores: int,
) -> None:
    runner_properties = job_registries.HTTPJobRegistry._extract_runner_properties(
        job_payload
    )

    assert runner_properties.operating_system == expected_os
    assert runner_properties.cores == expected_cores


@pytest.mark.parametrize(
    "job_payload,expected_error_message",
    (
        ({"labels": []}, "Unknown runner"),
        ({"labels": ["whatever-latest"]}, "Unknown runner"),
    ),
)
async def test_extract_runner_properties_error(
    job_payload: github_types.GitHubJobRun, expected_error_message: str
) -> None:
    with pytest.raises(RuntimeError, match=expected_error_message):
        job_registries.HTTPJobRegistry._extract_runner_properties(job_payload)
