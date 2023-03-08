import datetime

import pytest
import respx

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine.ci import job_registries
from mergify_engine.ci import models
from mergify_engine.clients import github


@pytest.mark.respx(base_url=config.GITHUB_REST_API_URL)
async def test_search(respx_mock: respx.MockRouter) -> None:
    client = github.AsyncGithubClient(auth=None)  # type: ignore [arg-type]
    registry = job_registries.HTTPJobRegistry(client)
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
                },
                {
                    "id": 3,
                    "name": "job name",
                    "conclusion": "pending",
                    "started_at": "2023-01-24T17:32:02Z",
                    "completed_at": None,
                },
            ],
        },
    )
    respx_mock.get("/repos/some-owner/some-repo/commits/some-sha/pulls").respond(
        200, json=[{"id": 5, "number": 6, "title": "feat: my awesome feature"}]
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
            models.PullRequest(id=5, number=6, title="feat: my awesome feature")
        ]
        assert job.run_attempt == 1
        assert job.operating_system == "Linux"
        assert job.cores == 2
