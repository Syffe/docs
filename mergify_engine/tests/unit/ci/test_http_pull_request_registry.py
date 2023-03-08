import pytest
import respx

from mergify_engine import config
from mergify_engine import github_types
from mergify_engine.ci import models
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github


@pytest.mark.respx(base_url=config.GITHUB_REST_API_URL)
async def test_get_from_commit(respx_mock: respx.MockRouter) -> None:
    client = github.AsyncGithubClient(auth=None)  # type: ignore [arg-type]
    registry = pull_registries.HTTPPullRequestRegistry(client)
    respx_mock.get("/repos/some-owner/some-repo/commits/some-sha/pulls").respond(
        200, json=[{"id": 1234, "number": 12, "title": "feat: my awesome feature"}]
    )

    pulls = await registry.get_from_commit(
        github_types.GitHubLogin("some-owner"),
        github_types.GitHubRepositoryName("some-repo"),
        github_types.SHAType("some-sha"),
    )

    assert len(pulls) == 1
    pull = pulls[0]
    assert pull.id == 1234
    assert pull.number == 12
    assert pull.title == "feat: my awesome feature"
    assert len(registry.cache_from_commit) == 1


async def test_get_from_commit_hit_cache(respx_mock: respx.MockRouter) -> None:
    client = github.AsyncGithubClient(auth=None)  # type: ignore [arg-type]
    registry = pull_registries.HTTPPullRequestRegistry(client)
    registry.cache_from_commit["some-sha"] = [
        models.PullRequest(id=1234, number=12, title="feat: my awesome feature")
    ]

    pulls = await registry.get_from_commit(
        github_types.GitHubLogin("some-owner"),
        github_types.GitHubRepositoryName("some-repo"),
        github_types.SHAType("some-sha"),
    )

    respx.calls.assert_not_called()
    assert len(pulls) == 1
    pull = pulls[0]
    assert pull.id == 1234
    assert pull.number == 12
    assert pull.title == "feat: my awesome feature"
