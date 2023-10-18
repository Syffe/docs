import pytest
import respx

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.ci import pull_registries
from mergify_engine.clients import github


@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_get_from_commit(
    respx_mock: respx.MockRouter,
    fake_pull_request: github_types.GitHubPullRequest,
) -> None:
    client = github.AsyncGitHubClient(auth=None)  # type: ignore [arg-type]
    registry = pull_registries.HTTPPullRequestRegistry(client=client)
    respx_mock.get("/repos/some-owner/some-repo/commits/some-sha/pulls").respond(
        200,
        json=[fake_pull_request],
    )
    respx_mock.get("/repos/some-owner/some-repo/pulls/69").respond(
        200,
        json=fake_pull_request,  # type: ignore[arg-type]
    )

    pulls = await registry.get_from_commit(
        github_types.GitHubLogin("some-owner"),
        github_types.GitHubRepositoryName("some-repo"),
        github_types.SHAType("some-sha"),
    )

    assert len(pulls) == 1
    pull = pulls[0]
    assert pull["id"] == 42
    assert pull["number"] == 69
    assert pull["title"] == "feat: my awesome feature"
