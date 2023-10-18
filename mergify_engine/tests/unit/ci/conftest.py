import pytest

from mergify_engine import date
from mergify_engine import github_types


@pytest.fixture
def fake_pull_request() -> github_types.GitHubPullRequest:
    user = github_types.GitHubAccount(
        login=github_types.GitHubLogin("god"),
        id=github_types.GitHubAccountIdType(0),
        type="User",
        avatar_url="",
    )
    repo = github_types.GitHubRepository(
        id=github_types.GitHubRepositoryIdType(1),
        owner=user,
        private=False,
        name=github_types.GitHubRepositoryName("world"),
        full_name="god/world",
        archived=False,
        url="lazyurl",
        html_url="lazyhtmlurl",
        default_branch=github_types.GitHubRefType("main"),
    )

    return github_types.GitHubPullRequest(
        id=github_types.GitHubPullRequestId(42),
        number=github_types.GitHubPullRequestNumber(69),
        base=github_types.GitHubBaseBranchRef(
            label=github_types.GitHubBaseBranchLabel("main"),
            ref=github_types.GitHubRefType("main"),
            sha=github_types.SHAType("12345"),
            repo=repo,
            user=user,
        ),
        head=github_types.GitHubHeadBranchRef(
            label=github_types.GitHubHeadBranchLabel("mypr"),
            ref=github_types.GitHubRefType("mypr"),
            sha=github_types.SHAType("123456"),
            repo=repo,
            user=user,
        ),
        user=user,
        labels=[],
        merged_at=None,
        draft=False,
        merge_commit_sha=None,
        html_url="",
        issue_url="",
        body="body",
        title="feat: my awesome feature",
        state="open",
        locked=False,
        assignees=[],
        requested_reviewers=[],
        requested_teams=[],
        milestone=None,
        created_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
        updated_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
        closed_at=None,
        node_id="123456789",
        maintainer_can_modify=True,
        merged=False,
        merged_by=None,
        rebaseable=True,
        mergeable=True,
        mergeable_state=None,
        changed_files=1,
        commits=1,
    )
