from collections import abc
import datetime
from unittest import mock

from mergify_engine import duplicate_pull
from mergify_engine import github_types
from mergify_engine.tests.unit import conftest


async def fake_get_github_pulls_from_sha(
    url: str, resource_name: str, page_limit: str, api_version: None | str = None
) -> abc.AsyncGenerator[github_types.GitHubPullRequest, None]:
    pr = github_types.GitHubPullRequest(
        {
            "id": github_types.GitHubPullRequestId(1),
            "maintainer_can_modify": False,
            "merged_at": None,
            "state": "open",
            "merge_commit_sha": github_types.SHAType(""),
            "user": github_types.GitHubAccount(
                {
                    "login": github_types.GitHubLogin(""),
                    "id": github_types.GitHubAccountIdType(0),
                    "type": "User",
                    "avatar_url": "",
                }
            ),
            "labels": [],
            "merged": False,
            "mergeable": False,
            "mergeable_state": "unknown",
            "html_url": "",
            "issue_url": "",
            "locked": False,
            "assignees": [],
            "requested_teams": [],
            "created_at": github_types.ISODateTimeType(""),
            "title": "",
            "body": "",
            "changed_files": 0,
            "commits": 0,
            "requested_reviewers": [],
            "closed_at": github_types.ISODateTimeType(""),
            "node_id": "",
            "merged_by": None,
            "rebaseable": False,
            "draft": False,
            "milestone": None,
            "updated_at": github_types.ISODateTimeType(""),
            "number": github_types.GitHubPullRequestNumber(6),
            "base": {
                "label": github_types.GitHubBaseBranchLabel(""),
                "user": github_types.GitHubAccount(
                    {
                        "login": github_types.GitHubLogin(""),
                        "id": github_types.GitHubAccountIdType(0),
                        "type": "User",
                        "avatar_url": "",
                    }
                ),
                "ref": github_types.GitHubRefType("main"),
                "sha": github_types.SHAType("the-base-sha"),
                "repo": {
                    "id": github_types.GitHubRepositoryIdType(1),
                    "owner": github_types.GitHubAccount(
                        {
                            "login": github_types.GitHubLogin(""),
                            "id": github_types.GitHubAccountIdType(0),
                            "type": "User",
                            "avatar_url": "",
                        }
                    ),
                    "archived": False,
                    "url": "",
                    "html_url": "",
                    "default_branch": github_types.GitHubRefType(""),
                    "full_name": "Mergifyio/mergify-engine",
                    "name": github_types.GitHubRepositoryName("mergify-engine"),
                    "private": False,
                },
            },
            "head": {
                "ref": github_types.GitHubRefType("main"),
                "sha": github_types.SHAType("the-base-sha"),
                "label": github_types.GitHubHeadBranchLabel(""),
                "user": github_types.GitHubAccount(
                    {
                        "login": github_types.GitHubLogin(""),
                        "id": github_types.GitHubAccountIdType(0),
                        "type": "User",
                        "avatar_url": "",
                    }
                ),
                "repo": {
                    "id": github_types.GitHubRepositoryIdType(1),
                    "owner": github_types.GitHubAccount(
                        {
                            "login": github_types.GitHubLogin(""),
                            "id": github_types.GitHubAccountIdType(0),
                            "type": "User",
                            "avatar_url": "",
                        }
                    ),
                    "archived": False,
                    "url": "",
                    "html_url": "",
                    "default_branch": github_types.GitHubRefType(""),
                    "full_name": "Mergifyio/mergify-engine",
                    "name": github_types.GitHubRepositoryName("mergify-engine"),
                    "private": False,
                },
            },
        }
    )
    if url.endswith("commits/rebased_c1/pulls"):
        yield pr
    elif url.endswith("commits/rebased_c2/pulls"):
        yield pr
    else:
        return


@mock.patch(
    "mergify_engine.context.Context.commits",
    new_callable=mock.PropertyMock,
)
async def test_get_commits_to_cherry_pick_rebase(
    commits: mock.PropertyMock,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    c1 = github_types.CachedGitHubBranchCommit(
        sha=github_types.SHAType("c1f"),
        commit_message="foobar",
        commit_verification_verified=False,
        parents=[],
        author="someone",
        committer="someone-else",
        email_author="",
        email_committer="",
        date_author=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        date_committer=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        gh_author_login=github_types.GitHubLogin("someone"),
    )
    c2 = github_types.CachedGitHubBranchCommit(
        sha=github_types.SHAType("c2"),
        commit_message="foobar",
        commit_verification_verified=False,
        parents=[c1.sha],
        author="someone",
        committer="someone-else",
        email_author="",
        email_committer="",
        date_author=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        date_committer=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        gh_author_login=github_types.GitHubLogin("someone"),
    )
    commits.return_value = [c1, c2]

    client = mock.Mock()
    client.auth.get_access_token.return_value = "<token>"
    client.items.side_effect = fake_get_github_pulls_from_sha

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(6))
    ctxt.repository.installation.client = client

    base_branch = github_types.GitHubBranchCommitParent(
        {"sha": github_types.SHAType("base_branch")}
    )
    rebased_c1 = github_types.GitHubBranchCommit(
        {
            "sha": github_types.SHAType("rebased_c1"),
            "parents": [base_branch],
            "commit": {
                "message": "hello c1",
                "verification": {"verified": False},
                "author": {
                    "email": "",
                    "name": "someone",
                    "date": github_types.ISODateTimeType(
                        datetime.datetime.utcnow().isoformat()
                    ),
                },
                "committer": {
                    "email": "",
                    "name": "someone-else",
                    "date": github_types.ISODateTimeType(
                        datetime.datetime.utcnow().isoformat()
                    ),
                },
            },
            "committer": {
                "login": github_types.GitHubLogin("foobar"),
                "id": github_types.GitHubAccountIdType(1),
                "type": "User",
                "avatar_url": "",
            },
            "author": {
                "login": github_types.GitHubLogin("someone"),
                "id": github_types.GitHubAccountIdType(2),
                "type": "User",
                "avatar_url": "",
            },
        }
    )
    rebased_c2 = github_types.GitHubBranchCommit(
        {
            "sha": github_types.SHAType("rebased_c2"),
            "parents": [rebased_c1],
            "commit": {
                "message": "hello c2",
                "verification": {"verified": False},
                "author": {
                    "email": "",
                    "name": "someone",
                    "date": github_types.ISODateTimeType(
                        datetime.datetime.utcnow().isoformat()
                    ),
                },
                "committer": {
                    "email": "",
                    "name": "someone-else",
                    "date": github_types.ISODateTimeType(
                        datetime.datetime.utcnow().isoformat()
                    ),
                },
            },
            "committer": {
                "login": github_types.GitHubLogin("foobar"),
                "id": github_types.GitHubAccountIdType(1),
                "type": "User",
                "avatar_url": "",
            },
            "author": {
                "login": github_types.GitHubLogin("someone"),
                "id": github_types.GitHubAccountIdType(2),
                "type": "User",
                "avatar_url": "",
            },
        }
    )

    async def fake_get_github_commit_from_sha(
        url: str, api_version: None | str = None
    ) -> github_types.GitHubBranchCommit:
        if url.endswith("/commits/rebased_c1"):
            return rebased_c1
        if url.endswith("/commits/rebased_c2"):
            return rebased_c2
        if url.endswith(f"/commits/{ctxt.pull['merge_commit_sha']}"):
            return rebased_c2
        raise RuntimeError(f"Unknown URL {url}")

    client.item.side_effect = fake_get_github_commit_from_sha

    assert await duplicate_pull.get_commits_to_cherrypick(ctxt) == [
        github_types.to_cached_github_branch_commit(rebased_c1),
        github_types.to_cached_github_branch_commit(rebased_c2),
    ]


@mock.patch(
    "mergify_engine.context.Context.commits",
    new_callable=mock.PropertyMock,
)
async def test_get_commits_to_cherry_pick_merge(
    commits: mock.PropertyMock,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    c1 = github_types.CachedGitHubBranchCommit(
        sha=github_types.SHAType("c1f"),
        commit_message="foobar",
        commit_verification_verified=False,
        parents=[],
        author="someone",
        committer="someone-else",
        email_author="",
        email_committer="",
        date_author=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        date_committer=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        gh_author_login=github_types.GitHubLogin("someone"),
    )
    c2 = github_types.CachedGitHubBranchCommit(
        sha=github_types.SHAType("c2"),
        commit_message="foobar",
        commit_verification_verified=False,
        parents=[c1.sha],
        author="someone",
        committer="someone-else",
        email_author="",
        email_committer="",
        date_author=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        date_committer=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        gh_author_login=github_types.GitHubLogin("someone"),
    )

    async def fake_commits() -> list[github_types.CachedGitHubBranchCommit]:
        return [c1, c2]

    commits.return_value = fake_commits()

    client = mock.Mock()
    client.auth.get_access_token.return_value = "<token>"

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client

    base_branch = github_types.CachedGitHubBranchCommit(
        sha=github_types.SHAType("base_branch"),
        commit_message="foobar",
        commit_verification_verified=False,
        parents=[],
        author="someone",
        committer="someone-else",
        email_author="",
        email_committer="",
        date_author=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        date_committer=github_types.ISODateTimeType(
            datetime.datetime.utcnow().isoformat()
        ),
        gh_author_login=github_types.GitHubLogin("someone"),
    )
    merge_commit = github_types.GitHubBranchCommit(
        sha=github_types.SHAType("merge_commit"),
        commit={
            "message": "foobar",
            "verification": {"verified": True},
            "author": {
                "name": "",
                "email": "",
                "date": github_types.ISODateTimeType(
                    datetime.datetime.utcnow().isoformat()
                ),
            },
            "committer": {
                "name": "",
                "email": "",
                "date": github_types.ISODateTimeType(
                    datetime.datetime.utcnow().isoformat()
                ),
            },
        },
        parents=[{"sha": base_branch.sha}, {"sha": c2.sha}],
        committer={
            "id": github_types.GitHubAccountIdType(0),
            "type": "User",
            "login": github_types.GitHubLogin("someone-else"),
            "avatar_url": "",
        },
        author={
            "id": github_types.GitHubAccountIdType(1),
            "type": "User",
            "login": github_types.GitHubLogin("someone"),
            "avatar_url": "",
        },
    )
    client.item = mock.AsyncMock(return_value=merge_commit)
    assert await duplicate_pull.get_commits_to_cherrypick(ctxt) == [
        c1,
        c2,
    ]
