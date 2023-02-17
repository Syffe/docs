from datetime import datetime
import typing
from unittest import mock

import pytest

from mergify_engine import github_types
from mergify_engine.tests.unit import conftest


def create_commit(sha: github_types.SHAType) -> github_types.GitHubBranchCommit:
    return github_types.GitHubBranchCommit(
        {
            "sha": sha,
            "parents": [],
            "commit": {
                "message": "",
                "verification": {"verified": False},
                "author": {
                    "email": "",
                    "name": "someone",
                    "date": github_types.ISODateTimeType(str(datetime.utcnow())),
                },
                "committer": {
                    "email": "",
                    "name": "someone-else",
                    "date": github_types.ISODateTimeType(str(datetime.utcnow())),
                },
            },
            "committer": {
                "login": github_types.GitHubLogin("foobar"),
                "id": github_types.GitHubAccountIdType(1),
                "type": "User",
                "avatar_url": "",
            },
        }
    )


@pytest.fixture(
    params=[
        "U-A-B-C",
        "O-A-B-C",
        "O-A-BO-C",
        "O-A-BU-C",
        "O-A-B-CU",
        "O-A-PB-CU",
        "P-A-B-CU",
        "P-A-B-C",
        "O-AP-BP-C",
        "O-AP-B-CP",
    ]
)
def commits_tree_generator(
    request: typing.Any,
) -> tuple[bool, list[github_types.GitHubBranchCommit]]:
    # NOTE(sileht):
    # tree direction: ->
    # U: mean HEAD of base branch
    # O: mean old commit of base branch
    # P: mean another unknown branch
    commits = []
    cur = create_commit(github_types.SHAType("whatever"))
    tree = request.param
    behind = "U" not in tree
    while tree:
        elem = tree[0]
        tree = tree[1:]
        if elem == "-":
            commits.append(cur)
            cur = create_commit(github_types.SHAType("whatever"))
            cur["parents"].append(commits[-1])
        elif elem == "U":
            cur["parents"].append(create_commit(github_types.SHAType("base")))
        elif elem == "O":
            cur["parents"].append(create_commit(github_types.SHAType("outdated")))
        elif elem == "P":
            cur["parents"].append(create_commit(github_types.SHAType("random-branch")))
        else:
            cur["parents"].append(create_commit(github_types.SHAType(f"sha-{elem}")))
    commits.append(cur)
    return behind, commits


async def test_pull_behind(
    commits_tree_generator: typing.Any, context_getter: conftest.ContextGetterFixture
) -> None:
    expected, commits = commits_tree_generator

    async def get_commits(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        # /pulls/X/commits
        for c in commits:
            yield c

    async def item(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        # /branch/#foo
        return {"commit": {"sha": "base"}}

    async def get_compare(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        return github_types.GitHubCompareCommits(
            {"behind_by": 0 if expected else 100, "status": "behind"}
        )

    client = mock.Mock()
    client.items.return_value = get_commits()
    client.item.side_effect = [item(), get_compare()]

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    assert expected == await ctxt.is_behind
