from collections import abc
import datetime
import typing
from unittest import mock
import zoneinfo

import pytest

from mergify_engine import context
from mergify_engine import date
from mergify_engine import dependabot_helpers
from mergify_engine import dependabot_types
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.clients import github
from mergify_engine.clients import http


async def test_user_permission_cache(redis_links: redis_utils.RedisLinks) -> None:
    class FakeClient(github.AsyncGitHubInstallationClient):
        called: int

        def __init__(self, owner: str, repo: str) -> None:
            super().__init__(auth=None)  # type: ignore[arg-type]
            self.owner = owner
            self.repo = repo
            self.called = 0

        async def item(
            self, url: str, *args: typing.Any, **kwargs: typing.Any
        ) -> dict[str, str] | None:
            self.called += 1
            if self.repo == "test":
                if (
                    url
                    == f"/repos/{self.owner}/{self.repo}/collaborators/foo/permission"
                ):
                    return {"permission": "admin"}

                if url.startswith(f"/repos/{self.owner}/{self.repo}/collaborators/"):
                    return {"permission": "loser"}

            elif self.repo == "test2":
                if (
                    url
                    == f"/repos/{self.owner}/{self.repo}/collaborators/bar/permission"
                ):
                    return {"permission": "admin"}

                if url.startswith(f"/repos/{self.owner}/{self.repo}/collaborators/"):
                    return {"permission": "loser"}

            raise ValueError(f"Unknown test URL `{url}` for repo {self.repo}")

    gh_owner = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(123),
            "login": github_types.GitHubLogin("jd"),
            "type": "User",
            "avatar_url": "",
        }
    )

    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test"),
            "private": False,
        }
    )

    user_1 = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(1),
            "login": github_types.GitHubLogin("foo"),
            "type": "User",
            "avatar_url": "",
        }
    )
    user_2 = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(2),
            "login": github_types.GitHubLogin("bar"),
            "type": "User",
            "avatar_url": "",
        }
    )
    user_3 = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(3),
            "login": github_types.GitHubLogin("baz"),
            "type": "User",
            "avatar_url": "",
        }
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
            "suspended_at": None,
        }
    )

    sub = subscription.Subscription(
        redis_links.cache,
        github_types.GitHubAccountIdType(0),
        "",
        frozenset([subscription.Features.PUBLIC_REPOSITORY]),
        ["public_repository"],
    )
    client = FakeClient(gh_owner["login"], gh_repo["name"])
    installation = context.Installation(installation_json, sub, client, redis_links)
    repository = context.Repository(installation, gh_repo)
    assert client.called == 0
    assert await repository.has_write_permission(user_1)
    assert client.called == 1
    assert await repository.has_write_permission(user_1)
    assert client.called == 1
    assert not await repository.has_write_permission(user_2)
    assert client.called == 2
    # From local cache
    assert not await repository.has_write_permission(user_2)
    assert client.called == 2
    # From redis
    repository._caches.user_permissions.clear()
    assert not await repository.has_write_permission(user_2)
    assert client.called == 2
    assert not await repository.has_write_permission(user_3)
    assert client.called == 3

    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(1),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test2"),
            "private": False,
        }
    )

    client = FakeClient(gh_owner["login"], gh_repo["name"])
    installation = context.Installation(installation_json, sub, client, redis_links)
    repository = context.Repository(installation, gh_repo)
    assert client.called == 0
    assert await repository.has_write_permission(user_2)
    assert client.called == 1
    # From local cache
    assert await repository.has_write_permission(user_2)
    assert client.called == 1
    # From redis
    repository._caches.user_permissions.clear()
    assert await repository.has_write_permission(user_2)
    assert client.called == 1

    assert not await repository.has_write_permission(user_1)
    assert client.called == 2
    await context.Repository.clear_user_permission_cache_for_repo(
        redis_links.user_permissions_cache, gh_owner, gh_repo
    )
    repository._caches.user_permissions.clear()
    assert not await repository.has_write_permission(user_1)
    assert client.called == 3
    assert not await repository.has_write_permission(user_3)
    assert client.called == 4
    await context.Repository.clear_user_permission_cache_for_org(
        redis_links.user_permissions_cache, gh_owner
    )
    repository._caches.user_permissions.clear()
    assert not await repository.has_write_permission(user_3)
    assert client.called == 5
    assert await repository.has_write_permission(user_2)
    assert client.called == 6
    # From local cache
    assert await repository.has_write_permission(user_2)
    assert client.called == 6
    # From redis
    repository._caches.user_permissions.clear()
    assert await repository.has_write_permission(user_2)
    assert client.called == 6
    await context.Repository.clear_user_permission_cache_for_user(
        redis_links.user_permissions_cache, gh_owner, gh_repo, user_2
    )
    repository._caches.user_permissions.clear()
    assert await repository.has_write_permission(user_2)
    assert client.called == 7


async def test_team_members_cache(redis_links: redis_utils.RedisLinks) -> None:
    class FakeClient(github.AsyncGitHubInstallationClient):
        called: int

        def __init__(self, owner: str, repo: str) -> None:
            super().__init__(auth=None)  # type: ignore[arg-type]
            self.owner = owner
            self.repo = repo
            self.called = 0

        async def items(
            self, url: str, *args: typing.Any, **kwargs: typing.Any
        ) -> abc.AsyncGenerator[dict[str, str], None] | None:
            self.called += 1
            if url == f"/orgs/{self.owner}/teams/team1/members":
                yield {"login": "member1"}
                yield {"login": "member2"}
            elif url == f"/orgs/{self.owner}/teams/team2/members":
                yield {"login": "member3"}
                yield {"login": "member4"}
            elif url == f"/orgs/{self.owner}/teams/team3/members":
                return
            else:
                raise ValueError(f"Unknown test URL `{url}` for repo {self.repo}")

    gh_owner = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(123),
            "login": github_types.GitHubLogin("jd"),
            "type": "User",
            "avatar_url": "",
        }
    )
    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test"),
            "private": False,
        }
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
            "suspended_at": None,
        }
    )

    team_slug1 = github_types.GitHubTeamSlug("team1")
    team_slug2 = github_types.GitHubTeamSlug("team2")
    team_slug3 = github_types.GitHubTeamSlug("team3")

    sub = subscription.Subscription(
        redis_links.cache,
        github_types.GitHubAccountIdType(0),
        "",
        frozenset([subscription.Features.PUBLIC_REPOSITORY]),
        ["public_repository"],
    )
    client = FakeClient(gh_owner["login"], gh_repo["name"])
    installation = context.Installation(installation_json, sub, client, redis_links)
    assert client.called == 0
    assert (await installation.get_team_members(team_slug1)) == ["member1", "member2"]
    assert client.called == 1
    assert (await installation.get_team_members(team_slug1)) == ["member1", "member2"]
    assert client.called == 1
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 2
    # From local cache
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 2
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 2

    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 3
    # From local cache
    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 3
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 3

    await installation.clear_team_members_cache_for_team(
        redis_links.team_members_cache,
        gh_owner,
        github_types.GitHubTeamSlug(team_slug2),
    )
    installation._caches.team_members.clear()

    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 4
    # From local cache
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 4
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 4

    await installation.clear_team_members_cache_for_org(
        redis_links.team_members_cache, gh_owner
    )
    installation._caches.team_members.clear()

    assert (await installation.get_team_members(team_slug1)) == ["member1", "member2"]
    assert client.called == 5
    # From local cache
    assert (await installation.get_team_members(team_slug1)) == ["member1", "member2"]
    assert client.called == 5
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug1)) == ["member1", "member2"]
    assert client.called == 5

    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 6
    # From local cache
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 6
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug2)) == ["member3", "member4"]
    assert client.called == 6
    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 7
    # From local cache
    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 7
    # From redis
    installation._caches.team_members.clear()
    assert (await installation.get_team_members(team_slug3)) == []
    assert client.called == 7


async def test_team_permission_cache(redis_links: redis_utils.RedisLinks) -> None:
    class FakeClient(github.AsyncGitHubInstallationClient):
        called: int

        def __init__(self, owner: str, repo: str) -> None:
            super().__init__(auth=None)  # type: ignore[arg-type]
            self.owner = owner
            self.repo = repo
            self.called = 0

        async def get(  # type: ignore[override]
            self, url: str, *args: typing.Any, **kwargs: typing.Any
        ) -> dict[typing.Any, typing.Any] | None:
            self.called += 1
            if (
                url
                == f"/orgs/{self.owner}/teams/team-ok/repos/{self.owner}/{self.repo}"
            ):
                return {}

            if (
                url
                == f"/orgs/{self.owner}/teams/team-nok/repos/{self.owner}/{self.repo}"
            ):
                raise http.HTTPNotFound(
                    message="Not found", request=mock.ANY, response=mock.ANY
                )

            if (
                url
                == f"/orgs/{self.owner}/teams/team-also-nok/repos/{self.owner}/{self.repo}"
            ):
                raise http.HTTPNotFound(
                    message="Not found", request=mock.ANY, response=mock.ANY
                )

            raise ValueError(f"Unknown test URL `{url}`")

    gh_owner = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(123),
            "login": github_types.GitHubLogin("jd"),
            "type": "User",
            "avatar_url": "",
        }
    )

    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test"),
            "private": False,
        }
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
            "suspended_at": None,
        }
    )

    team_slug1 = github_types.GitHubTeamSlug("team-ok")
    team_slug2 = github_types.GitHubTeamSlug("team-nok")
    team_slug3 = github_types.GitHubTeamSlug("team-also-nok")

    sub = subscription.Subscription(
        redis_links.cache,
        github_types.GitHubAccountIdType(0),
        "",
        frozenset([subscription.Features.PUBLIC_REPOSITORY]),
        ["public_repository"],
    )
    client = FakeClient(gh_owner["login"], gh_repo["name"])
    installation = context.Installation(installation_json, sub, client, redis_links)
    repository = context.Repository(installation, gh_repo)
    assert client.called == 0
    assert await repository.team_has_read_permission(team_slug1)
    assert client.called == 1
    assert await repository.team_has_read_permission(team_slug1)
    assert client.called == 1
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 2
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 2
    assert not await repository.team_has_read_permission(team_slug3)
    assert client.called == 3

    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(1),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test2"),
            "private": False,
        }
    )

    client = FakeClient(gh_owner["login"], gh_repo["name"])
    installation = context.Installation(installation_json, sub, client, redis_links)
    repository = context.Repository(installation, gh_repo)
    assert client.called == 0
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 1
    # From local cache
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 1
    # From redis
    repository._caches.team_has_read_permission.clear()
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 1
    assert await repository.team_has_read_permission(team_slug1)
    assert client.called == 2
    await context.Repository.clear_team_permission_cache_for_repo(
        redis_links.team_permissions_cache, gh_owner, gh_repo
    )
    repository._caches.team_has_read_permission.clear()
    assert await repository.team_has_read_permission(team_slug1)
    assert client.called == 3
    assert not await repository.team_has_read_permission(team_slug3)
    assert client.called == 4
    await context.Repository.clear_team_permission_cache_for_org(
        redis_links.team_permissions_cache, gh_owner
    )
    repository._caches.team_has_read_permission.clear()
    assert not await repository.team_has_read_permission(team_slug3)
    assert client.called == 5
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 6
    # From local cache
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 6
    # From redis
    repository._caches.team_has_read_permission.clear()
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 6
    repository._caches.team_has_read_permission.clear()
    await context.Repository.clear_team_permission_cache_for_team(
        redis_links.team_permissions_cache, gh_owner, team_slug2
    )
    repository._caches.team_has_read_permission.clear()
    assert not await repository.team_has_read_permission(team_slug2)
    assert client.called == 7


@pytest.fixture
def a_pull_request() -> github_types.GitHubPullRequest:
    gh_owner = github_types.GitHubAccount(
        {
            "login": github_types.GitHubLogin("user"),
            "id": github_types.GitHubAccountIdType(0),
            "type": "User",
            "avatar_url": "",
        }
    )

    gh_repo = github_types.GitHubRepository(
        {
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "id": github_types.GitHubRepositoryIdType(456),
            "full_name": "user/repo",
            "name": github_types.GitHubRepositoryName("repo"),
            "private": False,
            "owner": gh_owner,
        }
    )

    return github_types.GitHubPullRequest(
        {
            "node_id": "42",
            "locked": False,
            "assignees": [],
            "requested_reviewers": [],
            "requested_teams": [],
            "milestone": None,
            "title": "",
            "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
            "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
            "closed_at": None,
            "id": github_types.GitHubPullRequestId(0),
            "maintainer_can_modify": False,
            "rebaseable": False,
            "draft": False,
            "merge_commit_sha": None,
            "labels": [],
            "number": github_types.GitHubPullRequestNumber(6),
            "commits": 1,
            "merged": True,
            "state": "closed",
            "changed_files": 1,
            "html_url": "<html_url>",
            "issue_url": "",
            "base": {
                "label": github_types.GitHubBaseBranchLabel(""),
                "sha": github_types.SHAType("sha"),
                "user": {
                    "login": github_types.GitHubLogin("user"),
                    "id": github_types.GitHubAccountIdType(0),
                    "type": "User",
                    "avatar_url": "",
                },
                "ref": github_types.GitHubRefType("ref"),
                "repo": gh_repo,
            },
            "head": {
                "label": github_types.GitHubHeadBranchLabel(""),
                "sha": github_types.SHAType("old-sha-one"),
                "ref": github_types.GitHubRefType("fork"),
                "user": {
                    "login": github_types.GitHubLogin("user"),
                    "id": github_types.GitHubAccountIdType(0),
                    "type": "User",
                    "avatar_url": "",
                },
                "repo": {
                    "archived": False,
                    "url": "",
                    "html_url": "",
                    "default_branch": github_types.GitHubRefType(""),
                    "id": github_types.GitHubRepositoryIdType(123),
                    "full_name": "fork/other",
                    "name": github_types.GitHubRepositoryName("other"),
                    "private": False,
                    "owner": {
                        "login": github_types.GitHubLogin("user"),
                        "id": github_types.GitHubAccountIdType(0),
                        "type": "User",
                        "avatar_url": "",
                    },
                },
            },
            "user": {
                "login": github_types.GitHubLogin("user"),
                "id": github_types.GitHubAccountIdType(0),
                "type": "User",
                "avatar_url": "",
            },
            "merged_by": None,
            "merged_at": None,
            "mergeable_state": "clean",
            "mergeable": True,
            "body": None,
        }
    )


async def test_length_optimisation(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request["commits"] = 10
    a_pull_request["changed_files"] = 5
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert await getattr(ctxt.pull_request, "#commits") == 10
    assert await getattr(ctxt.pull_request, "#files") == 5


async def test_context_depends_on(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request[
        "body"
    ] = f"""header

Depends-On: #123
depends-on: {settings.GITHUB_URL}/foo/bar/pull/999
depends-on: {settings.GITHUB_URL}/foo/bar/999
depends-on: azertyuiopqsdfghjklmwxcvbn
depends-on: https://somewhereelse.com/foo/bar/999
Depends-oN: {settings.GITHUB_URL}/user/repo/pull/456
Depends-oN: {settings.GITHUB_URL}/user/repo/pull/457\r
Depends-oN: {settings.GITHUB_URL}/user/repo/pull/789
 DEPENDS-ON: #42
Depends-On:  #48
Depends-On:  #50\r
Depends-On:  #999 with crap
DePeNdS-oN: {settings.GITHUB_URL}/user/repo/pull/999 with crap

footer
"""

    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert ctxt.get_depends_on() == [42, 48, 50, 123, 456, 457, 789]


@pytest.mark.parametrize(
    "merge_after_str,expected_out",
    (
        (
            "Merge-After: 2023-01-18",
            context.MergeAfterTuple(
                datetime.datetime(2023, 1, 18, tzinfo=date.UTC),
                False,
            ),
        ),
        (
            "Merge-After: 2023-01-18[Australia/Sydney]",
            context.MergeAfterTuple(
                datetime.datetime(
                    2023, 1, 18, tzinfo=zoneinfo.ZoneInfo("Australia/Sydney")
                ),
                False,
            ),
        ),
        (
            "Merge-After: 2023-01-18 08:10",
            context.MergeAfterTuple(
                datetime.datetime(2023, 1, 18, 8, 10, tzinfo=date.UTC),
                True,
            ),
        ),
        (
            "Merge-After: 2023-01-18 08:10[Europe/Paris]",
            context.MergeAfterTuple(
                datetime.datetime(
                    2023, 1, 18, 8, 10, tzinfo=zoneinfo.ZoneInfo("Europe/Paris")
                ),
                True,
            ),
        ),
        (
            "Merge-After: 2023-01-18 08:10[Africa/Porto-Novo]",
            context.MergeAfterTuple(
                datetime.datetime(
                    2023, 1, 18, 8, 10, tzinfo=zoneinfo.ZoneInfo("Africa/Porto-Novo")
                ),
                True,
            ),
        ),
        (
            "Merge-After: 2023-01-18 08:10[Africa/Sao_Tome]",
            context.MergeAfterTuple(
                datetime.datetime(
                    2023, 1, 18, 8, 10, tzinfo=zoneinfo.ZoneInfo("Africa/Sao_Tome")
                ),
                True,
            ),
        ),
        (
            "Merge-After: 2023-01-18 08:10[America/Argentina/Cordoba]",
            context.MergeAfterTuple(
                datetime.datetime(
                    2023,
                    1,
                    18,
                    8,
                    10,
                    tzinfo=zoneinfo.ZoneInfo("America/Argentina/Cordoba"),
                ),
                True,
            ),
        ),
    ),
)
async def test_context_merge_after_valid(
    a_pull_request: github_types.GitHubPullRequest,
    merge_after_str: str,
    expected_out: context.MergeAfterTuple,
) -> None:
    a_pull_request[
        "body"
    ] = f"""header

{merge_after_str}
"""

    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert ctxt.get_merge_after() == expected_out


@pytest.mark.parametrize(
    "merge_after_str",
    (
        "Merge-After: 1-01-18",
        "Merge-After: -1-01-18",
        "Merge-After: 2023-1-18",
        "Merge-After: 2023--1-18",
        "Merge-After: 2023-abc-18",
        "Merge-After: 2023-01-1",
        "Merge-After: 2023-01--1",
        "Merge-After: -1-01-18[Australia/Sydney]",
        "Merge-After: 2023-1-18[Australia/Sydney]",
        "Merge-After: 2023-01-1[Australia/Sydney]",
        "Merge-After: 2023-01-18[Australia/Sydne]",
        "Merge-After: 2023-01-18[Australia/Sydne",
        "Merge-After: 2023-01-18Australia/Sydney]",
        "Merge-After: -1-01-18 08:10",
        "Merge-After: 2023-1-18 08:10",
        "Merge-After: 2023-01-1 08:10",
        "Merge-After: 2023-01-18 8:10",
        "Merge-After: 2023-01-18 08:1",
        "Merge-After: 2023-01-18 08-10",
        "Merge-After: -1-01-18 08:10[Europe/Paris]",
        "Merge-After: 202-01-18 08:10[Europe/Paris]",
        "Merge-After: 2023-01-1 08:10[Europe/Paris]",
        "Merge-After: 2023-01-01 8:10[Europe/Paris]",
        "Merge-After: 2023-01-01 08:0[Europe/Paris]",
        "Merge-After: 2023-01-01 08:10Europe/Paris]",
        "Merge-After: 2023-01-01 08:10[lol]",
        "Merge-After: 2023-01-01 08:10[UTC",
    ),
)
async def test_context_merge_after_invalid(
    a_pull_request: github_types.GitHubPullRequest,
    merge_after_str: str,
) -> None:
    a_pull_request[
        "body"
    ] = f"""header

{merge_after_str}
"""

    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert ctxt.get_merge_after() is None


async def test_context_body_null(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request["body"] = None
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert await ctxt.pull_request._get_consolidated_data(ctxt, "body") == ""


async def test_context_body_html(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request["title"] = "chore(deps-dev): update flake8 requirement from <4 to <5"
    a_pull_request[
        "body"
    ] = """
Updates the requirements on [flake8](https://github.com/pycqa/flake8) to permit the latest version.
<details>
<summary>Commits</summary>
<ul>
<li><a href="https://github.com/PyCQA/flake8/commit/82b698e09996cdde5d473e234681d8380810d7a2"><code>82b698e</code></a> Release 4.0.1</li>
<li><a href="https://github.com/PyCQA/flake8/commit/0fac346d8437d205e508643253c7a7d5fdf5dee7"><code>0fac346</code></a> Merge pull request <a href="https://github-redirect.dependabot.com/pycqa/flake8/issues/1410">#1410</a> from PyCQA/parallel-syntax-error</li>
<li><a href="https://github.com/PyCQA/flake8/commit/aa54693c9ec03368c6e592efff4dd4757dd72a47"><code>aa54693</code></a> fix parallel execution collecting a SyntaxError</li>
<li><a href="https://github.com/PyCQA/flake8/commit/d31c5356bbb0a884555662185697ddc6bb46a44c"><code>d31c535</code></a> Release 4.0.0</li>
<li><a href="https://github.com/PyCQA/flake8/commit/afd2399b4cc9b27c4e8a5c2dec8444df8f480293"><code>afd2399</code></a> Merge pull request <a href="https://github-redirect.dependabot.com/pycqa/flake8/issues/1407">#1407</a> from asottile/setup-cfg-fmt</li>
<li><a href="https://github.com/PyCQA/flake8/commit/960cf8cf2044359d5fbd3454a2a9a1d7a0586594"><code>960cf8c</code></a> rerun setup-cfg-fmt (and restore comments)</li>
<li><a href="https://github.com/PyCQA/flake8/commit/d7baba5f14091e7975d2abb3ba9bf321b5be6102"><code>d7baba5</code></a> Merge pull request <a href="https://github-redirect.dependabot.com/pycqa/flake8/issues/1406">#1406</a> from asottile/update-versions</li>
<li><a href="https://github.com/PyCQA/flake8/commit/d79021aafc809d999c4cbbc0a513a5ceb473efa2"><code>d79021a</code></a> update dependency versions</li>
<li><a href="https://github.com/PyCQA/flake8/commit/283f0c81241673221d9628beb11e2d7356826f00"><code>283f0c8</code></a> Merge pull request <a href="https://github-redirect.dependabot.com/pycqa/flake8/issues/1404">#1404</a> from PyCQA/drop-xdg-config</li>
<li><a href="https://github.com/PyCQA/flake8/commit/807904aebc20814ac595b0004ab526fffb5ef681"><code>807904a</code></a> Drop support for Home and XDG config files</li>
<li>Additional commits viewable in <a href="https://github.com/pycqa/flake8/compare/0.1...4.0.1">compare view</a></li>
</ul>
</details>
<br />


Dependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.

[//]: # (dependabot-automerge-start)
[//]: # (dependabot-automerge-end)

---

<details>
<summary>Dependabot commands and options</summary>
<br />

You can trigger Dependabot actions by commenting on this PR:
- `@dependabot rebase` will rebase this PR
- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it
- `@dependabot merge` will merge this PR after your CI passes on it
- `@dependabot squash and merge` will squash and merge this PR after your CI passes on it
- `@dependabot cancel merge` will cancel a previously requested merge and block automerging
- `@dependabot reopen` will reopen this PR if it is closed
- `@dependabot close` will close this PR and stop Dependabot recreating it. You can achieve the same result by closing it manually
- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)
- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)
- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)


</details>
"""

    expected_title = "chore(deps-dev): update flake8 requirement from <4 to <5 (#6)"
    expected_body = """Updates the requirements on [flake8](https://github.com/pycqa/flake8) to permit the latest version.

Commits
* [`82b698e`](https://github.com/PyCQA/flake8/commit/82b698e09996cdde5d473e234681d8380810d7a2) Release 4.0.1
* [`0fac346`](https://github.com/PyCQA/flake8/commit/0fac346d8437d205e508643253c7a7d5fdf5dee7) Merge pull request [#1410](https://github-redirect.dependabot.com/pycqa/flake8/issues/1410) from PyCQA/parallel-syntax-error
* [`aa54693`](https://github.com/PyCQA/flake8/commit/aa54693c9ec03368c6e592efff4dd4757dd72a47) fix parallel execution collecting a SyntaxError
* [`d31c535`](https://github.com/PyCQA/flake8/commit/d31c5356bbb0a884555662185697ddc6bb46a44c) Release 4.0.0
* [`afd2399`](https://github.com/PyCQA/flake8/commit/afd2399b4cc9b27c4e8a5c2dec8444df8f480293) Merge pull request [#1407](https://github-redirect.dependabot.com/pycqa/flake8/issues/1407) from asottile/setup-cfg-fmt
* [`960cf8c`](https://github.com/PyCQA/flake8/commit/960cf8cf2044359d5fbd3454a2a9a1d7a0586594) rerun setup-cfg-fmt (and restore comments)
* [`d7baba5`](https://github.com/PyCQA/flake8/commit/d7baba5f14091e7975d2abb3ba9bf321b5be6102) Merge pull request [#1406](https://github-redirect.dependabot.com/pycqa/flake8/issues/1406) from asottile/update-versions
* [`d79021a`](https://github.com/PyCQA/flake8/commit/d79021aafc809d999c4cbbc0a513a5ceb473efa2) update dependency versions
* [`283f0c8`](https://github.com/PyCQA/flake8/commit/283f0c81241673221d9628beb11e2d7356826f00) Merge pull request [#1404](https://github-redirect.dependabot.com/pycqa/flake8/issues/1404) from PyCQA/drop-xdg-config
* [`807904a`](https://github.com/PyCQA/flake8/commit/807904aebc20814ac595b0004ab526fffb5ef681) Drop support for Home and XDG config files
* Additional commits viewable in [compare view](https://github.com/pycqa/flake8/compare/0.1...4.0.1)



  



Dependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.

[//]: # (dependabot-automerge-start)
[//]: # (dependabot-automerge-end)

---


Dependabot commands and options
  


You can trigger Dependabot actions by commenting on this PR:
- `@dependabot rebase` will rebase this PR
- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it
- `@dependabot merge` will merge this PR after your CI passes on it
- `@dependabot squash and merge` will squash and merge this PR after your CI passes on it
- `@dependabot cancel merge` will cancel a previously requested merge and block automerging
- `@dependabot reopen` will reopen this PR if it is closed
- `@dependabot close` will close this PR and stop Dependabot recreating it. You can achieve the same result by closing it manually
- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)
- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)
- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)



"""  # noqa
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert await ctxt.pull_request.get_commit_message(
        "{{ title }} (#{{ number }})\n\n{{ body | markdownify }}"
    ) == (expected_title, expected_body)


async def test_context_body_section(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request[
        "title"
    ] = "chore(deps-dev): update flake8 requirement <-- noway we commit this-->from <4 to <5"
    a_pull_request["head"]["ref"] = github_types.GitHubRefType(
        "daily_merge/beta/pv6.0.0"
    )
    a_pull_request[
        "body"
    ] = """
### Description

My awesome section with a beautiful description
<!-- I hide a comment !!! -->
Fixes MRGFY-XXX

### Development

- [X] All checks must pass (Semantic Pull Request, pep8, requirements, unit tests, functional tests, security checks, …)
- [X] The code changed/added as part of this pull request must be covered with tests
- [X] Features must have a link to a Linear task
- [X] Hotfixes must have a link to Sentry issue and the ``hotfix`` label

### Code Review

Code review policies are handled and automated by Mergify.

* When all tests pass, reviewers will be assigned automatically.
* When change is approved by at least one review and no pending review are
  remaining, pull request is retested against its base branch.
* The pull request is then merged automatically.

"""

    expected_title = "chore(deps-dev): update flake8 requirement from <4 to <5 (#6)"
    expected_body = """My awesome section with a beautiful description

Fixes MRGFY-XXX

daily\\_merge/beta/pv6.0.0

commits:

* azerty
* qsdfgh
"""
    ctxt = context.Context(mock.Mock(), a_pull_request)
    ctxt._caches.commits.set(
        [
            github_types.CachedGitHubBranchCommit(
                sha=github_types.SHAType("foo"),
                commit_message="azerty",
                commit_verification_verified=False,
                parents=[],
                author="someone",
                committer="someone-else",
                email_author="",
                email_committer="",
                date_author=github_types.ISODateTimeType(""),
                date_committer=github_types.ISODateTimeType(""),
            ),
            github_types.CachedGitHubBranchCommit(
                sha=github_types.SHAType("foobar"),
                commit_message="qsdfgh",
                commit_verification_verified=False,
                parents=[],
                author="someone",
                committer="someone-else",
                email_author="",
                email_committer="",
                date_author=github_types.ISODateTimeType(""),
                date_committer=github_types.ISODateTimeType(""),
            ),
        ]
    )
    assert await ctxt.pull_request.get_commit_message(
        "{{ title | striptags }} (#{{ number }})\n\n{{ body | get_section('### Description') }}\n\n"
        "{{ head | markdownify }}\n\ncommits:\n\n{% for c in commits %}* {{ c }}\n{% endfor %}",
    ) == (expected_title, expected_body)


async def test_context_unexisting_section(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert (
        await ctxt.pull_request.get_commit_message(
            "{{ body | get_section('### Description', '') }}",
        )
        is None
    )


async def test_context_unexisting_section_with_templated_default(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert await ctxt.pull_request.get_commit_message(
        "{{ body | get_section('### Description', '{{number}}\n{{author}}') }}",
    ) == (str(a_pull_request["number"]), a_pull_request["user"]["login"])


async def test_context_body_section_with_template(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request[
        "body"
    ] = """

Yo!

### Commit

BODY OF #{{number}}

"""
    ctxt = context.Context(mock.Mock(), a_pull_request)
    assert await ctxt.pull_request.get_commit_message(
        "TITLE\n{{ body | get_section('### Commit') }}",
    ) == ("TITLE", f"BODY OF #{a_pull_request['number']}")


async def test_context_body_section_with_bad_template(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request[
        "body"
    ] = """
Description
---

Test Plan
---

Instructions
---

"""
    ctxt = context.Context(mock.Mock(), a_pull_request)
    with pytest.raises(context.RenderTemplateFailure):
        await ctxt.pull_request.get_commit_message(
            "TITLE\n{{ body | get_section('### Commit') }}",
        )


async def test_check_runs_ordering(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    repo = mock.Mock()
    repo.get_branch_protection.side_effect = mock.AsyncMock(return_value=None)
    repo.installation.client.items = mock.MagicMock(__aiter__=[])
    ctxt = context.Context(repo, a_pull_request)
    with mock.patch(
        "mergify_engine.check_api.get_checks_for_ref",
        return_value=[
            {
                "id": 6928770773,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "0ccccc7a-9630-5914-467b-15a9c61f0287",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "Publish release",
                "status": "completed",
                "output": {
                    "title": None,
                    "summary": "",
                    "text": None,
                    "annotations_count": 0,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "skipped",
                "completed_at": "2022-06-17T01:10:22Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928770773?check_suite_focus=true",
            },
            {
                "id": 6928212025,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "6614165d-8168-5766-9db6-f35f537f8e36",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "ci",
                "status": "completed",
                "output": {
                    "title": None,
                    "summary": "",
                    "text": None,
                    "annotations_count": 0,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "skipped",
                "completed_at": "2022-06-17T00:09:57Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928212025?check_suite_focus=true",
            },
            {
                "id": 6928210780,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "2a132eb7-5003-5af1-9f28-e7804a9048f4",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "mima",
                "status": "completed",
                "output": {
                    "title": None,
                    "summary": "",
                    "text": None,
                    "annotations_count": 0,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "success",
                "completed_at": "2022-06-17T00:23:06Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928210780?check_suite_focus=true",
            },
            {
                "id": 6928210606,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "af160194-4251-5b45-90f2-db2e90a93f16",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "ci (Native)",
                "status": "completed",
                "output": {
                    "title": None,
                    "summary": "",
                    "text": None,
                    "annotations_count": 0,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "success",
                "completed_at": "2022-06-17T01:10:20Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928210606?check_suite_focus=true",
            },
            {
                "id": 6928210520,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "ce3d09a6-c2ac-5c0d-cd25-ca5c547abb3a",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "ci (JS)",
                "status": "completed",
                "output": {
                    "title": None,
                    "summary": "",
                    "text": None,
                    "annotations_count": 0,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "success",
                "completed_at": "2022-06-17T00:44:12Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928210520?check_suite_focus=true",
            },
            {
                "id": 6928210396,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "980a8645-8e3e-5677-ba14-1ce4199013e8",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "ci (JVM)",
                "status": "completed",
                "output": {
                    "title": "ci (JVM)",
                    "summary": "",
                    "text": None,
                    "annotations_count": 1,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": "success",
                "completed_at": "2022-06-17T01:08:00Z",
                "html_url": "https://github.com/softwaremill/tapir/runs/6928210396?check_suite_focus=true",
            },
            {
                "id": 6928210396,
                "app_id": 15368,
                "app_name": "GitHub Actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/u/9919?v=4",
                "external_id": "980a8645-8e3e-5677-ba14-1ce4199013e8",
                "head_sha": "d47d59a4723567c294c84c29336e2777038e7e30",
                "name": "ci (JVM)",
                "status": "in_progress",
                "output": {
                    "title": "ci (JVM)",
                    "summary": "",
                    "text": None,
                    "annotations_count": 1,
                    "annotations_url": "",
                    "annotations": [],
                },
                "conclusion": None,
                "completed_at": None,
                "html_url": "https://github.com/softwaremill/tapir/runs/6928210396?check_suite_focus=true",
            },
        ],
    ):
        assert await ctxt.checks == {
            "Publish release": "skipped",
            "ci": "skipped",
            "ci (JS)": "success",
            "ci (JVM)": None,
            "ci (Native)": "success",
            "mima": "success",
        }


async def test_reviews_filtering(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    all_reviews = [
        github_types.GitHubReview(
            {
                "id": github_types.GitHubReviewIdType(123456),
                "user": a_pull_request["user"],
                "body": "",
                "pull_request": a_pull_request,
                "repository": a_pull_request["base"]["repo"],
                "state": "APPROVED",
                "author_association": "COLLABORATOR",
                "submitted_at": github_types.ISODateTimeType(
                    "2022-07-26T04:49:04.750767+00:00"
                ),
            }
        ),
        github_types.GitHubReview(
            {
                "id": github_types.GitHubReviewIdType(424242),
                "user": a_pull_request["user"],
                "body": "",
                "pull_request": a_pull_request,
                "repository": a_pull_request["base"]["repo"],
                "state": "CHANGES_REQUESTED",
                "author_association": "COLLABORATOR",
                "submitted_at": github_types.ISODateTimeType(
                    "2022-07-26T14:14:14.000000+00:00"
                ),
            }
        ),
        github_types.GitHubReview(
            {
                "id": github_types.GitHubReviewIdType(987654),
                "user": a_pull_request["user"],
                "body": "",
                "pull_request": a_pull_request,
                "repository": a_pull_request["base"]["repo"],
                "state": "APPROVED",
                "author_association": "COLLABORATOR",
                "submitted_at": github_types.ISODateTimeType(
                    "2022-07-26T22:42:24.000001+00:00"
                ),
            }
        ),
    ]

    async def fake_client_items(
        url: str,
        *,
        resource_name: str,
        page_limit: int,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        list_items: str | None = None,
        params: dict[str, str] | None = None,
    ) -> typing.Any:
        if url.endswith("/pulls/6/reviews"):
            for review in all_reviews:
                yield review
        else:
            raise RuntimeError(f"not mocked api call: {url}")

    repo = mock.Mock()
    repo.get_branch_protection.side_effect = mock.AsyncMock(return_value=None)
    repo.installation.client = mock.AsyncMock(items=fake_client_items)
    ctxt = context.Context(repo, a_pull_request)
    assert await ctxt.reviews == all_reviews

    ctxt = context.Context(repo, a_pull_request)
    ctxt.sources = [
        context.T_PayloadEventSource(
            {
                "event_type": "refresh",
                "data": github_types.GitHubEventRefresh(
                    {
                        "received_at": github_types.ISODateTimeType(
                            "2022-07-26T14:14:14.000000+00:00"
                        ),
                        "organization": a_pull_request["base"]["repo"]["owner"],
                        "installation": {
                            "id": github_types.GitHubInstallationIdType(123456),
                            "account": a_pull_request["base"]["repo"]["owner"],
                            "target_type": "User",
                            "permissions": {},
                            "suspended_at": None,
                        },
                        "sender": a_pull_request["user"],
                        "repository": a_pull_request["base"]["repo"],
                        "action": "user",
                        "ref": a_pull_request["base"]["ref"],
                        "pull_request_number": a_pull_request["number"],
                        "source": "internal",
                    }
                ),
                "timestamp": github_types.ISODateTimeType(
                    "2022-07-26T14:14:14.000000+00:00"
                ),
                "initial_score": 0,
            }
        ),
    ]
    # Drop review done after the refresh event.
    assert await ctxt.reviews == all_reviews[0:2]


@pytest.mark.parametrize(
    "commit_msg,dependabot_properties",
    [
        (
            """
            chore(deps-dev): bump bootstrap from 5.1.3 to 5.2.0 in /docs

            Bumps [bootstrap](https://github.com/twbs/bootstrap) from 5.1.3 to 5.2.0.
            - [Release notes](https://github.com/twbs/bootstrap/releases)
            - [Commits](https://github.com/twbs/bootstrap/compare/v5.1.3...v5.2.0)

            ---
            updated-dependencies:
            - dependency-name: bootstrap
              dependency-type: direct:development
              update-type: version-update:semver-minor
            ...

            Signed-off-by: dependabot[bot] <support@github.com>
            """,
            {
                "dependency-name": "bootstrap",
                "dependency-type": "direct:development",
                "update-type": "version-update:semver-minor",
            },
        ),
        (
            """
            chore(deps): bump terser from 5.10.0 to 5.10.2 in /installer

            Bumps [terser](https://github.com/terser/terser) from 5.10.0 to 5.14.2.
            - [Release notes](https://github.com/terser/terser/releases)
            - [Changelog](https://github.com/terser/terser/blob/master/CHANGELOG.md)
            - [Commits](https://github.com/terser/terser/commits)

            ---
            updated-dependencies:
            - dependency-name: terser
              dependency-type: indirect
            ...

            Signed-off-by: dependabot[bot] <support@github.com>
            """,
            {
                "dependency-name": "terser",
                "dependency-type": "indirect",
            },
        ),
    ],
)
async def test_dependabot_attributes_parsing(
    commit_msg: str, dependabot_properties: dependabot_types.DependabotAttributes
) -> None:
    res = dependabot_helpers.get_dependabot_consolidated_data_from_commit_msg(
        mock.Mock(), commit_msg
    )
    assert res == dependabot_properties


@pytest.mark.parametrize(
    "commit_msg",
    [
        "",
        """
        chore(deps-dev): bump bootstrap from 5.1.3 to 5.2.0 in /docs

        Bumps [bootstrap](https://github.com/twbs/bootstrap) from 5.1.3 to 5.2.0.
        - Signed-off-by: dependabot[bot] <support@github.com>",
                "chore(deps-dev): bump bootstrap from 5.1.3 to 5.2.0 in /docs

        Bumps [bootstrap](https://github.com/twbs/bootstrap) from 5.1.3 to 5.2.0.
        - [Release notes](https://github.com/twbs/bootstrap/releases)
        - [Commits](https://github.com/twbs/bootstrap/compare/v5.1.3...v5.2.0)

        ---
        ...

        Signed-off-by: dependabot[bot] <support@github.com>
        """,
        """chore(deps): bump terser from 5.10.0 to 5.10.2 in /installer

        Bumps [terser](https://github.com/terser/terser) from 5.10.0 to 5.14.2.
        - [Release notes](https://github.com/terser/terser/releases)
        - [Changelog](https://github.com/terser/terser/blob/master/CHANGELOG.md)
        - [Commits](https://github.com/terser/terser/commits)

        ---
        updated-dependencies:
        - dependency-name: terser
          dependency-type: indirect
        - dependency-name: terser2
          dependency-type: indirect
        ...

        Signed-off-by: dependabot[bot] <support@github.com>
        """,
        """chore(deps): bump terser from 5.10.0 to 5.10.2 in /installer

        Bumps [terser](https://github.com/terser/terser) from 5.10.0 to 5.14.2.
        - [Release notes](https://github.com/terser/terser/releases)
        - [Changelog](https://github.com/terser/terser/blob/master/CHANGELOG.md)
        - [Commits](https://github.com/terser/terser/commits)

        ---
        updated-dependencies:
        - invalid-key: terser
          dependency-type: indirect
        ...

        Signed-off-by: dependabot[bot] <support@github.com>""",
    ],
)
async def test_dependabot_attributes_parsing_ko(commit_msg: str) -> None:
    res = dependabot_helpers.get_dependabot_consolidated_data_from_commit_msg(
        mock.Mock(), commit_msg
    )
    assert res is None


async def test_template_with_mandatory_variables(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request["body"] = "Such a test."
    ctxt = context.Context(mock.Mock(), a_pull_request)
    output = await ctxt.pull_request.render_template(
        "{{ body }}",
        extra_variables={"greeting": "Hello"},
        mandatory_template_variables={"greeting": "\n{{ greeting }} world."},
    )
    assert output == "Such a test.\nHello world."


async def test_commit_details_from_attributes(
    a_pull_request: github_types.GitHubPullRequest,
) -> None:
    a_pull_request[
        "body"
    ] = """
Yo!
### Commits:

# COMMIT LIST:
{% for commit in commits %}
    - {{ commit.author }}
    - {{ commit.date_author }}
    - {{ commit }}
    - {{ commit.committer }}
    - {{ commit.date_committer }}
{% endfor %}
"""
    commits = [
        github_types.CachedGitHubBranchCommit(
            sha=github_types.SHAType("6666bbbb"),
            commit_message="first commit to do something",
            commit_verification_verified=True,
            parents=[github_types.SHAType("6666aaaa")],
            author="someone",
            committer="someone 2",
            email_author="",
            email_committer="",
            date_author=github_types.ISODateTimeType("2012-04-14T16:00:49Z"),
            date_committer=github_types.ISODateTimeType("2013-04-14T16:00:49Z"),
        ),
        github_types.CachedGitHubBranchCommit(
            sha=github_types.SHAType("7777bbbb"),
            commit_message="second commit to do something",
            commit_verification_verified=True,
            parents=[github_types.SHAType("7777aaaa")],
            author="someone-else",
            committer="someone-else 2",
            email_author="",
            email_committer="",
            date_author=github_types.ISODateTimeType("2013-04-14T16:00:49Z"),
            date_committer=github_types.ISODateTimeType("2014-04-14T16:00:49Z"),
        ),
        github_types.CachedGitHubBranchCommit(
            sha=github_types.SHAType("8888bbbb"),
            commit_message="third commit to do something",
            commit_verification_verified=True,
            parents=[github_types.SHAType("8888aaaa")],
            author="another-someone",
            committer="another-someone 2",
            email_author="",
            email_committer="",
            date_author=github_types.ISODateTimeType("2014-04-14T16:00:49Z"),
            date_committer=github_types.ISODateTimeType("2015-04-14T16:00:49Z"),
        ),
    ]
    ctxt = context.Context(mock.Mock(), a_pull_request)
    ctxt._caches.commits.set(commits)

    template = await ctxt.pull_request.render_template(a_pull_request["body"])  # type: ignore[arg-type]
    assert (
        template
        == """
Yo!
### Commits:

# COMMIT LIST:

    - someone
    - 2012-04-14T16:00:49Z
    - first commit to do something
    - someone 2
    - 2013-04-14T16:00:49Z

    - someone-else
    - 2013-04-14T16:00:49Z
    - second commit to do something
    - someone-else 2
    - 2014-04-14T16:00:49Z

    - another-someone
    - 2014-04-14T16:00:49Z
    - third commit to do something
    - another-someone 2
    - 2015-04-14T16:00:49Z
"""
    )
