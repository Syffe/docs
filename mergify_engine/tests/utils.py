from __future__ import annotations

import asyncio
import os
import subprocess
import threading
import typing
from unittest import mock

import anys
import click.testing
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.models import github as gh_models
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.tests.db_populator import DbPopulator


if typing.TYPE_CHECKING:
    import pathlib

    import respx

    from mergify_engine.config import types
    from mergify_engine.tests import conftest

ANY_UUID4 = anys.AnyMatch(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
)


async def load_mergify_config(content: str) -> mergify_conf.MergifyConfig:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=content,
        encoding="base64",
    )

    return await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)


def create_database_url(db_name: str) -> tuple[types.PostgresDSN, types.PostgresDSN]:
    mocked_url = settings.DATABASE_URL._replace(path=f"/{db_name}")
    mocked_url_without_db_name = settings.DATABASE_URL._replace(path="")
    return mocked_url, mocked_url_without_db_name


async def create_database(
    db_url: str,
    db_name: str,
    template: str | None = None,
) -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(db_url)
    try:
        engine_no_transaction = engine.execution_options(isolation_level="AUTOCOMMIT")
        async with engine_no_transaction.connect() as conn:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            await conn.execute(sqlalchemy.text(f"DROP DATABASE IF EXISTS {db_name}"))
            template_cmd = ""
            if template is not None:
                template_cmd = f" TEMPLATE {template}"
            await conn.execute(
                # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
                sqlalchemy.text(f"CREATE DATABASE {db_name}{template_cmd}"),
            )
    finally:
        await engine.dispose()


def dump_schema(dbname: str, filepath: pathlib.Path) -> None:
    pg_dump_cmd = [
        "pg_dump",
        "--no-acl",
        "--no-owner",
        "--no-comments",
        f"--dbname={dbname}",
        "--user=postgres",
        "--exclude-schema=heroku_ext",
        "--schema-only",
        "--exclude-table=alembic_version",
        "--format=p",
        "--encoding=UTF8",
    ]
    if os.environ.get("CI") == "true":
        docker_cmd = ["docker", "exec", "postgres"]
    else:
        docker_cmd = ["docker", "compose", "exec", "postgres"]

    process = subprocess.run(
        [*docker_cmd, *pg_dump_cmd],
        check=True,
        capture_output=True,
        timeout=10,
    )

    with open(filepath, "w") as f:
        f.write(process.stdout.decode())


def test_console_scripts(
    *args: typing.Any,
    **kwargs: typing.Any,
) -> click.testing.Result:
    saved_state = database.APP_STATE
    database.APP_STATE = None
    result = None
    try:

        def task() -> None:
            nonlocal result
            asyncio.set_event_loop(asyncio.new_event_loop())
            runner = click.testing.CliRunner()
            result = runner.invoke(*args, **kwargs)

        thread = threading.Thread(target=task)
        thread.start()
        thread.join()
        assert result is not None
        return result
    finally:
        database.APP_STATE = saved_state


async def mock_user_authorization_on_repo(
    respx_mock: respx.MockRouter,
    repo: github_types.GitHubRepository,
    db: sqlalchemy.ext.asyncio.AsyncSession | None = None,
    user: gh_models.GitHubUser | None = None,
    permission: github_types.GitHubRepositoryPermission = github_types.GitHubRepositoryPermission.WRITE,
) -> gh_models.GitHubUser:
    if user is None:
        if db is None:
            raise RuntimeError("If user is not provided, db must be set")
        user = gh_models.GitHubUser(
            id=DbPopulator.next_id(gh_models.GitHubUser),
            login=github_types.GitHubLogin("user_login"),
            oauth_access_token=github_types.GitHubOAuthToken("user-token"),
        )
        db.add(user)
        await db.commit()

    respx_mock.get(
        f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/installation",
    ).respond(200, json={"account": repo["owner"], "suspended_at": None})
    respx_mock.get(
        f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}",
    ).respond(
        200,
        json=repo,  # type: ignore[arg-type]
    )
    respx_mock.get(
        f"http://localhost:5000/engine/subscription/{repo['owner']['id']}",
    ).respond(
        200,
        json={
            "subscription_active": True,
            "subscription_reason": "",
            "features": [feature.value for feature in subscription.Features],
        },
    )

    respx_mock.get(
        f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/collaborators/{user.login}/permission",
    ).respond(
        200,
        json=github_types.GitHubRepositoryCollaboratorPermission(  # type: ignore[arg-type]
            {
                "user": repo["owner"],
                "permission": permission.value,
            },
        ),
    )

    return user


async def configure_web_client_to_work_with_a_repo(
    respx_mock: respx.MockRouter,
    session: sqlalchemy.ext.asyncio.AsyncSession,
    web_client: conftest.CustomTestClient,
    repo_full_name: str,
) -> None:
    repo_info = typing.cast(
        github_types.GitHubRepository,
        (
            (
                await session.execute(
                    sqlalchemy.select(gh_models.GitHubRepository)
                    .where(gh_models.GitHubRepository.full_name == repo_full_name)
                    .limit(1),
                )
            ).scalar_one()
        ).as_github_dict(),
    )

    user = await mock_user_authorization_on_repo(respx_mock, repo_info, session)

    await web_client.log_as(user.id)


def fake_full_pull_request(
    pull_id: github_types.GitHubPullRequestId,
    number: github_types.GitHubPullRequestNumber,
    repository: github_types.GitHubRepository,
    **kwargs: typing.Any,
) -> github_types.GitHubPullRequest:
    pull_request_author = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(123),
            "type": "User",
            "login": github_types.GitHubLogin("contributor"),
            "avatar_url": "",
        },
    )

    pull: github_types.GitHubPullRequest = {
        "node_id": "42",
        "locked": False,
        "assignees": [],
        "requested_reviewers": [
            {
                "id": github_types.GitHubAccountIdType(123),
                "type": "User",
                "login": github_types.GitHubLogin("jd"),
                "avatar_url": "",
            },
            {
                "id": github_types.GitHubAccountIdType(456),
                "type": "User",
                "login": github_types.GitHubLogin("sileht"),
                "avatar_url": "",
            },
        ],
        "requested_teams": [
            {"slug": github_types.GitHubTeamSlug("foobar")},
            {"slug": github_types.GitHubTeamSlug("foobaz")},
        ],
        "milestone": None,
        "title": "awesome",
        "body": "",
        "created_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
        "closed_at": None,
        "updated_at": github_types.ISODateTimeType("2021-06-01T18:41:39Z"),
        "id": pull_id,
        "maintainer_can_modify": True,
        "user": pull_request_author,
        "labels": [],
        "rebaseable": True,
        "draft": False,
        "merge_commit_sha": None,
        "number": number,
        "commits": 1,
        "mergeable_state": "clean",
        "mergeable": True,
        "state": "open",
        "changed_files": 1,
        "head": {
            "sha": github_types.SHAType("the-head-sha"),
            "label": github_types.GitHubHeadBranchLabel(
                f"{pull_request_author['login']}:feature-branch",
            ),
            "ref": github_types.GitHubRefType("feature-branch"),
            "repo": {
                "id": github_types.GitHubRepositoryIdType(123),
                "default_branch": github_types.GitHubRefType("main"),
                "name": github_types.GitHubRepositoryName("mergify-engine"),
                "full_name": "contributor/mergify-engine",
                "archived": False,
                "private": False,
                "owner": pull_request_author,
                "url": "https://api.github.com/repos/contributor/mergify-engine",
                "html_url": "https://github.com/contributor/mergify-engine",
            },
            "user": pull_request_author,
        },
        "merged": False,
        "merged_by": None,
        "merged_at": None,
        "html_url": "https://...",
        "issue_url": "",
        "base": {
            "label": github_types.GitHubBaseBranchLabel("mergify_engine:main"),
            "ref": github_types.GitHubRefType("main"),
            "repo": repository,
            "sha": github_types.SHAType("the-base-sha"),
            "user": repository["owner"],
        },
    }
    pull.update(kwargs)  # type: ignore[typeddict-item]
    return pull
