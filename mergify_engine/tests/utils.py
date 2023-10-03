from __future__ import annotations

import asyncio
import os
import pathlib
import subprocess
import threading
import typing
from unittest import mock

import click.testing
import respx
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.config import types
from mergify_engine.models import github as gh_models
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.tests import conftest
from mergify_engine.tests.db_populator import DbPopulator


async def load_mergify_config(content: str) -> mergify_conf.MergifyConfig:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=content,
    )

    return await mergify_conf.get_mergify_config_from_file(mock.MagicMock(), file)


def create_database_url(db_name: str) -> tuple[types.PostgresDSN, types.PostgresDSN]:
    mocked_url = settings.DATABASE_URL._replace(path=f"/{db_name}")
    mocked_url_without_db_name = settings.DATABASE_URL._replace(path="")
    return mocked_url, mocked_url_without_db_name


async def create_database(db_url: str, db_name: str) -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(db_url)
    try:
        engine_no_transaction = engine.execution_options(isolation_level="AUTOCOMMIT")
        async with engine_no_transaction.connect() as conn:
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            await conn.execute(sqlalchemy.text(f"DROP DATABASE IF EXISTS {db_name}"))
            # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            await conn.execute(sqlalchemy.text(f"CREATE DATABASE {db_name}"))
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
    *args: typing.Any, **kwargs: typing.Any
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


def add_workflow_job(
    session: sqlalchemy.ext.asyncio.AsyncSession,
    job_data: dict[str, typing.Any],
) -> gh_models.WorkflowJob:
    job = gh_models.WorkflowJob(
        id=job_data["id"],
        repository=job_data["repository"],
        log_embedding=job_data.get("log_embedding"),
        log_status=job_data.get("log_status"),
        embedded_log=job_data.get("embedded_log"),
        workflow_run_id=job_data.get("workflow_run_id", 1),
        name=job_data.get("name", "job_name"),
        started_at=job_data.get(
            "started_at",
            github_types.ISODateTimeType(date.utcnow().isoformat()),
        ),
        completed_at=job_data.get(
            "completed_at",
            github_types.ISODateTimeType(date.utcnow().isoformat()),
        ),
        conclusion=job_data.get("conclusion", gh_models.WorkflowJobConclusion.SUCCESS),
        labels=job_data.get("labels", []),
        run_attempt=job_data.get("run_attempt", 1),
        failed_step_name=job_data.get("failed_step_name"),
        failed_step_number=job_data.get("failed_step_number"),
        steps=job_data.get("steps"),
    )
    session.add(job)
    return job


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
        f"https://api.github.com/users/{repo['owner']['login']}/installation"
    ).respond(200, json={"account": repo["owner"]})
    respx_mock.get(
        f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}"
    ).respond(
        200, json=repo  # type: ignore[arg-type]
    )
    respx_mock.get(
        f"http://localhost:5000/engine/subscription/{repo['owner']['id']}"
    ).respond(
        200,
        json={
            "subscription_active": True,
            "subscription_reason": "",
            "features": [feature.value for feature in subscription.Features],
        },
    )

    respx_mock.get(
        f"https://api.github.com/repos/{repo['owner']['login']}/{repo['name']}/collaborators/{user.login}/permission"
    ).respond(
        200,
        json=github_types.GitHubRepositoryCollaboratorPermission(  # type: ignore[arg-type]
            {
                "user": repo["owner"],
                "permission": permission.value,
            }
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
                    .limit(1)
                )
            ).scalar_one()
        ).as_github_dict(),
    )

    user = await mock_user_authorization_on_repo(respx_mock, repo_info, session)

    await web_client.log_as(user.id)
