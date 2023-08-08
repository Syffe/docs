import asyncio
import os
import pathlib
import subprocess
import threading
import typing
from unittest import mock

import click.testing
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.rules.config import mergify as mergify_conf


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
