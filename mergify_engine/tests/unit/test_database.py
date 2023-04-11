import asyncio
import filecmp
import pathlib
import subprocess
from unittest import mock

import alembic.command
import alembic.config

from mergify_engine import database
from mergify_engine import settings
from mergify_engine.models import manage
from mergify_engine.tests import utils


def test_migration(tmp_path: pathlib.Path) -> None:
    # We need to manually run the coroutine in an event loop because
    # pytest-asyncio has its own `event_loop` fixture that is function scoped
    # and in autouse (session scope fixture cannot require function scoped
    # fixture)
    loop = asyncio.get_event_loop_policy().new_event_loop()

    # Create database objects with SQLAlchemy
    url, url_without_db_name = utils.create_database_url("test_migration_create")
    loop.run_until_complete(
        utils.create_database(url_without_db_name.geturl(), "test_migration_create")
    )
    with mock.patch.object(settings, "DATABASE_URL", url):
        database.init_sqlalchemy("test_migration_create")
        loop.run_until_complete(manage.create_all())

        schema_dump_creation_path = tmp_path / "test_migration_create.sql"
        dump_schema(schema_dump_creation_path)

        loop.run_until_complete(dispose_engine())

    # Create database objects with Alembic
    url, url_without_db_name = utils.create_database_url("test_migration_migrate")
    loop.run_until_complete(
        utils.create_database(url_without_db_name.geturl(), "test_migration_migrate")
    )

    with mock.patch.object(settings, "DATABASE_URL", url):
        config = alembic.config.Config("alembic.ini")
        alembic.command.upgrade(config, "head")

        schema_dump_migration_path = tmp_path / "test_migration_migrate.sql"
        dump_schema(schema_dump_migration_path)

        loop.run_until_complete(dispose_engine())

    assert filecmp.cmp(
        schema_dump_creation_path, schema_dump_migration_path, shallow=False
    ), filediff(schema_dump_creation_path, schema_dump_migration_path)

    loop.close()


def dump_schema(filepath: pathlib.Path) -> None:
    db_url = settings.DATABASE_URL.geturl().replace("postgresql+psycopg", "postgresql")
    subprocess.run(
        (
            "pg_dump",
            f"--dbname={db_url}",
            "--schema-only",
            "--exclude-table=alembic_version",
            "--format=p",
            "--encoding=UTF8",
            "--file",
            filepath,
        ),
        check=True,
    )


async def dispose_engine() -> None:
    if database.APP_STATE is not None:
        await database.APP_STATE["engine"].dispose()
        database.APP_STATE = None


def filediff(path1: pathlib.Path, path2: pathlib.Path) -> str | None:
    with path1.open() as f1, path2.open() as f2:
        for i, (l1, l2) in enumerate(zip(f1, f2, strict=True)):
            if l1 != l2:
                return f'Difference at line {i+1}: "{l1.strip()}" != "{l2.strip()}"'
    return None
