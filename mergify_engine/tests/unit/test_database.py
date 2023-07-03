import asyncio
import dataclasses
import filecmp
import os
import pathlib
import re
import subprocess

from mergify_engine import settings
from mergify_engine.config import types
from mergify_engine.models import manage
from mergify_engine.tests import utils


@dataclasses.dataclass
class DatabaseServerVersionMismatchError(Exception):
    dump_error_message: dataclasses.InitVar[str]
    server_version: str = dataclasses.field(init=False)
    local_version: str = dataclasses.field(init=False)

    def __post_init__(self, error_message: str) -> None:
        self.server_version = re.search(  # type: ignore [union-attr]
            r"server version: (\d+.\d+)", error_message
        ).group(1)
        self.local_version = re.search(  # type: ignore [union-attr]
            r"pg_dump version: (\d+.\d+)", error_message
        ).group(1)

    def __str__(self) -> str:
        return f"pg_dump is not up to date (server version={self.server_version}, local_version={self.local_version})"


def _run_alembic(*args: str, environ: dict[str, str] | None = None) -> str:
    # NOTE(sileht): we must run alembic in separate process to ensure the already loaded sqlalchemy
    # resource does not interfer with the migration scripts
    if environ is None:
        environ = os.environ.copy()

    config = manage.load_alembic_config()
    assert config.config_file_name is not None
    p = subprocess.run(
        ("alembic", "-c", config.config_file_name, *args),
        env=environ,
        check=True,
        capture_output=True,
    )
    return p.stdout.decode()


def _run_migration_scripts(url: types.PostgresDSN) -> None:
    # NOTE(sileht): we must run alembic in separate process to ensure the already loaded sqlalchemy
    # resource does not interfer with the migration scripts
    environ = os.environ.copy()
    environ["MERGIFYENGINE_DATABASE_URL"] = url.geturl()
    environ["MERGIFYENGINE_LOG_STDOUT"] = "false"

    output = _run_alembic("history", environ=environ)
    scripts_count = len(output.splitlines())
    for _ in range(scripts_count):
        subprocess.run(
            ("mergify-database-update", "+1"),
            env=environ,
            check=True,
            capture_output=True,
        )
    _run_alembic("check", environ=environ)


def test_migration(setup_database: None, tmp_path: pathlib.Path) -> None:
    # We need to manually run the coroutine in an event loop because
    # pytest-asyncio has its own `event_loop` fixture that is function scoped
    # and in autouse (session scope fixture cannot require function scoped
    # fixture)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    schema_dump_creation_path = tmp_path / "test_migration_create.sql"
    dump_schema(settings.DATABASE_URL, schema_dump_creation_path)

    if os.getenv("MIGRATED_DATA_DUMP") is None:
        url_migrate, url_migrate_without_db_name = utils.create_database_url(
            "test_migration_migrate"
        )
        loop.run_until_complete(
            utils.create_database(
                url_migrate_without_db_name.geturl(), "test_migration_migrate"
            )
        )
        _run_migration_scripts(url_migrate)
        schema_dump_migration_path = tmp_path / "test_migration_migrate.sql"
        dump_schema(url_migrate, schema_dump_migration_path)
    else:
        schema_dump_migration_path = pathlib.Path(os.environ["MIGRATED_DATA_DUMP"])

    for _file in (schema_dump_creation_path, schema_dump_migration_path):
        # nosemgrep: python.lang.security.audit.subprocess-shell-true.subprocess-shell-true
        subprocess.run(
            "sed -i"
            " -e '/^--/d'"  # remove comments
            " -e '/^$/d'"  # remove empty lines
            " -e '/^CREATE EXTENSION/d' -e '/^COMMENT ON EXTENSION/d'"  # remove heroku extensions
            " -e 's/public\\.//g'"  # remove schema prefix
            f" {_file}",
            shell=True,
            check=True,
        )

    assert filecmp.cmp(
        schema_dump_creation_path, schema_dump_migration_path, shallow=False
    ), filediff(schema_dump_creation_path, schema_dump_migration_path)

    loop.close()


def dump_schema(url: types.PostgresDSN, filepath: pathlib.Path) -> None:
    db_url = url.geturl().replace("postgresql+psycopg", "postgresql")

    try:
        subprocess.run(
            (
                "pg_dump",
                "--no-acl",
                "--no-owner",
                "--no-comments",
                f"--dbname={db_url}",
                "--exclude-schema=heroku_ext",
                "--schema-only",
                "--exclude-table=alembic_version",
                "--format=p",
                "--encoding=UTF8",
                "--file",
                filepath,
            ),
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        error_message = e.stderr.decode()
        if "aborting because of server version mismatch" in error_message:
            raise DatabaseServerVersionMismatchError(error_message)

        raise


def filediff(path1: pathlib.Path, path2: pathlib.Path) -> str | None:
    with path1.open() as f1, path2.open() as f2:
        for i, (l1, l2) in enumerate(zip(f1, f2, strict=True)):
            if l1 != l2:
                return f'Difference at line {i+1}: "{l1.strip()}" != "{l2.strip()}"'
    return None


def test_one_head() -> None:
    output = _run_alembic("heads")
    assert "(head)" in output
    heads = output.splitlines()
    assert (
        len(heads) == 1
    ), f"One head revision allowed, {len(heads)} found: {', '.join(heads)}"
