import asyncio
import filecmp
import io
import os
import pathlib
import subprocess
from unittest import mock
import warnings

import alembic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
from mergify_engine import models
from mergify_engine import settings
from mergify_engine.config import types as config_types
from mergify_engine.models import manage
from mergify_engine.tests import utils


def _run_alembic(command: str, *args: str) -> str:
    config = manage.load_alembic_config()
    config.stdout = io.StringIO()

    APP_STATE = database.APP_STATE
    database.APP_STATE = None
    try:
        with mock.patch.object(settings, "LOG_STDOUT", command == "check"):
            getattr(alembic.command, command)(config, *args)
    finally:
        database.APP_STATE = APP_STATE
    return config.stdout.getvalue()


def _run_migration_scripts(url: config_types.PostgresDSN) -> None:
    with mock.patch.object(settings, "DATABASE_URL", url):
        output = _run_alembic("history")
        scripts_count = len(output.splitlines())
        for _ in range(scripts_count):
            _run_alembic("upgrade", "+1")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sqlalchemy.exc.SAWarning)
            _run_alembic("check")


def test_migration(setup_database: None, tmp_path: pathlib.Path) -> None:
    # We need to manually run the coroutine in an event loop because
    # pytest-asyncio has its own `event_loop` fixture that is function scoped
    # and in autouse (session scope fixture cannot require function scoped
    # fixture)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    schema_dump_creation_path = tmp_path / "test_migration_create.sql"
    utils.dump_schema(settings.DATABASE_URL.path[1:], schema_dump_creation_path)

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
        utils.dump_schema("test_migration_migrate", schema_dump_migration_path)
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
            timeout=10,
        )

    assert filecmp.cmp(
        schema_dump_creation_path, schema_dump_migration_path, shallow=False
    ), filediff(schema_dump_creation_path, schema_dump_migration_path)

    loop.close()


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


def test_model_as_dict() -> None:
    class TestSimpleModel(models.Base):
        __tablename__ = "test_simple_table"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]

    obj = TestSimpleModel(id=0)
    assert obj.as_dict() == {"id": 0, "name": None}  # type: ignore [comparison-overlap]

    obj = TestSimpleModel(id=0, name="hello")
    assert obj.as_dict() == {"id": 0, "name": "hello"}  # type: ignore [comparison-overlap]


def test_relational_model_as_dict() -> None:
    class TestRelationalUserModel(models.Base):
        __tablename__ = "test_relational_user_table"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]

    class TestRelationalModel(models.Base):
        __tablename__ = "test_relational_table"
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]
        user_id: orm.Mapped[int] = orm.mapped_column(
            sqlalchemy.ForeignKey("test_relational_user_table.id")
        )
        user: orm.Mapped[TestRelationalUserModel] = orm.relationship(
            lazy="joined", foreign_keys=[user_id]
        )

    obj = TestRelationalModel(id=0)
    assert obj.as_dict() == {"id": 0, "name": None, "user_id": None}  # type: ignore [comparison-overlap]

    obj = TestRelationalModel(
        id=0, name="hello", user_id=0, user=TestRelationalUserModel(id=0, name="me")
    )
    assert obj.as_dict() == {  # type: ignore [comparison-overlap]
        "id": 0,
        "name": "hello",
        "user_id": 0,
        "user": {"id": 0, "name": "me"},
    }
