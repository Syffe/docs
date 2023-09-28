import argparse
from collections import abc
import importlib.resources  # nosemgrep: python.lang.compatibility.python37.python37-compatibility-importlib2
import typing

import alembic.command
import alembic.config
from alembic_utils import replaceable_entity
import sqlalchemy

from mergify_engine import database
from mergify_engine import models


async def handle_pg_entities(
    conn: sqlalchemy.ext.asyncio.AsyncConnection,
    action: typing.Literal["create", "drop"],
    postgres_entities: abc.Sequence[replaceable_entity.ReplaceableEntity],
) -> None:
    if action == "drop":
        postgres_entities = list(reversed(postgres_entities))

    for postgres_entity in postgres_entities:
        await conn.execute(getattr(postgres_entity, f"to_sql_statement_{action}")())


async def create_all() -> None:
    async with database.get_engine().begin() as conn:
        await handle_pg_entities(conn, "create", models.Base.__postgres_extensions__)
        await conn.run_sync(models.Base.metadata.create_all)
        await handle_pg_entities(conn, "create", models.Base.get_postgres_entities())


async def drop_all() -> None:
    async with database.get_engine().begin() as conn:
        await handle_pg_entities(conn, "drop", models.Base.get_postgres_entities())
        await conn.run_sync(models.Base.metadata.drop_all)
        await handle_pg_entities(conn, "drop", models.Base.__postgres_extensions__)


def load_alembic_config() -> alembic.config.Config:
    config_file = importlib.resources.files(__package__).joinpath(
        "db_migration/alembic.ini"
    )
    return alembic.config.Config(str(config_file))


def database_update(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Update Mergify database schema")
    parser.add_argument("revision", default="head", nargs="?")
    args = parser.parse_args(argv)
    config = load_alembic_config()
    alembic.command.upgrade(config, args.revision)
