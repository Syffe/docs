import argparse
import importlib.resources  # nosemgrep: python.lang.compatibility.python37.python37-compatibility-importlib2

import alembic.command
import alembic.config
import sqlalchemy

from mergify_engine import models
from mergify_engine import settings


async def create_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(settings.DATABASE_URL.geturl())
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)


async def drop_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(settings.DATABASE_URL.geturl())
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all)


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
