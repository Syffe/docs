import asyncio
import subprocess

from alembic import context
from alembic.script import write_hooks
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import models
from mergify_engine import service


async def run_async_migrations() -> None:
    service.setup("db-migration")

    async with database.get_engine().connect() as connection:
        await connection.run_sync(do_run_migrations)


def do_run_migrations(connection: sqlalchemy.Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=models.Base.metadata,
        alembic_module_prefix="alembic.op.",
        sqlalchemy_module_prefix="sqlalchemy.",
    )

    with context.begin_transaction():
        context.run_migrations()


asyncio.run(run_async_migrations())


@write_hooks.register("ruff")
def ruff_hook(filename: str, options: dict[str, str | int]) -> None:
    subprocess.run(["ruff", "check", "--fix", filename])
