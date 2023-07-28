import asyncio
import subprocess

from alembic import context
from alembic.script import write_hooks
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import logs
from mergify_engine import models


async def run_async_migrations() -> None:
    logs.setup_logging(dump_config=False)
    database.init_sqlalchemy("db-migration")

    engine = database.get_engine()
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


def do_run_migrations(connection: sqlalchemy.Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=models.Base.metadata,
        alembic_module_prefix="alembic.op.",
        sqlalchemy_module_prefix="sqlalchemy.",
        transactional_ddl=True,
        transaction_per_migration=True,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


asyncio.run(run_async_migrations())


@write_hooks.register("ruff")
def ruff_hook(filename: str, options: dict[str, str | int]) -> None:
    subprocess.run(["ruff", "check", "--fix", filename])
