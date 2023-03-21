import asyncio

import sqlalchemy

from mergify_engine import database
from mergify_engine import models

# NOTE(sileht): ensure all models are loaded, to
# allow create_all() to find all tables to creates
from mergify_engine.models import github_actions  # noqa
from mergify_engine.models import github_user  # noqa


async def create_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(
        database.get_async_database_url()
    )
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)


async def drop_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(
        database.get_async_database_url()
    )
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all)


def database_update() -> None:
    asyncio.run(create_all())
