import asyncio

import sqlalchemy

from mergify_engine import models
from mergify_engine import settings

# NOTE(sileht): ensure all models are loaded, to
# allow create_all() to find all tables to creates
from mergify_engine.models import github_actions  # noqa
from mergify_engine.models import github_user  # noqa


async def create_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(settings.DATABASE_URL.geturl())
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)


async def drop_all() -> None:
    engine = sqlalchemy.ext.asyncio.create_async_engine(settings.DATABASE_URL.geturl())
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all)


def database_update() -> None:
    asyncio.run(create_all())
