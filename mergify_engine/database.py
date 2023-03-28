from collections import abc
import typing

import ddtrace
import fastapi
import sqlalchemy.ext.asyncio

from mergify_engine import database
from mergify_engine import settings


AsyncSessionMaker = typing.NewType(
    "AsyncSessionMaker",
    "sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]",
)


class SQLAlchemyAppState(typing.TypedDict):
    sessionmaker: AsyncSessionMaker
    engine: sqlalchemy.ext.asyncio.AsyncEngine


APP_STATE: SQLAlchemyAppState | None = None


def init_sqlalchemy(service_name: str) -> None:
    global APP_STATE

    if APP_STATE is not None:
        raise RuntimeError("APP_STATE already initialized")

    pool_size = settings.DATABASE_POOL_SIZES.get(service_name, 10)

    # NOTE(sileht): Pool need to be adjusted with number of fastapi concurrent requests
    # the number of dyno and the Heroku postgres plan.
    # Current setup:
    # * one dyno
    # * postgres standard/premium 0 plan that allows 120 connections max
    async_engine = sqlalchemy.ext.asyncio.create_async_engine(
        settings.DATABASE_URL.geturl(),
        pool_size=pool_size,
        max_overflow=-1,
        # Ensure old pooled connection still works
        pool_pre_ping=True,
    )
    ddtrace.Pin.override(async_engine.sync_engine, service="engine-db")

    APP_STATE = SQLAlchemyAppState(
        {
            "engine": async_engine,
            "sessionmaker": AsyncSessionMaker(
                sqlalchemy.ext.asyncio.async_sessionmaker(
                    async_engine,
                    expire_on_commit=False,
                    class_=sqlalchemy.ext.asyncio.AsyncSession,
                )
            ),
        }
    )


def _get_app() -> SQLAlchemyAppState:
    global APP_STATE
    if APP_STATE is None:
        raise RuntimeError("APP_STATE not initialized")
    return APP_STATE


def create_session() -> sqlalchemy.ext.asyncio.AsyncSession:
    return _get_app()["sessionmaker"]()


async def get_session() -> abc.AsyncGenerator[
    sqlalchemy.ext.asyncio.AsyncSession, None
]:
    async with create_session() as session:
        yield session


Session = typing.Annotated[
    sqlalchemy.ext.asyncio.AsyncSession, fastapi.Depends(database.get_session)
]
