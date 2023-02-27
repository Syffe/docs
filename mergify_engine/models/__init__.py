from collections import abc
import typing
from urllib import parse

from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import config


class Base(orm.DeclarativeBase):
    __allow_unmapped__ = True
    __mapper_args__ = {"eager_defaults": True}


def get_async_database_url() -> str:
    parsed = parse.urlparse(config.DATABASE_URL)
    if parsed.scheme.startswith("postgres"):
        parsed = parsed._replace(scheme="postgresql+psycopg")
    return parse.urlunparse(parsed)


AsyncSessionMaker = typing.NewType(
    "AsyncSessionMaker",
    "sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]",
)


class SQLAlchemyAppState(typing.TypedDict):
    sessionmaker: AsyncSessionMaker
    engine: sqlalchemy.ext.asyncio.AsyncEngine


APP_STATE: SQLAlchemyAppState | None = None


def init_sqlalchemy() -> None:
    global APP_STATE

    if APP_STATE is not None:
        raise RuntimeError("APP_STATE already initialized")

    # NOTE(sileht): Pool need to be adjusted with number of fastapi concurrent requests
    # the number of dyno and the Heroku postgres plan.
    # Current setup:
    # * one dyno
    # * postgres standard/premium 0 plan that allows 120 connections max
    async_engine = sqlalchemy.ext.asyncio.create_async_engine(
        get_async_database_url(),
        pool_size=100,
        # Ensure old pooled connection still works
        pool_pre_ping=True,
    )
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
