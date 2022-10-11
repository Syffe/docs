from collections import abc
import typing
from urllib import parse

from sqlalchemy import orm
import sqlalchemy.ext
import sqlalchemy.ext.asyncio

from mergify_engine import config


mapper_registry = orm.registry()


class Base(metaclass=orm.decl_api.DeclarativeMeta):
    __abstract__ = True

    registry = mapper_registry
    metadata = mapper_registry.metadata
    __init__ = mapper_registry.constructor


def get_async_database_url() -> str:
    parsed = parse.urlparse(config.DATABASE_URL)
    if parsed.scheme.startswith("postgres"):
        parsed = parsed._replace(scheme="postgresql+asyncpg")
    return parse.urlunparse(parsed)


AsyncSessionMaker = typing.NewType(
    "AsyncSessionMaker", "orm.sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]"
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
    # TODO(sileht): We may need to be able to configure a different pool size
    # depending of the service (fastapi vs synack)
    async_engine = sqlalchemy.ext.asyncio.create_async_engine(
        get_async_database_url(), pool_size=16
    )
    APP_STATE = SQLAlchemyAppState(
        {
            "engine": async_engine,
            "sessionmaker": AsyncSessionMaker(
                orm.sessionmaker(
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
