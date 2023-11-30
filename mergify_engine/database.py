from collections import abc
import dataclasses
import datetime
import typing

import ddtrace
import fastapi
import psycopg.errors
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.orm
import tenacity

from mergify_engine import settings


CLIENT_DATA_RETENTION_TIME: datetime.timedelta = datetime.timedelta(days=90)


AsyncSessionMaker = typing.NewType(
    "AsyncSessionMaker",
    "sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]",
)


class SQLAlchemyAppState(typing.TypedDict):
    sessionmaker: AsyncSessionMaker
    engine: sqlalchemy.ext.asyncio.AsyncEngine


APP_STATE: SQLAlchemyAppState | None = None


# NOTE(sileht): this is a fake dialect to be able to annotate column with anonymizer config
class Anonymizer(sqlalchemy.Dialect):
    pass


@dataclasses.dataclass
class CustomPostgresException(Exception):
    msg: typing.ClassVar[str]
    registry: typing.ClassVar[dict[str, type["CustomPostgresException"]]] = {}

    CUSTOM_PG_EXCEPTION_MARKER = "CUSTOM_PG_EXCEPTION"

    def __init_subclass__(cls, *args: typing.Any, **kwargs: typing.Any) -> None:
        if cls.__name__ in cls.registry:
            raise ValueError(
                f"A CustomPostgresException named '{cls.__name__}' has already been registered.",
            )
        cls.registry[cls.__name__] = cls
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    def to_pg_exception(cls) -> str:
        return f"EXCEPTION '{cls.CUSTOM_PG_EXCEPTION_MARKER}' USING DETAIL = '{cls.__name__}'"


def handle_custom_pg_exception(context: sqlalchemy.ExceptionContext) -> None:
    if (
        isinstance(context.original_exception, psycopg.errors.RaiseException)
        and context.original_exception.diag.message_primary
        == CustomPostgresException.CUSTOM_PG_EXCEPTION_MARKER
    ):
        class_name = context.original_exception.diag.message_detail
        if class_name is None:
            raise RuntimeError("CUSTOM_PG_EXCEPTION raised with DETAIL set")
        raise CustomPostgresException.registry[class_name]()


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

    sqlalchemy.event.listens_for(async_engine.sync_engine, "handle_error")(
        handle_custom_pg_exception,
    )

    APP_STATE = SQLAlchemyAppState(
        {
            "engine": async_engine,
            "sessionmaker": AsyncSessionMaker(
                sqlalchemy.ext.asyncio.async_sessionmaker(
                    async_engine,
                    expire_on_commit=False,
                    class_=sqlalchemy.ext.asyncio.AsyncSession,
                ),
            ),
        },
    )


def _get_app() -> SQLAlchemyAppState:
    global APP_STATE
    if APP_STATE is None:
        raise RuntimeError("APP_STATE not initialized")
    return APP_STATE


def get_engine() -> sqlalchemy.ext.asyncio.AsyncEngine:
    return _get_app()["engine"]


def create_session() -> sqlalchemy.ext.asyncio.AsyncSession:
    return _get_app()["sessionmaker"]()


def tenacity_retry_on_pk_integrity_error(
    models: tuple[type[sqlalchemy.orm.decl_api.DeclarativeBase], ...],
) -> tenacity.AsyncRetrying:
    matches = []
    for model in models:
        matches.append(
            f'\\(psycopg.errors.UniqueViolation\\) duplicate key value violates unique constraint "{model.__table__.primary_key.name}"',  # type: ignore[attr-defined]
        )
    return tenacity.AsyncRetrying(
        retry=tenacity.retry_if_exception_message(match="|".join(matches)),
    )


async def get_session() -> (
    abc.AsyncGenerator[sqlalchemy.ext.asyncio.AsyncSession, None]
):
    async with create_session() as session:
        yield session


Session = typing.Annotated[
    sqlalchemy.ext.asyncio.AsyncSession,
    fastapi.Depends(get_session),
]
