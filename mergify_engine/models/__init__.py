from collections import abc
import datetime
import enum
import typing

from alembic_utils import pg_extension
from alembic_utils import replaceable_entity
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio


class ORMObjectAsDict(typing.TypedDict):
    pass


class Base(orm.DeclarativeBase):
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {"eager_defaults": True}  # type: ignore [misc]
    __github_attributes__: typing.ClassVar[tuple[str, ...]] = ()
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ()

    __postgres_entities__: typing.ClassVar[
        tuple[replaceable_entity.ReplaceableEntity, ...]
    ] = ()

    __postgres_extensions__: typing.ClassVar[tuple[pg_extension.PGExtension, ...]] = (
        pg_extension.PGExtension("public", "vector"),
    )

    metadata = sqlalchemy.MetaData(
        naming_convention={
            "pk": "%(table_name)s_pkey",
            "fk": "%(table_name)s_%(column_0_N_name)s_fkey",
            "ix": "%(table_name)s_%(column_0_N_name)s_idx",
            "uq": "%(table_name)s_%(column_0_N_name)s_key",
            "ck": "%(table_name)s_%(constraint_name)s_check",
        },
    )

    def _as_dict(
        self,
        included_columns_attribute: str | None = None,
        value_transformer: abc.Callable[[typing.Any], typing.Any] | None = None,
    ) -> ORMObjectAsDict:
        if included_columns_attribute is None:
            included_columns = None
        else:
            included_columns = getattr(self, included_columns_attribute, ())

        result = {}
        inspector = sqlalchemy.inspect(self.__class__)
        # mypy is not happy without the `.keys()`
        for name in inspector.all_orm_descriptors.keys():  # noqa: SIM118
            if name.startswith("_"):
                continue
            if not (included_columns is None or name in included_columns):
                continue

            value = getattr(self, name)
            if value is not None and name in inspector.relationships:
                if isinstance(value, list):
                    value = [
                        v._as_dict(included_columns_attribute, value_transformer)
                        for v in value
                    ]
                else:
                    value = value._as_dict(
                        included_columns_attribute,
                        value_transformer,
                    )
            elif value_transformer is not None:
                value = value_transformer(value)

            result[name] = value

        return result  # type: ignore [return-value]

    @staticmethod
    def _as_github_dict_value_transformer(value: typing.Any) -> typing.Any:
        if isinstance(value, datetime.datetime):
            # Replace the +00:00 with Z to be iso with GitHub's way of returning iso dates
            return value.isoformat().replace("+00:00", "Z")

        if isinstance(value, enum.StrEnum):
            return value.value

        return value

    def as_github_dict(self) -> ORMObjectAsDict:
        return self._as_dict(
            "__github_attributes__",
            self._as_github_dict_value_transformer,
        )

    @classmethod
    async def total(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> int:
        return (
            await session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(cls),
            )
        ) or 0

    @classmethod
    def get_postgres_entities(cls) -> list[replaceable_entity.ReplaceableEntity]:
        postgres_entities: list[replaceable_entity.ReplaceableEntity] = []
        for mapper in cls.registry.mappers:
            postgres_entities.extend(
                getattr(mapper.class_, "__postgres_entities__", ()),
            )
        return postgres_entities

    def __repr__(self) -> str:
        r = super().__repr__()
        # To get better Sentry data
        attrs = [f"{attr}={getattr(self, attr)!r}" for attr in self.__repr_attributes__]
        return f"{r[:-1]} {' '.join(attrs)}>"


# NOTE(charly): ensure all models are loaded, to
# allow Alembic to find all tables
from mergify_engine.models import active_user  # noqa: E402,F401
from mergify_engine.models import application_keys  # noqa: E402,F401
from mergify_engine.models import ci_issue  # noqa: E402,F401
from mergify_engine.models import enumerations  # noqa: E402,F401
from mergify_engine.models import events  # noqa: E402,F401
from mergify_engine.models import github  # noqa: E402,F401
