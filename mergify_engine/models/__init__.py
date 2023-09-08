import typing

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio


class ORMObjectAsDict(typing.TypedDict):
    pass


class Base(orm.DeclarativeBase):
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {"eager_defaults": True}  # type: ignore [misc]

    metadata = sqlalchemy.MetaData(
        naming_convention={
            "pk": "%(table_name)s_pkey",
            "fk": "%(table_name)s_%(column_0_N_name)s_fkey",
            "ix": "%(table_name)s_%(column_0_N_name)s_idx",
            "uq": "%(table_name)s_%(column_0_N_name)s_key",
            "ck": "%(table_name)s_%(constraint_name)s_check",
        }
    )

    def as_dict(self) -> ORMObjectAsDict:
        columns = self.__table__.columns.values()

        # NOTE(lecrepont01): for an inherited relation, the attributes of
        # the parent class must be added
        if (
            "polymorphic_identity" in self.__mapper_args__
            and "polymorphic_on" not in self.__mapper_args__
        ):
            parent_relation = self.__class__.__bases__[0]
            columns += parent_relation.__table__.columns.values()  # type: ignore [attr-defined]

        result = {c.name: getattr(self, c.name) for c in columns}
        for relationship in sqlalchemy.inspect(self.__class__).relationships:
            relationship_name = relationship.key
            relationship_value = getattr(self, relationship_name)
            if relationship_value is not None:
                result[relationship_name] = relationship_value.as_dict()

        return result  # type: ignore [return-value]

    @classmethod
    async def total(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> int:
        return (
            await session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(cls)
            )
        ) or 0


# NOTE(charly): ensure all models are loaded, to
# allow Alembic to find all tables
from mergify_engine.models import application_keys  # noqa
from mergify_engine.models import github_account  # noqa
from mergify_engine.models import github_actions  # noqa
from mergify_engine.models import github_repository  # noqa
from mergify_engine.models import github_user  # noqa
from mergify_engine.models import events  # noqa
from mergify_engine.models import enumerations  # noqa
