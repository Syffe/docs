import typing

import sqlalchemy
from sqlalchemy import orm


class ORMObjectAsDict(typing.TypedDict):
    pass


class Base(orm.DeclarativeBase):
    __allow_unmapped__ = True
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
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        for relationship in sqlalchemy.inspect(self.__class__).relationships:
            relationship_name = relationship.key
            relationship_value = getattr(self, relationship_name)
            if relationship_value is not None:
                result[relationship_name] = relationship_value.as_dict()

        return result  # type: ignore [return-value]


# NOTE(charly): ensure all models are loaded, to
# allow Alembic to find all tables
from mergify_engine.models import application_keys  # noqa
from mergify_engine.models import github_account  # noqa
from mergify_engine.models import github_actions  # noqa
from mergify_engine.models import github_repository  # noqa
from mergify_engine.models import github_user  # noqa
from mergify_engine.models import events  # noqa
from mergify_engine.models import enumerations  # noqa
