import sqlalchemy
from sqlalchemy import orm


class Base(orm.DeclarativeBase):
    __allow_unmapped__ = True
    __mapper_args__ = {"eager_defaults": True}

    metadata = sqlalchemy.MetaData(
        naming_convention={
            "pk": "%(table_name)s_pkey",
            "fk": "%(table_name)s_%(column_0_N_name)s_fkey",
            "ix": "%(table_name)s_%(column_0_N_name)s_idx",
            "uq": "%(table_name)s_%(column_0_N_name)s_key",
            "ck": "%(table_name)s_%(constraint_name)s_check",
        }
    )


# NOTE(charly): ensure all models are loaded, to
# allow Alembic to find all tables
from mergify_engine.models import application_keys  # noqa
from mergify_engine.models import github_account  # noqa
from mergify_engine.models import github_actions  # noqa
from mergify_engine.models import github_repository  # noqa
from mergify_engine.models import github_user  # noqa
