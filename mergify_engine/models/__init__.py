from sqlalchemy import orm


class Base(orm.DeclarativeBase):
    __allow_unmapped__ = True
    __mapper_args__ = {"eager_defaults": True}
