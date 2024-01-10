import datetime
import typing

import pytest
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import models
from mergify_engine.models.github.account import GitHubAccountType


@pytest.mark.parametrize(
    ("value", "expected_value"),
    [
        (
            datetime.datetime(2023, 11, 10, 10, 5, 23, tzinfo=datetime.UTC),
            "2023-11-10T10:05:23Z",
        ),
        (GitHubAccountType.USER, "User"),
        (GitHubAccountType.ORGANIZATION, "Organization"),
    ],
)
def test_as_github_dict_value_transformer(
    value: typing.Any,
    expected_value: typing.Any,
) -> None:
    assert models.Base._as_github_dict_value_transformer(value) == expected_value
    assert type(models.Base._as_github_dict_value_transformer(value)) is type(
        expected_value,
    )


def test_model_as_dict() -> None:
    class TestSimpleModel(models.Base):
        __tablename__ = "test_simple_table"
        __github_attributes__ = ("id", "name")
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]
        not_a_github_thing: orm.Mapped[str]

    obj = TestSimpleModel(id=0)
    assert obj.as_github_dict() == typing.cast(
        models.ORMObjectAsDict,
        {"id": 0, "name": None},
    )

    obj = TestSimpleModel(id=0, name="hello")
    assert obj.as_github_dict() == typing.cast(
        models.ORMObjectAsDict,
        {"id": 0, "name": "hello"},
    )


def test_relational_model_as_dict() -> None:
    class TestRelationalUserModel(models.Base):
        __tablename__ = "test_relational_user_table"
        __github_attributes__ = ("id", "name", "copy_id")

        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]

        @sqlalchemy.ext.hybrid.hybrid_property
        def copy_id(self) -> int:
            return self.id

    class TestRelationalModel(models.Base):
        __tablename__ = "test_relational_table"
        __github_attributes__ = ("id", "name", "user")
        id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
        name: orm.Mapped[str]
        user_id: orm.Mapped[int] = orm.mapped_column(
            sqlalchemy.ForeignKey("test_relational_user_table.id"),
        )
        user: orm.Mapped[TestRelationalUserModel] = orm.relationship(
            lazy="joined",
            foreign_keys=[user_id],
        )

    obj = TestRelationalModel(id=0)
    assert obj.as_github_dict() == typing.cast(
        models.ORMObjectAsDict,
        {"id": 0, "name": None, "user": None},
    )

    obj = TestRelationalModel(
        id=0,
        name="hello",
        user_id=0,
        user=TestRelationalUserModel(id=0, name="me"),
    )
    assert obj.as_github_dict() == typing.cast(
        models.ORMObjectAsDict,
        {
            "id": 0,
            "name": "hello",
            "user": {"id": 0, "name": "me", "copy_id": 0},
        },
    )
