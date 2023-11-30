from __future__ import annotations

import datetime  # noqa: TCH003
import typing
import uuid  # noqa: TCH003

from alembic_utils import pg_function
from alembic_utils import pg_trigger
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy_utils

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import models


if typing.TYPE_CHECKING:
    from mergify_engine.models import github as gh_models


APPLICATIONS_LIMIT = 200


class ApplicationKeyLimitReached(database.CustomPostgresException):
    msg = (
        f"The number of application keys has reached the limit of {APPLICATIONS_LIMIT}"
    )


class ApplicationKey(models.Base):
    __tablename__ = "application"
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = ("id", "name")

    name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    api_access_key: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        index=True,
        anonymizer_config="''CONFIDENTIAL''",
    )

    api_secret_key: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy_utils.PasswordType(
            schemes=["pbkdf2_sha512"],
        ),
        nullable=False,
        anonymizer_config="''CONFIDENTIAL''",
    )
    github_account_id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.ForeignKey("github_account.id"),
        index=True,
        nullable=False,
        anonymizer_config=None,
    )
    created_by_github_user_id: orm.Mapped[
        github_types.GitHubAccountIdType | None
    ] = orm.mapped_column(
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("github_user.id"),
        anonymizer_config=None,
    )

    github_account: orm.Mapped[gh_models.GitHubAccount] = orm.relationship(
        "GitHubAccount",
        foreign_keys=[github_account_id],
        lazy="immediate",
    )

    created_by: orm.Mapped[gh_models.GitHubUser | None] = orm.relationship(
        "GitHubUser",
        uselist=False,
        foreign_keys=[created_by_github_user_id],
        lazy="immediate",
    )

    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime,
        server_default=sqlalchemy.func.now(),
        nullable=False,
        anonymizer_config="anon.dnoise(created_at, ''2 days'')",
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sqlalchemy.Uuid,
        primary_key=True,
        server_default=sqlalchemy.text("gen_random_uuid()"),
        anonymizer_config=None,
    )

    @staticmethod
    async def get_by_key(
        session: sqlalchemy.ext.asyncio.AsyncSession,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationKey | None:
        result_application = await session.execute(
            sqlalchemy.select(ApplicationKey).where(
                ApplicationKey.api_access_key == api_access_key,
            ),
        )
        try:
            application = result_application.unique().scalar_one()
        except sqlalchemy.exc.NoResultFound:
            return None

        if application.api_secret_key != api_secret_key:
            return None

        return application

    __postgres_entities__ = (
        pg_function.PGFunction.from_sql(
            f"""
CREATE FUNCTION public.increase_application_keys_count()
RETURNS TRIGGER AS $$
DECLARE
    current_count INTEGER;
BEGIN
    -- Get the current application_keys_count for the related github_account and lock the row for update
    SELECT application_keys_count INTO current_count
    FROM github_account
    WHERE github_account.id = NEW.github_account_id
    FOR UPDATE;

    -- Check if the application_keys_count is less than APPLICATIONS_LIMIT
    IF current_count < {APPLICATIONS_LIMIT} THEN
        -- Increase the application_keys_count by 1
        UPDATE github_account
        SET application_keys_count = current_count + 1
        WHERE id = NEW.github_account_id;

        -- Allow the insertion
        RETURN NEW;
    ELSE
        -- Reject the insertion if the count is {APPLICATIONS_LIMIT} or more
        RAISE {ApplicationKeyLimitReached.to_pg_exception()};
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
""".strip(),
        ),
        pg_trigger.PGTrigger.from_sql(
            """
CREATE TRIGGER trigger_increase_application_keys_count
BEFORE INSERT
ON application
FOR EACH ROW EXECUTE FUNCTION increase_application_keys_count();
""".strip(),
        ),
        pg_function.PGFunction.from_sql(
            """
CREATE FUNCTION public.decrease_application_keys_count()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE github_account
    SET application_keys_count = application_keys_count - 1
    WHERE id = OLD.github_account_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
""".strip(),
        ),
        pg_trigger.PGTrigger.from_sql(
            """
CREATE TRIGGER trigger_decrease_application_keys_count
BEFORE DELETE
ON application
FOR EACH ROW EXECUTE FUNCTION decrease_application_keys_count();
""".strip(),
        ),
    )
