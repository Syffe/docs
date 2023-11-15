from __future__ import annotations

import typing

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.event
import sqlalchemy.ext.asyncio
import sqlalchemy.sql.expression
import sqlalchemy_utils
from sqlalchemy_utils.types.encrypted import encrypted_type

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import settings


class OAuthTokenSecretString(str):
    key: str

    def __new__(cls, value: str, key: str) -> OAuthTokenSecretString:
        obj = str.__new__(cls, value)
        obj.key = key
        return obj


class OAuthTokenEncryptedType(sqlalchemy_utils.StringEncryptedType):  # type: ignore[misc]
    cache_ok = True
    key: str

    def __init__(self) -> None:
        super().__init__(
            sqlalchemy.String(512), None, encrypted_type.AesGcmEngine, length=512
        )

    def process_bind_param(
        self,
        value: OAuthTokenSecretString | str | None,
        dialect: typing.Any,
    ) -> typing.Any:
        self.key = settings.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value()
        return super().process_bind_param(value, dialect)

    def process_result_value(
        self, value: str | None, dialect: typing.Any
    ) -> OAuthTokenSecretString:
        self.key = settings.DATABASE_OAUTH_TOKEN_SECRET_CURRENT.get_secret_value()
        try:
            secret = typing.cast(str, super().process_result_value(value, dialect))
            oauth_secret = OAuthTokenSecretString(secret, self.key)
        except sqlalchemy_utils.types.encrypted.encrypted_type.InvalidCiphertextError:
            if settings.DATABASE_OAUTH_TOKEN_SECRET_OLD is not None:
                self.key = settings.DATABASE_OAUTH_TOKEN_SECRET_OLD.get_secret_value()
                secret = typing.cast(str, super().process_result_value(value, dialect))
                return OAuthTokenSecretString(secret, self.key)
            raise
        else:
            return oauth_secret


class GitHubUser(models.Base):
    __tablename__ = "github_user"
    __repr_attributes__ = ("id", "login")

    id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.Integer,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )

    login: orm.Mapped[github_types.GitHubLogin] = orm.mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )

    oauth_access_token: orm.Mapped[github_types.GitHubOAuthToken] = orm.mapped_column(
        OAuthTokenEncryptedType(), anonymizer_config="''CONFIDENTIAL''"
    )

    def get_id(self) -> int:
        # NOTE(silet): for imia UserLike protocol
        return self.id

    def get_display_name(self) -> str:
        # NOTE(silet): for imia UserLike protocol
        return f"{self.id}"

    def get_hashed_password(self) -> str:
        # NOTE(silet): for imia UserLike protocol
        # We don't care about the password for imia as we only manual login
        # Security is done by OAuth2 and session stored in redis.
        return ""

    @classmethod
    async def get_by_id(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        _id: github_types.GitHubAccountIdType,
    ) -> GitHubUser | None:
        result = await session.execute(sqlalchemy.select(cls).where(cls.id == _id))
        return result.unique().scalar_one_or_none()

    @classmethod
    async def get_by_login(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        login: github_types.GitHubLogin,
    ) -> GitHubUser | None:
        result = await session.execute(
            sqlalchemy.select(cls).where(
                sqlalchemy.func.lower(cls.login) == login.lower()
            )
        )
        return result.unique().scalar_one_or_none()

    @classmethod
    async def create_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        _id: github_types.GitHubAccountIdType,
        login: github_types.GitHubLogin,
        oauth_access_token: github_types.GitHubOAuthToken,
    ) -> GitHubUser:
        user = await cls.get_by_id(session, _id)
        if user is None:
            user = cls(id=_id, oauth_access_token=oauth_access_token, login=login)
            session.add(user)
        else:
            user.oauth_access_token = oauth_access_token
            user.login = login
        await session.flush()
        await session.commit()
        return user

    def to_github_account(self) -> github_types.GitHubAccount:
        return github_types.GitHubAccount(
            {"id": self.id, "login": self.login, "type": "User", "avatar_url": ""}
        )
