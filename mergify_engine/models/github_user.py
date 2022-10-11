from __future__ import annotations

import typing

import sqlalchemy
import sqlalchemy.dialects.postgresql
import sqlalchemy.event
import sqlalchemy.ext.asyncio
import sqlalchemy.orm.attributes
import sqlalchemy.orm.exc
import sqlalchemy.orm.session
import sqlalchemy.sql.expression
import sqlalchemy_utils
from sqlalchemy_utils.types.encrypted import encrypted_type

from mergify_engine import config
from mergify_engine import models


class OAuthTokenSecretString(str):
    def __new__(cls, value: str, key: str) -> OAuthTokenSecretString:
        obj = str.__new__(cls, value)
        obj.key = key  # type: ignore[attr-defined]
        return obj


class OAuthTokenEncryptedType(sqlalchemy_utils.StringEncryptedType):  # type: ignore[misc]
    cache_ok = True

    def __init__(self) -> None:
        super().__init__(sqlalchemy.String(512), None, encrypted_type.AesGcmEngine)

    def process_bind_param(
        self,
        value: OAuthTokenSecretString | str | None,
        dialect: typing.Any,
    ) -> typing.Any:
        self.key = config.DATABASE_OAUTH_TOKEN_SECRET_CURRENT
        return super().process_bind_param(value, dialect)

    def process_result_value(
        self, value: str | None, dialect: typing.Any
    ) -> OAuthTokenSecretString:
        self.key = config.DATABASE_OAUTH_TOKEN_SECRET_CURRENT
        try:
            secret = typing.cast(str, super().process_result_value(value, dialect))
            oauth_secret = OAuthTokenSecretString(secret, self.key)
        except sqlalchemy_utils.types.encrypted.encrypted_type.InvalidCiphertextError:
            if config.DATABASE_OAUTH_TOKEN_SECRET_OLD is not None:
                self.key = config.DATABASE_OAUTH_TOKEN_SECRET_OLD
                secret = typing.cast(str, super().process_result_value(value, dialect))
                return OAuthTokenSecretString(secret, self.key)
            raise
        else:
            return oauth_secret


class GitHubUser(models.Base):
    __tablename__ = "github_user"

    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.Column(
        sqlalchemy.Integer, primary_key=True
    )

    login: sqlalchemy.orm.Mapped[str] = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False
    )

    oauth_access_token: sqlalchemy.orm.Mapped[str] = sqlalchemy.Column(
        OAuthTokenEncryptedType()
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
        _id: int,
    ) -> GitHubUser | None:
        result = await session.execute(sqlalchemy.select(cls).where(cls.id == _id))
        return result.unique().scalar_one_or_none()

    @classmethod
    async def create_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        _id: int,
        login: str,
        oauth_access_token: str,
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
