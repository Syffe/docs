from collections import abc
import logging
import typing
from urllib import parse

import pydantic

from mergify_engine import utils


class NormalizedUrl(pydantic.HttpUrl):
    @typing.no_type_check
    def __new__(cls, url: str | None, **kwargs) -> object:
        # Always rebuild the url from parts
        return str.__new__(cls, cls.build(**kwargs))

    @classmethod
    def validate_parts(
        cls, parts: "pydantic.networks.Parts", validate_port: bool = True
    ) -> "pydantic.networks.Parts":
        super().validate_parts(parts, validate_port)
        if parts["path"] and parts["path"].endswith("/"):
            parts["path"] = parts["path"][:-1]
        if not parts["path"]:
            parts["path"] = None
        parts["query"] = None
        parts["fragment"] = None
        return parts


class SecretUrl(parse.SplitResult):
    allowed_schemes: typing.ClassVar[tuple[str, ...] | None] = None
    override_scheme: typing.ClassVar[str | None] = None

    def __str__(self) -> str:
        if self.username or self.password:
            netloc = f"***@{self.hostname or ''}"
        else:
            netloc = self.hostname or ""
        if self.port is not None:
            netloc += f":{self.port}"
        return self._replace(netloc=netloc).geturl()

    __repr__ = __str__

    @classmethod
    def parse(cls, v: str | parse.SplitResult) -> typing.Self:
        if isinstance(v, parse.SplitResult):
            parsed = v
        else:
            parsed = parse.urlsplit(v)
            if (
                cls.allowed_schemes is not None
                and parsed.scheme not in cls.allowed_schemes
            ):
                raise ValueError(
                    f"scheme `{parsed.scheme}` is invalid, must be {','.join(cls.allowed_schemes)}"
                )

        return cls(
            scheme=parsed.scheme
            if cls.override_scheme is None
            else cls.override_scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            query=parsed.query,
            fragment=parsed.fragment,
        )

    @classmethod
    def __get_validators__(cls) -> abc.Iterator[abc.Callable[[str], typing.Self]]:
        yield cls.parse

    @classmethod
    def __modify_schema__(cls, field_schema: dict[str, typing.Any]) -> None:
        pass


class PostgresDSN(SecretUrl):
    allowed_schemes = ("postgres", "postgresql", "postgresql+psycopg")
    override_scheme = "postgresql+psycopg"


class RedisDSN(SecretUrl):
    allowed_schemes = ("redis", "rediss")


class LogLevel(pydantic.PositiveInt):
    @typing.no_type_check
    def __new__(cls, value):
        value = cls.parse_loglevel_aliases(value)
        return super().__new__(cls, value)

    @classmethod
    def __get_validators__(cls) -> typing.Any:
        yield cls.parse_loglevel_aliases
        yield from super().__get_validators__()  # type: ignore[misc]

    @classmethod
    def parse_loglevel_aliases(cls, value: str | int) -> str | int:
        if isinstance(value, str) and value.upper() in (
            "CRITICAL",
            "ERROR",
            "WARNING",
            "INFO",
            "DEBUG",
        ):
            value = int(getattr(logging, value.upper()))
        return value


def AccountTokens(v: str) -> list[tuple[int, str, pydantic.SecretStr]]:
    try:
        return [
            (int(_id), login, pydantic.SecretStr(token))
            for _id, login, token in typing.cast(
                list[tuple[int, str, str]],
                utils.string_to_list_of_tuple(v, split=3),
            )
        ]
    except ValueError:
        raise ValueError("wrong format, expect `id1:login1:token1,id2:login2:token2`")


API_ACCESS_KEY_LEN = 32
API_SECRET_KEY_LEN = 32


class ApplicationAPIKey(typing.TypedDict):
    api_access_key: str
    api_secret_key: str
    account_id: int
    account_login: str


def ApplicationAPIKeys(v: str) -> dict[str, ApplicationAPIKey]:
    try:
        applications = utils.string_to_list_of_tuple(v, split=3)
        _validate_application_api_keys(applications)
    except ValueError:
        raise ValueError(
            "wrong format, "
            "expect `api_key1:github_account_id1:github_account_login1,api_key1:github_account_id2:github_account_login2`, "
            "api_key must be 64 character long"
        )

    return {
        api_key[:API_ACCESS_KEY_LEN]: {
            "api_access_key": api_key[:API_ACCESS_KEY_LEN],
            "api_secret_key": api_key[API_ACCESS_KEY_LEN:],
            "account_id": int(account_id),
            "account_login": account_login,
        }
        for api_key, account_id, account_login in applications
    }


def _validate_application_api_keys(applications: list[tuple[str, ...]]) -> None:
    for api_key, _, _ in applications:
        if len(api_key) != API_ACCESS_KEY_LEN + API_ACCESS_KEY_LEN:
            raise ValueError("api_key must be 64 character long")
