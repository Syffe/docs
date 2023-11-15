from __future__ import annotations

import base64
import logging
import typing
from urllib import parse

import pydantic
import pydantic_core
import typing_extensions

from mergify_engine import github_types
from mergify_engine import utils


UdpUrl = typing.Annotated[
    pydantic.AnyUrl,
    pydantic.UrlConstraints(allowed_schemes=["udp"]),
]


class NormalizedUrl(str):
    _parsed_url: pydantic.HttpUrl

    @typing.no_type_check
    def __new__(cls, url: str):
        temp_parsed_url = pydantic.HttpUrl(url)
        parsed_url = pydantic.HttpUrl.build(
            scheme=temp_parsed_url.scheme,
            host=temp_parsed_url.host,
            port=temp_parsed_url.port,
            path=temp_parsed_url.path,
            query=None,
            fragment=None,
        )
        str_url = str(parsed_url).strip("/")
        # XXX: Don't know if this is a bug from pydantic or from the rust
        # library they use, but the str of any url always has a double /
        # before the path for some reason.
        scheme_length = len(parsed_url.scheme) + 3  # +3 = "://"
        while "//" in str_url[scheme_length:]:
            str_url = (
                f"{str_url[:scheme_length]}{str_url[scheme_length:].replace('//', '/')}"
            )

        obj = str.__new__(cls, str_url)
        obj._parsed_url = parsed_url
        return obj

    def __getattr__(self, attr: str) -> typing.Any:
        return getattr(self._parsed_url, attr)

    @classmethod
    def __get_pydantic_core_schema__(
        self,
        _source_type: typing.Any,
        _handler: typing.Callable[[typing.Any], pydantic_core.core_schema.CoreSchema],
    ) -> pydantic_core.core_schema.CoreSchema:
        def validate_from_str(value: str) -> NormalizedUrl:
            # For some reason mypy think we do not return a NormalizedUrl here...
            return NormalizedUrl(value)  # type: ignore[no-any-return]

        from_str_schema = pydantic_core.core_schema.chain_schema(
            [
                pydantic_core.core_schema.str_schema(),
                pydantic_core.core_schema.no_info_plain_validator_function(
                    validate_from_str,
                ),
            ],
        )

        return pydantic_core.core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=pydantic_core.core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    pydantic_core.core_schema.is_instance_schema(NormalizedUrl),
                    from_str_schema,
                ],
            ),
            serialization=pydantic_core.core_schema.plain_serializer_function_ser_schema(
                lambda instance: str(instance),
            ),
        )


T = typing.TypeVar("T")


class ListFromStrWithComma(list[T]):
    _type: type[T]

    @classmethod
    def parse(cls, v: str | list[typing.Any]) -> typing.Self:
        if isinstance(v, list):
            return typing.cast(typing.Self, [cls._type(x) for x in v])
        return typing.cast(typing.Self, [cls._type(x) for x in v.split(",")])

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_plain_validator_function(cls.parse)


class StrListFromStrWithComma(ListFromStrWithComma[str]):
    _type = str


class IntListFromStrWithComma(ListFromStrWithComma[int]):
    _type = int


class GitHubLoginListFromStrWithComma(ListFromStrWithComma[github_types.GitHubLogin]):
    _type = github_types.GitHubLogin


TT = typing.TypeVar("TT")


class DictFromStr(dict[str, TT]):
    _type: type[TT]

    @classmethod
    def parse(cls, v: str | dict[typing.Any, typing.Any]) -> typing.Self:
        if isinstance(v, dict):
            return typing.cast(
                typing.Self,
                {str(k): cls._type(va) for k, va in v.items()},
            )
        return typing.cast(typing.Self, utils.string_to_dict(v, cls._type))

    @classmethod
    def from_dict(cls, _dict: dict[typing.Any, typing.Any]) -> typing.Self:
        return cls({str(k): cls._type(v) for k, v in _dict.items()})

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_plain_validator_function(cls.parse)


class SecretStrFromBase64(pydantic.SecretStr):
    def __init__(self, value: str):
        super().__init__(base64.b64decode(value).decode())


class StrIntDictFromStr(DictFromStr[int]):
    _type = int


class StrStrDictFromStr(DictFromStr[str]):
    _type = str


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
                    f"scheme `{parsed.scheme}` is invalid, must be {','.join(cls.allowed_schemes)}",
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
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_plain_validator_function(cls.parse)


class PostgresDSN(SecretUrl):
    allowed_schemes = ("postgres", "postgresql", "postgresql+psycopg")
    override_scheme = "postgresql+psycopg"


class RedisDSN(SecretUrl):
    allowed_schemes = ("redis", "rediss")


def parse_loglevel_aliases(value: str | int) -> str | int:
    if isinstance(value, str) and value.upper() in (
        "CRITICAL",
        "ERROR",
        "WARNING",
        "INFO",
        "DEBUG",
    ):
        return int(getattr(logging, value.upper()))
    return int(value)


LogLevel = typing.Annotated[
    pydantic.PositiveInt,
    pydantic.BeforeValidator(parse_loglevel_aliases),
]


class AccountTokens(list[tuple[int, str, pydantic.SecretStr]]):
    @classmethod
    def parse(cls, v: str | list[tuple[int, str, pydantic.SecretStr]]) -> AccountTokens:
        if isinstance(v, list):
            for _, _, token in v:
                if not isinstance(token, pydantic.SecretStr | str):
                    raise ValueError("token must be a `str` or `pydantic.SecretStr`")

            return AccountTokens(
                [
                    (
                        int(_id),
                        str(login),
                        pydantic.SecretStr(token) if isinstance(token, str) else token,
                    )
                    for _id, login, token in v
                ],
            )

        return AccountTokens(
            [
                (int(_id), login, pydantic.SecretStr(token))
                for _id, login, token in typing.cast(
                    list[tuple[int, str, str]],
                    utils.string_to_list_of_tuple(v, split=3),
                )
            ],
        )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: typing.Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_plain_validator_function(cls.parse)


API_ACCESS_KEY_LEN = 32
API_SECRET_KEY_LEN = 32


class ApplicationAPIKey(typing_extensions.TypedDict):
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
            "api_key must be 64 character long",
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
