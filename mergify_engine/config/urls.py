from collections import abc
import typing
from urllib import parse


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
