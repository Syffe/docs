from collections import abc
import enum
import hashlib
import hmac
import json
import math
import re
import typing

import daiquiri
import voluptuous

from mergify_engine import github_types


LOG = daiquiri.getLogger()

MERGIFY_COMMENT_PAYLOAD_STR_PREFIX = "DO NOT EDIT\n-*- Mergify Payload -*-"
MERGIFY_COMMENT_PAYLOAD_STR_SUFFIX = "-*- Mergify Payload End -*-"
MERGIFY_COMMENT_PAYLOAD_REGEX = (
    "^"
    + MERGIFY_COMMENT_PAYLOAD_STR_PREFIX.replace("*", "\\*")
    + r"\n(.+)\n"
    + MERGIFY_COMMENT_PAYLOAD_STR_SUFFIX.replace("*", "\\*")
)
MERGIFY_COMMENT_PAYLOAD_MATCHER = re.compile(
    MERGIFY_COMMENT_PAYLOAD_REGEX,
    re.MULTILINE,
)


# NOTE(sileht): Sentinel object (eg: `marker = object()`) can't be expressed
# with typing yet use the proposed workaround instead:
#   https://github.com/python/typing/issues/689
#   https://www.python.org/dev/peps/pep-0661/
class _UnsetMarker(enum.Enum):
    _MARKER = 0


UnsetMarker: typing.Final = _UnsetMarker._MARKER


def DeprecatedOption(
    message: str,
    default: typing.Any,
) -> abc.Callable[[typing.Any], typing.Any]:
    def validator(v: typing.Any) -> typing.Any:
        if v is UnsetMarker:
            return default
        else:
            raise voluptuous.Invalid(message % v)

    return validator


def unicode_truncate(
    s: str,
    length: int,
    placeholder: str = "",
    encoding: str = "utf-8",
) -> str:
    """Truncate a string to length in bytes.

    :param s: The string to truncate.
    :param length: The length in number of bytes — not characters (placeholder included).
    :param placeholder: String that will appear at the end of the output text if it has been truncated.
    """
    b = s.encode(encoding)
    if len(b) > length:
        placeholder_bytes = placeholder.encode(encoding)
        placeholder_length = len(placeholder_bytes)
        if placeholder_length > length:
            raise ValueError(
                "`placeholder` length must be greater or equal to `length`"
            )

        cut_at = length - placeholder_length

        return (b[:cut_at] + placeholder_bytes).decode(encoding, errors="ignore")
    else:
        return s


def compute_hmac(data: bytes, secret: str) -> str:
    mac = hmac.new(secret.encode("utf8"), msg=data, digestmod=hashlib.sha1)
    return str(mac.hexdigest())


class SupportsLessThan(typing.Protocol):
    def __lt__(self, __other: typing.Any) -> bool:
        ...


SupportsLessThanT = typing.TypeVar("SupportsLessThanT", bound=SupportsLessThan)


def get_random_choices(
    random_number: int, population: dict[SupportsLessThanT, int], k: int = 1
) -> set[SupportsLessThanT]:
    """Return a random number of item from a population without replacement.

    You need to provide the random number yourself.

    The output is always the same based on that number.

    The population is a dict where the key is the choice and the value is the weight.

    The argument k is the number of item that should be picked.

    :param random_number: The random_number that should be picked.
    :param population: The dict of {item: weight}.
    :param k: The number of choices to make.
    :return: A set with the choices.
    """
    if k > len(population):
        raise ValueError("k cannot be greater than the population size")

    picked: set[SupportsLessThanT] = set()
    population = population.copy()

    while len(picked) < k:
        total_weight = sum(population.values())
        choice_index = (random_number % total_weight) + 1
        for item in sorted(population.keys()):
            choice_index -= population[item]
            if choice_index <= 0:
                picked.add(item)
                del population[item]
                break

    return picked


ORDINAL_SUFFIXES = {1: "st", 2: "nd", 3: "rd"}


def to_ordinal_numeric(number: int) -> str:
    if number < 0:
        raise ValueError("number must be positive")
    last = number % 100
    if last in (11, 12, 13):
        suffix = "th"
    else:
        last = number % 10
        suffix = ORDINAL_SUFFIXES.get(last) or "th"
    return f"{number}{suffix}"


class FakePR:
    def __init__(self, key: str, value: typing.Any):
        setattr(self, key, value)


_T = typing.TypeVar("_T")


def split_list(remaining: list[_T], part: int) -> abc.Generator[list[_T], None, None]:
    size = math.ceil(len(remaining) / part)
    while remaining:
        yield remaining[:size]
        remaining = remaining[size:]


def get_hidden_payload_from_comment_body(
    comment_body: str,
) -> dict[str, typing.Any] | None:
    payload_match = MERGIFY_COMMENT_PAYLOAD_MATCHER.search(comment_body)

    if payload_match is None:
        return None

    try:
        payload: dict[typing.Any, typing.Any] = json.loads(payload_match[1])
    except Exception:
        LOG.error("Unable to load comment payload: '%s'", payload_match[1])
        return None

    return payload


def get_mergify_payload(json_payload: dict[str, typing.Any]) -> str:
    return f"""<!---
{MERGIFY_COMMENT_PAYLOAD_STR_PREFIX}
{json.dumps(json_payload)}
{MERGIFY_COMMENT_PAYLOAD_STR_SUFFIX}
-->"""


def strtobool(string: str) -> bool:
    if string.lower() in ("y", "yes", "t", "true", "on", "1"):
        return True

    if string.lower() in ("n", "no", "f", "false", "off", "0"):
        return False

    raise ValueError(f"Could not convert '{string}' to boolean")


def strip_comment_tags(line: str) -> str:
    return line.removeprefix("<!--").removesuffix("-->").strip()


def extract_default_branch(
    repository: github_types.GitHubRepository,
) -> github_types.GitHubRefType:
    # Helper to easily mock default branch during tests
    return repository["default_branch"]
