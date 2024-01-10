from __future__ import annotations

import enum
import functools
import hashlib
import hmac
import json
import math
import re
import typing
import urllib

import daiquiri
import tenacity
import voluptuous

from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine.models.github import repository as github_repository


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
        raise voluptuous.Invalid(message % v)

    return validator


def unicode_truncate(
    s: str,
    length: int,
    placeholder: str = "",
    position: typing.Literal["middle", "end"] = "end",
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
                "`placeholder` length must be greater or equal to `length`",
            )

        cut_at = length - placeholder_length
        if position == "end":
            return (b[:cut_at] + placeholder_bytes).decode(encoding, errors="ignore")

        if position == "middle":
            cut_at_middle = cut_at / 2.0
            cut_at_left = cut_at_right = int(cut_at_middle)

            if not cut_at_middle.is_integer():
                cut_at_left += 1

            start = b[:cut_at_left]
            end = b[-cut_at_right:] if cut_at_right > 0 else b""
            return (start + placeholder_bytes + end).decode(encoding, errors="ignore")

        raise RuntimeError(f"Invalid position: {position}")

    return s


def compute_hmac(data: bytes, secret: str) -> str:
    mac = hmac.new(secret.encode("utf8"), msg=data, digestmod=hashlib.sha1)
    return str(mac.hexdigest())


class SupportsLessThan(typing.Protocol):
    def __lt__(self, __other: typing.Any) -> bool:
        ...


SupportsLessThanT = typing.TypeVar("SupportsLessThanT", bound=SupportsLessThan)


def get_random_choices(
    random_number: int,
    population: dict[SupportsLessThanT, int],
    k: int = 1,
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
    if last in {11, 12, 13}:
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


class MergifyHiddenPayload(typing.TypedDict):
    pass


class MergifyHiddenPayloadNotFoundError(Exception):
    pass


def deserialize_hidden_payload(
    comment_body: str,
) -> MergifyHiddenPayload:
    payload_match = MERGIFY_COMMENT_PAYLOAD_MATCHER.search(comment_body)

    if payload_match is None:
        raise MergifyHiddenPayloadNotFoundError

    try:
        payload: MergifyHiddenPayload = json.loads(payload_match[1])
    except Exception:
        LOG.error("MergifyHiddenPayload is invalid: '%s'", payload_match[1])
        raise MergifyHiddenPayloadNotFoundError

    return payload


def serialize_hidden_payload(json_payload: MergifyHiddenPayload) -> str:
    return f"""<!---
{MERGIFY_COMMENT_PAYLOAD_STR_PREFIX}
{json.dumps(json_payload)}
{MERGIFY_COMMENT_PAYLOAD_STR_SUFFIX}
-->"""


def strtobool(string: str) -> bool:
    if string.lower() in {"y", "yes", "t", "true", "on", "1"}:
        return True

    if string.lower() in {"n", "no", "f", "false", "off", "0"}:
        return False

    raise ValueError(f"Could not convert '{string}' to boolean")


def string_to_list_of_tuple(
    v: str,
    split: int = 2,
    list_sep: str = ",",
    tuple_sep: str = ":",
) -> list[tuple[str, ...]]:
    d = []
    for tuple_str in v.split(list_sep):
        if tuple_str.strip():
            values = tuple_str.split(tuple_sep, maxsplit=split)
            if len(values) != split:
                raise ValueError(f"wrong number of {tuple_sep}")
            d.append(tuple(v.strip() for v in values))
    return d


StringToDictCastT = typing.TypeVar("StringToDictCastT", int, str)


def string_to_dict(
    v: str,
    _type: type[StringToDictCastT],
) -> dict[str, StringToDictCastT]:
    return {
        key: _type(value)
        for key, value in typing.cast(
            list[tuple[str, str]],
            string_to_list_of_tuple(v, split=2),
        )
    }


def strip_comment_tags(line: str) -> str:
    return line.removeprefix("<!--").removesuffix("-->").strip()


def extract_default_branch(
    repository: github_types.GitHubRepository | github_repository.GitHubRepositoryDict,
) -> github_types.GitHubRefType:
    # Helper to easily mock default branch during tests
    return repository["default_branch"]


def github_url_parser(
    url: str,
) -> tuple[
    github_types.GitHubLogin,
    github_types.GitHubRepositoryName | None,
    github_types.GitHubPullRequestNumber | None,
    github_types.GitHubRefType | None,
]:
    path = [el for el in urllib.parse.urlparse(url).path.split("/") if el]

    pull_number: str | None
    branch: str | None
    repo: str | None

    try:
        owner, repo, kind, pull_number_or_branch = path
    except ValueError:
        pull_number = None
        branch = None
        kind = None
        try:
            owner, repo = path
        except ValueError:
            if len(path) == 1:
                owner = path[0]
                repo = None
            else:
                raise ValueError
    else:
        if kind == "branch":
            pull_number = None
            branch = pull_number_or_branch
        elif kind == "pull":
            branch = None
            pull_number = pull_number_or_branch
        else:
            raise ValueError

    return (
        github_types.GitHubLogin(owner),
        None if repo is None else github_types.GitHubRepositoryName(repo),
        None
        if pull_number is None
        else github_types.GitHubPullRequestNumber(int(pull_number)),
        None if branch is None else github_types.GitHubRefType(branch),
    )


_D = typing.TypeVar("_D", bound=typing.Mapping[str, typing.Any])
Mask = dict[str, typing.Union[bool, "Mask", list["Mask"]]]


def filter_dict(data: _D, mask: Mask) -> _D:
    filtered = {}

    for key, value in data.items():
        if mask.get(key, False):
            if isinstance(mask[key], dict):
                filtered[key] = filter_dict(value, typing.cast(Mask, mask[key]))
            elif isinstance(mask[key], list):
                filtered[key] = [
                    filter_dict(item, typing.cast(list[Mask], mask[key])[0])
                    for item in value
                ]
            else:
                filtered[key] = value

    return typing.cast(_D, filtered)


P = typing.ParamSpec("P")
CoroReturn = typing.TypeVar("CoroReturn")
CoroSendT = typing.TypeVar("CoroSendT")
CoroThrowT = typing.TypeVar("CoroThrowT")


def map_tenacity_try_again_to_real_cause(
    func: abc.Callable[P, abc.Coroutine[CoroSendT, CoroThrowT, CoroReturn]],
) -> abc.Callable[P, abc.Coroutine[CoroSendT, CoroThrowT, CoroReturn]]:
    @functools.wraps(func)
    async def inner_func(*args: P.args, **kwargs: P.kwargs) -> CoroReturn:
        try:
            return await func(*args, **kwargs)
        except tenacity.TryAgain as exc:
            if exc.__context__ is None:
                raise RuntimeError(
                    "map_tenacity_try_again_to_real_cause must be used only if TryAgain is raise in an except block",
                )
            raise exc.__context__

    return inner_func
