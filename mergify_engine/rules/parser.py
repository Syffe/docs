import dataclasses
import enum
import re
import string
import typing

import jinja2
import jinja2.meta
import jinja2.sandbox

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.rules import filter


@dataclasses.dataclass
class ConditionParsingError(Exception):
    message: str


class Parser(enum.Enum):
    BOOL = enum.auto()
    BRANCH = enum.auto()
    DAY = enum.auto()
    DOW = enum.auto()
    ENUM = enum.auto()
    LOGIN_AND_TEAMS = enum.auto()
    MONTH = enum.auto()
    NUMBER = enum.auto()
    PERMISSION = enum.auto()
    POSITIVE_NUMBER = enum.auto()
    SCHEDULE = enum.auto()
    TEXT = enum.auto()
    TIME = enum.auto()
    TIMESTAMP = enum.auto()
    TIMESTAMP_OR_TIMEDELTA = enum.auto()
    WORD = enum.auto()
    YEAR = enum.auto()


PARSERS_FOR_ARRAY_SUBATTRIBUTES = {
    "commits": {
        "author": Parser.LOGIN_AND_TEAMS,
        "commit_message": Parser.TEXT,
        "commit_verification_verified": Parser.BOOL,
        "committer": Parser.LOGIN_AND_TEAMS,
        "date_author": Parser.TIMESTAMP_OR_TIMEDELTA,
        "date_committer": Parser.TIMESTAMP_OR_TIMEDELTA,
        "email_author": Parser.TEXT,
        "email_committer": Parser.TEXT,
    }
}

CONDITION_PARSERS = {
    "number": Parser.POSITIVE_NUMBER,
    "head": Parser.BRANCH,
    "base": Parser.BRANCH,
    "author": Parser.LOGIN_AND_TEAMS,
    "merged-by": Parser.LOGIN_AND_TEAMS,
    "body": Parser.TEXT,
    "body-raw": Parser.TEXT,
    # backward compat
    "assignee": Parser.LOGIN_AND_TEAMS,
    "assignees": Parser.LOGIN_AND_TEAMS,
    "label": Parser.TEXT,
    "title": Parser.TEXT,
    "files": Parser.TEXT,
    "added-files": Parser.TEXT,
    "modified-files": Parser.TEXT,
    "removed-files": Parser.TEXT,
    "commits-behind": Parser.TEXT,
    "commits": Parser.TEXT,
    "milestone": Parser.WORD,
    "queue-position": Parser.NUMBER,
    "review-requested": Parser.LOGIN_AND_TEAMS,
    "approved-reviews-by": Parser.LOGIN_AND_TEAMS,
    "dismissed-reviews-by": Parser.LOGIN_AND_TEAMS,
    "changes-requested-reviews-by": Parser.LOGIN_AND_TEAMS,
    "commented-reviews-by": Parser.LOGIN_AND_TEAMS,
    "status-success": Parser.TEXT,
    "status-failure": Parser.TEXT,
    "status-neutral": Parser.TEXT,
    "check-success": Parser.TEXT,
    "check-success-or-neutral": Parser.TEXT,
    "check-failure": Parser.TEXT,
    "check-neutral": Parser.TEXT,
    "check-skipped": Parser.TEXT,
    "check-timed-out": Parser.TEXT,
    "check-pending": Parser.TEXT,
    "check-stale": Parser.TEXT,
    "commits-unverified": Parser.TEXT,
    "review-threads-resolved": Parser.TEXT,
    "review-threads-unresolved": Parser.TEXT,
    "repository-name": Parser.TEXT,
    "repository-full-name": Parser.TEXT,
    "schedule": Parser.SCHEDULE,
    "created-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "updated-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "closed-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "merged-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "queued-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "queue-merge-started-at": Parser.TIMESTAMP_OR_TIMEDELTA,
    "locked": Parser.BOOL,
    "merged": Parser.BOOL,
    "closed": Parser.BOOL,
    "conflict": Parser.BOOL,
    "draft": Parser.BOOL,
    "linear-history": Parser.BOOL,
    "dependabot-dependency-name": Parser.TEXT,
    "dependabot-dependency-type": Parser.TEXT,
    "dependabot-update-type": Parser.TEXT,
    "branch-protection-review-decision": Parser.ENUM,
    "sender": Parser.LOGIN_AND_TEAMS,
    "sender-permission": Parser.PERMISSION,
    "queue-partition-name": Parser.TEXT,
    "current-datetime": Parser.TIMESTAMP,
}
COMMAND_ONLY_ATTRIBUTES = ("sender", "sender-permission")
CONDITION_ENUMS = {
    "branch-protection-review-decision": [
        "APPROVED",
        "REVIEW_REQUIRED",
        "CHANGES_REQUESTED",
    ]
}

# NOTE(sileht): From the longest string to the short one to ensure for
# example that merged-at is selected before merged
ATTRIBUTES = sorted(CONDITION_PARSERS, key=lambda v: (len(v), v), reverse=True)


ATTRIBUTES_WITH_ONLY_LENGTH = ("commits-behind",)

# Negate, quantity (default: True, True)
PARSER_MODIFIERS = {
    Parser.BOOL: (True, False),
    Parser.NUMBER: (True, False),
    Parser.POSITIVE_NUMBER: (True, False),
    Parser.SCHEDULE: (False, False),
    Parser.TIMESTAMP_OR_TIMEDELTA: (False, False),
    Parser.TIMESTAMP: (False, False),
}

NEGATION_OPERATORS = ("-", "¬")
POSITIVE_OPERATORS = ("+",)
RANGE_OPERATORS = (">=", "<=", "≥", "≤", "<", ">")
EQUALITY_OPERATORS = ("==", "!=", "≠", "=", ":")
OPERATOR_ALIASES = {
    ":": "=",
    "==": "=",
    "≠": "!=",
    "≥": ">=",
    "≤": "<=",
}
REGEX_OPERATOR = "~="
SIMPLE_OPERATORS = EQUALITY_OPERATORS + RANGE_OPERATORS
ALL_OPERATORS = (*SIMPLE_OPERATORS, REGEX_OPERATOR)


SUPPORTED_OPERATORS = {
    # ALL_OPERATORS
    Parser.BRANCH: ALL_OPERATORS,
    Parser.LOGIN_AND_TEAMS: ALL_OPERATORS,
    Parser.TEXT: ALL_OPERATORS,
    Parser.WORD: ALL_OPERATORS,
    # SIMPLE_OPERATORS
    Parser.NUMBER: SIMPLE_OPERATORS,
    Parser.PERMISSION: SIMPLE_OPERATORS,
    Parser.POSITIVE_NUMBER: SIMPLE_OPERATORS,
    # EQUALITY_OPERATORS
    Parser.ENUM: EQUALITY_OPERATORS,
    Parser.SCHEDULE: EQUALITY_OPERATORS,
    # RANGE_OPERATORS
    Parser.TIMESTAMP: SIMPLE_OPERATORS,
    Parser.TIMESTAMP_OR_TIMEDELTA: RANGE_OPERATORS,
}

INVALID_BRANCH_CHARS = "~^: []\\"

GITHUB_LOGIN_CHARS = string.ascii_letters + string.digits + "-[]_"
GITHUB_LOGIN_AND_TEAM_CHARS = GITHUB_LOGIN_CHARS + "@/"

JINJA_ENV = jinja2.sandbox.SandboxedEnvironment(
    undefined=jinja2.StrictUndefined, enable_async=True
)


def _to_dict(
    negate: bool,
    quantity: bool,
    attribute: str,
    operator: str,
    value: typing.Any,
) -> filter.TreeT:
    if quantity:
        attribute = f"#{attribute}"
    d = typing.cast(filter.TreeT, {operator: (attribute, value)})
    if negate:
        return filter.TreeT({"-": d})
    return d


def _unquote(value: str) -> str:
    if not value:
        return value

    if (
        (value[0] == "'" and value[-1] != "'")
        or (value[0] == '"' and value[-1] != '"')
        or (value[0] != "'" and value[-1] == "'")
        or (value[0] != '"' and value[-1] == '"')
    ):
        raise ConditionParsingError("Unbalanced quotes")

    if (
        (value[0] == '"' and value[-1] == '"')
        or (value[0] == "'" and value[-1] == "'")
        and len(value) >= 2
    ):
        return value[1:-1]

    return value


def parse_schedule(string: str) -> date.Schedule:
    try:
        return date.Schedule.from_string(_unquote(string))
    except date.InvalidDate as e:
        raise ConditionParsingError(e.message)


def _skip_ws(v: str, length: int, position: int) -> int:
    while position < length and v[position] == " ":
        position += 1
    return position


class ParsedCondition(typing.NamedTuple):
    attribute: str
    operator: str
    condition_value: str
    parser: Parser
    negate: bool
    quantity: bool


def parse_raw_condition(
    cond: str, allow_command_attributes: bool = False
) -> ParsedCondition:
    length = len(cond)
    position = _skip_ws(cond, length, 0)
    if position >= length:
        raise ConditionParsingError("Condition empty")

    # Search for modifiers
    negate = False
    quantity = False
    if cond[position] in NEGATION_OPERATORS:
        negate = True
        position += 1
    elif cond[position] in POSITIVE_OPERATORS:
        position += 1

    position = _skip_ws(cond, length, position)
    if position >= length:
        raise ConditionParsingError("Incomplete condition")

    if cond[position] == "#":
        quantity = True
        position = _skip_ws(cond, length, position + 1)
        if position >= length:
            raise ConditionParsingError("Incomplete condition")

    supported_attributes = ATTRIBUTES.copy()
    # Get the attribute
    for attribute in supported_attributes:
        if cond[position:].startswith(attribute):
            break
    else:
        raise ConditionParsingError("Invalid attribute")

    position = _skip_ws(cond, length, position + len(attribute))

    if not quantity and attribute in ATTRIBUTES_WITH_ONLY_LENGTH:
        raise ConditionParsingError(
            f"`#` modifier is required for attribute: `{attribute}`"
        )

    if not allow_command_attributes and attribute in COMMAND_ONLY_ATTRIBUTES:
        raise ConditionParsingError(
            "Attribute only allowed in commands_restrictions section"
        )

    # Get the type of parser
    parser = CONDITION_PARSERS[attribute]

    # Check if the condition is using an attribute's array
    if attribute in PARSERS_FOR_ARRAY_SUBATTRIBUTES:
        attribute_array_match = re.match(
            rf"{attribute}(\[(?:-?\d+|\*)\]\.(\w+))",
            cond[position - len(attribute) :],
        )
        if attribute_array_match is not None:
            array_subattribute = attribute_array_match.group(2)
            if (
                array_subattribute
                not in PARSERS_FOR_ARRAY_SUBATTRIBUTES[attribute].keys()
            ):
                raise ConditionParsingError(
                    f"`{array_subattribute}` is not a valid sub-attribute for `{attribute}`"
                )

            parser = PARSERS_FOR_ARRAY_SUBATTRIBUTES[attribute][array_subattribute]

            # Skip the "[\d].{subattribute}" part
            position = _skip_ws(cond, length, len(attribute_array_match.group(0)))
            if negate:
                position += 1
            # Set the attribute to be `{attribute}[\d].{subattribute}`
            attribute = attribute_array_match.group(0)

    # Check modifiers
    negate_allowed, quantity_allowed = PARSER_MODIFIERS.get(parser, (True, True))
    if negate and not negate_allowed:
        raise ConditionParsingError(
            f"`-` modifier is invalid for attribute: `{attribute}`"
        )
    if quantity and not quantity_allowed:
        raise ConditionParsingError(
            f"`#` modifier is invalid for attribute: `{attribute}`"
        )

    if parser == Parser.BOOL:
        # Bool doesn't have operators
        if len(cond[position:].strip()) > 0:
            raise ConditionParsingError(
                f"Operators are invalid for Boolean attribute: `{attribute}`"
            )
        return ParsedCondition(
            attribute=attribute,
            operator="=",
            condition_value=attribute,
            parser=parser,
            negate=negate,
            quantity=quantity,
        )

    # Extract operators
    operators = SUPPORTED_OPERATORS[parser]
    for op in operators:
        if cond[position:].startswith(op):
            break
    else:
        raise ConditionParsingError("Invalid operator")

    position += len(op)
    value = cond[position:].strip()
    op = OPERATOR_ALIASES.get(op, op)
    return ParsedCondition(
        attribute=attribute,
        operator=op,
        condition_value=value,
        parser=parser,
        negate=negate,
        quantity=quantity,
    )


def parse(v: str, allow_command_attributes: bool = False) -> typing.Any:
    attribute, op, value, parser, negate, quantity = parse_raw_condition(
        v, allow_command_attributes
    )

    if parser == Parser.BOOL:
        return _to_dict(negate, False, attribute, op, True)

    if parser == Parser.SCHEDULE:
        cond: dict[str, typing.Any] = {op: ("current-datetime", parse_schedule(value))}
        return _to_dict(False, False, attribute, "@", cond)

    if parser in (Parser.TIMESTAMP, Parser.TIMESTAMP_OR_TIMEDELTA):
        value = _unquote(value)
        if parser == Parser.TIMESTAMP_OR_TIMEDELTA:
            try:
                rd = date.RelativeDatetime.from_string(value)
            except date.InvalidDate:
                pass
            else:
                return _to_dict(False, False, f"{attribute}-relative", op, rd)

        try:
            dtr = date.DateTimeRange.fromisoformat_with_zoneinfo(value)
        except date.InvalidDate:
            pass
        else:
            if op not in EQUALITY_OPERATORS:
                raise ConditionParsingError("Invalid operator")
            return _to_dict(False, False, attribute, op, dtr)

        try:
            d = date.fromisoformat_with_zoneinfo(value)
        except date.InvalidDate as e:
            raise ConditionParsingError(e.message)

        if op not in RANGE_OPERATORS:
            raise ConditionParsingError("Invalid operator")
        return _to_dict(False, False, attribute, op, d)

    if parser in (
        Parser.NUMBER,
        Parser.POSITIVE_NUMBER,
    ):
        try:
            number = int(value)
        except ValueError:
            raise ConditionParsingError(f"{value} is not a number")

        if parser == Parser.POSITIVE_NUMBER and number < 0:
            raise ConditionParsingError("Value must be positive")
        return _to_dict(negate, False, attribute, op, number)

    if parser in (
        Parser.BRANCH,
        Parser.ENUM,
        Parser.LOGIN_AND_TEAMS,
        Parser.TEXT,
        Parser.WORD,
    ):
        if (
            parser == Parser.LOGIN_AND_TEAMS
            and is_github_team_name(value)
            and op not in SIMPLE_OPERATORS
        ):
            raise ConditionParsingError(
                "Regular expression are not supported for team slug"
            )

        if quantity:
            try:
                number = int(value)
            except ValueError:
                raise ConditionParsingError(f"{value} is not a number")
            return _to_dict(negate, True, attribute, op, number)

        if is_jinja_template(value):
            template = get_jinja_template_wrapper(_unquote(value))
            return _to_dict(negate, quantity, attribute, op, template)

        if op == REGEX_OPERATOR:
            try:
                # TODO(sileht): we can keep the compiled version, so the
                # Filter() doesn't have to (re)compile it.
                re.compile(value)
            except re.error as e:
                raise ConditionParsingError(f"Invalid regular expression: {e!s}")
            return _to_dict(negate, quantity, attribute, op, value)

        if parser == Parser.TEXT:
            value = _unquote(value)
        elif parser == Parser.ENUM:
            value = _unquote(value)
            if value not in CONDITION_ENUMS[attribute]:
                raise ConditionParsingError(
                    f"Invalid `{attribute}` value, must be one of `{'`, `'.join(CONDITION_ENUMS[attribute])}`"
                )
        elif parser == Parser.WORD:
            if " " in value:
                raise ConditionParsingError(f"Invalid `{attribute}` format")
        elif parser == Parser.BRANCH:
            for char in INVALID_BRANCH_CHARS:
                if char in value:
                    raise ConditionParsingError("Invalid branch name")
        elif parser == Parser.LOGIN_AND_TEAMS:
            if is_github_team_name(value):
                validate_github_team_name(value)
            else:
                validate_github_login(value)

        return _to_dict(negate, quantity, attribute, op, value)

    if parser == Parser.PERMISSION:
        try:
            permission = github_types.GitHubRepositoryPermission(value)
        except ValueError as e:
            raise ConditionParsingError(str(e))
        return _to_dict(negate, False, attribute, op, permission)

    raise RuntimeError(f"unhandled parser: {parser}")


def is_jinja_template(value: str) -> bool:
    return "{{" in value or "{%" in value or "{#" in value


def get_jinja_template_wrapper(value: str) -> filter.JinjaTemplateWrapper:
    try:
        template = JINJA_ENV.parse(value)
    except jinja2.exceptions.TemplateError:
        raise ConditionParsingError("Invalid template")

    used_variables = jinja2.meta.find_undeclared_variables(template)
    available_variables = set(CONDITION_PARSERS.keys())
    unexpected_variables = used_variables - available_variables

    if unexpected_variables:
        raise ConditionParsingError(
            f"Invalid template, unexpected variables {unexpected_variables}"
        )

    return filter.JinjaTemplateWrapper(JINJA_ENV, value, used_variables)


def is_github_team_name(value: str) -> bool:
    return value.startswith("@")


def validate_github_team_name(value: str) -> None:
    if value.count("@") > 1 or value.count("/") > 1:
        raise ConditionParsingError("Invalid GitHub team name")

    for char in value:
        if char not in GITHUB_LOGIN_AND_TEAM_CHARS:
            raise ConditionParsingError("Invalid GitHub team name")


def validate_github_login(value: str) -> None:
    for char in value:
        if char not in GITHUB_LOGIN_CHARS:
            raise ConditionParsingError("Invalid GitHub login")
