from __future__ import annotations

import curses.ascii
import dataclasses
import re
import typing

import voluptuous

from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import partition_rules as partr_config
    from mergify_engine.rules.config import priority_rules as pr_config
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config

T_Rule = typing.TypeVar(
    "T_Rule",
    "prr_config.PullRequestRule",
    "qr_config.QueueRule",
    "pr_config.PriorityRule",
    "partr_config.PartitionRule",
)
T_EvaluatedRule = typing.TypeVar(
    "T_EvaluatedRule",
    "prr_config.EvaluatedPullRequestRule",
    "qr_config.EvaluatedQueueRule",
    "pr_config.EvaluatedPriorityRule",
    "partr_config.EvaluatedPartitionRule",
)


@dataclasses.dataclass
class LineColumnPath:
    line: int
    column: int | None = None

    def __repr__(self) -> str:
        if self.column is None:
            return f"line {self.line}"
        return f"line {self.line}, column {self.column}"


def BranchName(value: typing.Any) -> str:
    if value is None:
        raise voluptuous.Invalid("Branch name cannot be null")
    if not isinstance(value, str):
        raise voluptuous.Invalid("Branch name must be a string")

    # NOTE(sileht): From https://git-scm.com/docs/git-check-ref-format

    # From 1.
    if value.startswith(".") or value.endswith(".lock"):
        raise voluptuous.Invalid("Branch name is invalid")

    for forbidden in (
        "..",  # From 3.,
        "^",  # From 4.,
        "~",  # From 4.,
        ":",  # From 4.,
        " ",  # From 4.,
        "?",  # From 5.
        "*",  # From 5.
        "[",  # From 5.
        "//",  # From 6.
        "@{",  # From 8.
        "\\",  # From 10.
    ):
        if forbidden in value:
            raise voluptuous.Invalid("Branch name is invalid")

    # From 4.
    for char in value:
        if curses.ascii.iscntrl(char):
            raise voluptuous.Invalid("Branch name is invalid")

    # From 6.
    if value.startswith("/") or value.endswith("/"):
        raise voluptuous.Invalid("Branch name is invalid")

    # From 7.
    if value.endswith("."):
        raise voluptuous.Invalid("Branch name is invalid")

    # From 9.
    if value == "@":
        raise voluptuous.Invalid("Branch name is invalid")

    return value


def Jinja2(
    value: typing.Any,
    extra_variables: dict[str, typing.Any] | None = None,
) -> str | None:
    """A Jinja2 type for voluptuous Schemas."""
    if value is None:
        raise voluptuous.Invalid("Template cannot be null")
    if not isinstance(value, str):
        raise voluptuous.Invalid("Template must be a string")

    # Postpone loading of context here to avoid rules module depending on context module
    # as we make this dependency only for typing
    from mergify_engine.rules import types_dummy_context

    try:
        # TODO: optimize this by returning, storing and using the parsed Jinja2 AST
        types_dummy_context.DUMMY_PR.render_template(value, extra_variables)
    except types_dummy_context.RenderTemplateFailureError as rtf:
        path = None if rtf.lineno is None else [LineColumnPath(rtf.lineno, None)]
        raise voluptuous.Invalid(
            "Template syntax error",
            error_message=str(rtf),
            path=path,
        )
    return value


def Jinja2WithNone(
    value: str | None,
    extra_variables: dict[str, typing.Any] | None = None,
) -> str | None:
    if value is None:
        return None

    return Jinja2(value, extra_variables)


def _check_GitHubLogin_format(
    value: str | None,
    _type: typing.Literal["login", "organization"] = "login",
) -> github_types.GitHubLogin:
    # GitHub says login cannot:
    # - start with an hyphen
    # - ends with an hyphen
    # - contains something else than hyphen and alpha numericals characters
    if not value:
        raise voluptuous.Invalid(f"A GitHub {_type} cannot be an empty string")
    if (
        value[0] == "-"
        or value[-1] == "-"
        or not value.isascii()
        or not value.replace("-", "").replace("_", "").isalnum()
    ):
        raise voluptuous.Invalid(f"GitHub {_type} contains invalid characters: {value}")
    return github_types.GitHubLogin(value)


GitHubLogin = voluptuous.All(str, _check_GitHubLogin_format)


@dataclasses.dataclass
class InvalidTeamError(Exception):
    details: str


@dataclasses.dataclass(unsafe_hash=True)
class _GitHubTeam:
    team: github_types.GitHubTeamSlug
    organization: github_types.GitHubLogin | None
    raw: str

    @classmethod
    def from_string(cls, value: str) -> _GitHubTeam:
        if not value:
            raise voluptuous.Invalid("A GitHub team cannot be an empty string")

        # Remove leading @ if any:
        # This format is accepted in conditions so we're happy to accept it here too.
        if value[0] == "@":
            org, sep, team = value[1:].partition("/")
        else:
            org, sep, team = value.partition("/")

        if not sep and not team:
            # Just a slug
            team = org
            final_org = None
        else:
            final_org = _check_GitHubLogin_format(org, "organization")

        if not team:
            raise voluptuous.Invalid("A GitHub team cannot be an empty string")

        if (
            "/" in team
            or team[0] == "-"
            or team[-1] == "-"
            or not team.isascii()
            or not team.replace("-", "").replace("_", "").isalnum()
        ):
            raise voluptuous.Invalid("GitHub team contains invalid characters")

        return cls(github_types.GitHubTeamSlug(team), final_org, value)

    async def has_read_permission(
        self,
        ctxt: context.Context,
    ) -> None:
        expected_organization = ctxt.pull["base"]["repo"]["owner"]["login"]
        if self.organization is not None and self.organization != expected_organization:
            raise InvalidTeamError(
                f"Team `{self.raw}` is not part of the organization `{expected_organization}`",
            )

        if not await ctxt.repository.team_has_read_permission(self.team):
            raise InvalidTeamError(
                f"Team `{self.raw}` does not exist or has not access to this repository",
            )


GitHubTeam = voluptuous.All(str, voluptuous.Coerce(_GitHubTeam.from_string))

# NOTE(sileht): cf the error message from GitHub repository creation:
# The repository name can only contain ASCII letters, digits, and the characters ., -, and _.


def check_forbidden_repository_name(v: str) -> str:
    if v in {".", "..", ".git"}:
        raise voluptuous.Invalid(f"Repository name '{v}' is forbidden")
    return v


GitHubRepositoryName = voluptuous.All(
    str,
    voluptuous.Length(min=1),
    voluptuous.Match(re.compile(r"^[\w\-.]+$")),
    check_forbidden_repository_name,
)


def ListOfAnything(v: typing.Any) -> typing.Any:
    # NOTE(sileht): we can't just use `list` as voluptuous will pass `v` to `list()`
    # This is a lighter version that just check the type
    if isinstance(v, list):
        return v
    raise voluptuous.Invalid("expected a list")


def DictOfAnything(v: typing.Any) -> typing.Any:
    # NOTE(sileht): we can't just use `dict` as voluptuous will pass `v` to `dict()`
    # This is a lighter version that just check the type
    if isinstance(v, dict):
        return v
    raise voluptuous.Invalid("expected a dictionnary")


def ListOf(value: typing.Any, _max: int, _min: int = 0) -> typing.Any:
    # NOTE(sileht): to not get DDoS with the yaml billion laughs attack,
    # we limit and check the size of the list before browsing the content that could be huge
    return voluptuous.All(
        ListOfAnything,
        voluptuous.Length(min=_min, max=_max),
        [value],
    )


def DictOf(
    key: typing.Any,
    value: typing.Any,
    _max: int,
    _min: int = 0,
) -> typing.Any:
    # NOTE(sileht): to not get DDoS with the yaml billion laughs attack,
    # we limit and check the size of the dict bfore browsing the content that could be huge
    return voluptuous.All(
        DictOfAnything,
        voluptuous.Length(min=_min, max=_max),
        {key: value},
    )
