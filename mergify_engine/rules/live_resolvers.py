from __future__ import annotations

import dataclasses
import functools
import itertools
import typing

from mergify_engine import github_types
from mergify_engine.clients import http


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules import conditions as conditions_mod
    from mergify_engine.rules import filter


@dataclasses.dataclass
class LiveResolutionFailure(Exception):
    reason: str


async def _resolve_login(
    repository: context.Repository,
    name: str,
) -> list[github_types.GitHubLogin]:
    if not name:
        return []

    if not isinstance(name, str):  # runtime seatbelt
        return [github_types.GitHubLogin(name)]  # type: ignore[unreachable]

    if name[0] != "@":
        return [github_types.GitHubLogin(name)]

    if "/" in name:
        organization, _, team_slug = name.partition("/")
        if not team_slug or "/" in team_slug:
            raise LiveResolutionFailure(f"Team `{name}` is invalid")
        organization = github_types.GitHubLogin(organization[1:])
        expected_organization = repository.repo["owner"]["login"]
        if organization != expected_organization:
            raise LiveResolutionFailure(
                f"Team `{name}` is not part of the organization `{expected_organization}`",
            )
        team_slug = github_types.GitHubTeamSlug(team_slug)
    else:
        team_slug = github_types.GitHubTeamSlug(name[1:])

    try:
        return await repository.installation.get_team_members(team_slug)
    except http.HTTPNotFound:
        raise LiveResolutionFailure(f"Team `{name}` does not exist")
    except http.HTTPClientSideError as e:
        repository.log.warning(
            "fail to get the organization, team or members",
            team=name,
            status_code=e.status_code,
            detail=e.message,
        )
        raise LiveResolutionFailure(
            f"Failed retrieve team `{name}`, details: {e.message}",
        )


async def teams(
    repository: context.Repository,
    values: list[str] | tuple[str] | str | None,
) -> list[github_types.GitHubLogin]:
    if not values:
        return []
    # FIXME(sileht): This should not belong here, we should accept only a List[str]
    if not isinstance(values, list | tuple):
        values = [values]

    return list(
        itertools.chain.from_iterable(
            [await _resolve_login(repository, value) for value in values],
        ),
    )


_TEAM_ATTRIBUTES = (
    "author",
    "sender",
    "merged_by",
    "approved-reviews-by",
    "dismissed-reviews-by",
    "commented-reviews-by",
    "changes-requested-reviews-by",
)


def configure_filter(
    repository: context.Repository,
    f: filter.Filter[filter.FilterResultT],
) -> None:
    for attrib in _TEAM_ATTRIBUTES:
        f.value_expanders[attrib] = functools.partial(  # type: ignore[assignment]
            teams,
            repository,
        )


def apply_configure_filter(
    repository: context.Repository,
    conditions: (
        conditions_mod.PullRequestRuleConditions
        | conditions_mod.QueueRuleMergeConditions
        | conditions_mod.PriorityRuleConditions
        | conditions_mod.PartitionRuleConditions
    ),
) -> None:
    for condition in conditions.walk():
        configure_filter(repository, condition.filters.boolean)
        configure_filter(repository, condition.filters.next_evaluation)
        if condition.filters.related_checks is not None:
            configure_filter(repository, condition.filters.related_checks)
