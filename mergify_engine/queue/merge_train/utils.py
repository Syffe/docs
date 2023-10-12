from __future__ import annotations

import dataclasses
import typing

from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from mergify_engine import context


def build_pr_link(
    repository: context.Repository,
    pull_request_number: github_types.GitHubPullRequestNumber,
    label: str | None = None,
) -> str:
    if label is None:
        label = f"#{pull_request_number}"

    return f"[{label}](/{repository.installation.owner_login}/{repository.repo['name']}/pull/{pull_request_number})"


@dataclasses.dataclass
class BaseBranchVanished(Exception):
    branch_name: github_types.GitHubRefType
