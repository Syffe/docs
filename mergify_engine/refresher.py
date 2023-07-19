from __future__ import annotations

import typing

import daiquiri

from mergify_engine import date
from mergify_engine import filtered_github_types
from mergify_engine import github_types
from mergify_engine import worker_pusher


if typing.TYPE_CHECKING:
    from mergify_engine import redis_utils


LOG = daiquiri.getLogger()


async def _send_refresh(
    redis_stream: redis_utils.RedisStream | redis_utils.PipelineStream,
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    source: str,
    pull_request_number: github_types.GitHubPullRequestNumber | None = None,
    ref: github_types.GitHubRefType | None = None,
    priority: worker_pusher.Priority = worker_pusher.Priority.high,
) -> None:
    data = github_types.GitHubEventRefresh(
        {
            "received_at": github_types.ISODateTimeType(date.utcnow().isoformat()),
            "action": action,
            "source": source,
            "ref": ref,
            "pull_request_number": pull_request_number,
            "repository": repository,
            "sender": {
                "login": github_types.GitHubLogin("<internal>"),
                "id": github_types.GitHubAccountIdType(0),
                "type": "User",
                "avatar_url": "",
            },
            "organization": repository["owner"],
            "installation": {
                "id": github_types.GitHubInstallationIdType(0),
                "account": repository["owner"],
                "target_type": repository["owner"]["type"],
                "permissions": {},
                "suspended_at": None,
            },
        }
    )

    slim_event = filtered_github_types.extract("refresh", None, data)
    await worker_pusher.push(
        redis_stream,
        repository["owner"]["id"],
        repository["owner"]["login"],
        repository["id"],
        repository["name"],
        pull_request_number,
        "refresh",
        slim_event,
        priority,
    )


async def send_pull_refresh(
    redis_stream: redis_utils.RedisStream | redis_utils.PipelineStream,
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    pull_request_number: github_types.GitHubPullRequestNumber,
    source: str,
    priority: worker_pusher.Priority = worker_pusher.Priority.high,
) -> None:
    LOG.debug(
        "sending pull refresh",
        gh_owner=repository["owner"]["login"],
        gh_repo=repository["name"],
        gh_private=repository["private"],
        gh_pull=pull_request_number,
        action=action,
        source=source,
        priority=priority,
    )

    await _send_refresh(
        redis_stream,
        repository,
        action,
        source,
        pull_request_number=pull_request_number,
        priority=priority,
    )


async def send_branch_refresh(
    redis_stream: redis_utils.RedisStream,
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    ref: github_types.GitHubRefType,
    source: str,
) -> None:
    LOG.debug(
        "sending repository branch refresh",
        gh_owner=repository["owner"]["login"],
        gh_repo=repository["name"],
        gh_private=repository["private"],
        gh_ref=ref,
        action=action,
        source=source,
    )

    await _send_refresh(
        redis_stream,
        repository,
        action,
        source,
        ref=ref,
        priority=worker_pusher.Priority.low,
    )
