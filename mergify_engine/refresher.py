#
# Copyright © 2022 Mergify SAS
#
import typing

import daiquiri

from mergify_engine import date
from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from mergify_engine import redis_utils
    from mergify_engine import worker


LOG = daiquiri.getLogger()


async def _send_refresh(
    redis_stream: "redis_utils.RedisStream",
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    source: str,
    pull_request_number: typing.Optional[github_types.GitHubPullRequestNumber] = None,
    ref: typing.Optional[github_types.GitHubRefType] = None,
    priority: typing.Optional["worker.Priority"] = None,
) -> None:
    # Break circular import
    from mergify_engine import github_events
    from mergify_engine import worker

    score = worker.get_priority_score(priority) if priority is not None else None

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
            },
        }
    )

    slim_event = github_events._extract_slim_event("refresh", data)
    await worker.push(
        redis_stream,
        repository["owner"]["id"],
        repository["owner"]["login"],
        repository["id"],
        repository["name"],
        pull_request_number,
        "refresh",
        slim_event,
        score,
    )


async def send_pull_refresh(
    redis_stream: "redis_utils.RedisStream",
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    pull_request_number: github_types.GitHubPullRequestNumber,
    source: str,
    priority: typing.Optional["worker.Priority"] = None,
) -> None:
    LOG.info(
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


async def send_repository_refresh(
    redis_stream: "redis_utils.RedisStream",
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    source: str,
) -> None:

    LOG.info(
        "sending repository refresh",
        gh_owner=repository["owner"]["login"],
        gh_repo=repository["name"],
        gh_private=repository["private"],
        action=action,
        source=source,
    )

    # Break circular import
    from mergify_engine import worker

    await _send_refresh(
        redis_stream, repository, action, source, priority=worker.Priority.low
    )


async def send_branch_refresh(
    redis_stream: "redis_utils.RedisStream",
    repository: github_types.GitHubRepository,
    action: github_types.GitHubEventRefreshActionType,
    ref: github_types.GitHubRefType,
    source: str,
) -> None:
    LOG.info(
        "sending repository branch refresh",
        gh_owner=repository["owner"]["login"],
        gh_repo=repository["name"],
        gh_private=repository["private"],
        gh_ref=ref,
        action=action,
        source=source,
    )
    # Break circular import
    from mergify_engine import worker

    await _send_refresh(
        redis_stream, repository, action, source, ref=ref, priority=worker.Priority.low
    )
