from __future__ import annotations

import dataclasses
import enum
import typing

import daiquiri
import msgpack

from mergify_engine import date
from mergify_engine import filtered_github_types
from mergify_engine import github_types
from mergify_engine import worker_pusher
from mergify_engine.worker import stream_lua


if typing.TYPE_CHECKING:
    from mergify_engine import redis_utils
    from mergify_engine.models.github import repository as github_repository


@dataclasses.dataclass
class MaxRefreshAttemptsExceededError(Exception):
    max_attempts: int


LOG = daiquiri.getLogger()


class RefreshFlag(enum.StrEnum):
    MERGE_FAILED = "merge_failed"


async def _add_refresh_attempt(
    redis_stream: redis_utils.RedisStream | redis_utils.PipelineStream,
    repository: github_types.GitHubRepository | github_repository.GitHubRepositoryDict,
    pull_request_number: github_types.GitHubPullRequestNumber | None = None,
    refresh_flag: RefreshFlag | None = None,
    max_attempts: int | None = None,
) -> int | None:
    if max_attempts is None:
        return None

    bucket_sources_key = stream_lua.get_bucket_sources_key(
        repository["id"],
        pull_request_number,
    )

    attempts = 1
    events = await redis_stream.xrevrange(bucket_sources_key)
    for event in events:
        event_unpacked = msgpack.unpackb(event[1][b"source"])
        if event_unpacked["event_type"] != "refresh":
            continue
        refresh_data = typing.cast(
            github_types.GitHubEventRefresh,
            event_unpacked["data"],
        )
        if (
            refresh_data["action"] == "internal"
            and refresh_data is not None
            and refresh_data.get("flag") == refresh_flag
        ):
            attempts = (
                refresh_data["attempts"]
                if refresh_data["attempts"] is not None
                else attempts
            )
            if attempts >= max_attempts:
                raise MaxRefreshAttemptsExceededError(max_attempts)
            attempts += 1
            # NOTE(lecrepont01): first refresh event if many is the most recent
            break
    return attempts


async def _send_refresh(
    redis_stream: redis_utils.RedisStream | redis_utils.PipelineStream,
    repository: github_types.GitHubRepository | github_repository.GitHubRepositoryDict,
    action: github_types.GitHubEventRefreshActionType,
    source: str,
    pull_request_number: github_types.GitHubPullRequestNumber | None = None,
    ref: github_types.GitHubRefType | None = None,
    priority: worker_pusher.Priority = worker_pusher.Priority.high,
    refresh_flag: RefreshFlag | None = None,
    max_attempts: int | None = None,
) -> None:
    attempts = await _add_refresh_attempt(
        redis_stream,
        repository,
        pull_request_number,
        refresh_flag,
        max_attempts,
    )

    data = github_types.GitHubEventRefresh(
        {
            "received_at": github_types.ISODateTimeType(date.utcnow().isoformat()),
            "action": action,
            "source": source,
            "ref": ref,
            "pull_request_number": pull_request_number,
            "repository": typing.cast(github_types.GitHubRepository, repository),
            "sender": {
                "login": github_types.GitHubLogin("<internal>"),
                "id": github_types.GitHubAccountIdType(0),
                "type": "User",
                "avatar_url": "",
            },
            "organization": typing.cast(
                github_types.GitHubAccount,
                repository["owner"],
            ),
            "installation": {
                "id": github_types.GitHubInstallationIdType(0),
                "account": typing.cast(github_types.GitHubAccount, repository["owner"]),
                "target_type": repository["owner"]["type"],
                "permissions": {},
                "suspended_at": None,
            },
            "flag": refresh_flag,
            "attempts": attempts,
        },
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
    repository: github_types.GitHubRepository | github_repository.GitHubRepositoryDict,
    action: github_types.GitHubEventRefreshActionType,
    pull_request_number: github_types.GitHubPullRequestNumber,
    source: str,
    priority: worker_pusher.Priority = worker_pusher.Priority.high,
    refresh_flag: RefreshFlag | None = None,
    max_attempts: int | None = None,
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
        refresh_flag=refresh_flag,
        max_attempts=max_attempts,
    )
    await _send_refresh(
        redis_stream,
        repository,
        action,
        source,
        pull_request_number=pull_request_number,
        priority=priority,
        refresh_flag=refresh_flag,
        max_attempts=max_attempts,
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
