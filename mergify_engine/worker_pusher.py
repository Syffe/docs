import datetime
import enum
import typing

import daiquiri
from ddtrace import tracer
import msgpack
from redis import exceptions as redis_exceptions
import tenacity

from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.worker import stream_lua


LOG = daiquiri.getLogger(__name__)

WORKER_PROCESSING_DELAY: float = 30


class Priority(enum.IntEnum):
    immediate = 1
    high = 2
    medium = 3
    low = 5


# NOTE(sileht): any score below comes from entry created before we introduce
# offset, the lower score at this times was around 16 557 192 804 (utcnow() * 10)
PRIORITY_OFFSET = 100_000_000_000
SCORE_TIMESTAMP_PRECISION = 10000


def get_priority_score(prio: Priority, offset: datetime.timedelta | None = None) -> str:
    # NOTE(sileht): we drop ms, to avoid float precision issue (eg:
    # 3.99999 becoming 4.0000) that could break priority offset
    when = date.utcnow()
    if offset is not None:
        when += offset
    return str(
        int(when.timestamp() * SCORE_TIMESTAMP_PRECISION)
        + prio.value * PRIORITY_OFFSET * SCORE_TIMESTAMP_PRECISION
    )


def get_priority_level_from_score(score: float) -> Priority:
    if score < PRIORITY_OFFSET * SCORE_TIMESTAMP_PRECISION:
        # NOTE(sileht): backward compatibilty for engine <= 5.0.0
        # prio < 1 so this is score computed before priorities
        return Priority.high
    prio_score = int(score / PRIORITY_OFFSET / SCORE_TIMESTAMP_PRECISION)
    return Priority(prio_score)


def get_date_from_score(score: float) -> datetime.datetime:
    if score < PRIORITY_OFFSET * SCORE_TIMESTAMP_PRECISION:
        # NOTE(sileht): backward compatibility for engine <= 5.0.0
        # prio < 1 so this is score computed before priorities
        # just return a date in the past to handle this event now
        return date.utcnow() - datetime.timedelta(minutes=5)
    timestamp = (
        score % (PRIORITY_OFFSET * SCORE_TIMESTAMP_PRECISION)
    ) / SCORE_TIMESTAMP_PRECISION
    return date.fromtimestamp(timestamp)


def extract_slim_event(
    event_type: str, event_id: str | None, data: typing.Any
) -> typing.Any:
    slim_data = {
        "delivery_id": event_id,
        "received_at": date.utcnow().isoformat(),
        "sender": {
            "id": data["sender"]["id"],
            "login": data["sender"]["login"],
            "type": data["sender"]["type"],
        },
    }

    if event_type == "status":
        # To get PR from sha
        slim_data["sha"] = data["sha"]
        slim_data["context"] = data["context"]
        # NOTE(sileht): only used for logging purpose
        slim_data["state"] = data["state"]

    elif event_type == "pull_request_review":
        # NOTE(sileht): only used for logging purpose
        slim_data["action"] = data["action"]

    elif event_type == "refresh":
        # To get PR from sha or branch name
        slim_data["action"] = data["action"]
        slim_data["ref"] = data["ref"]
        slim_data["pull_request_number"] = data["pull_request_number"]
        slim_data["source"] = data["source"]

    elif event_type == "push":
        # To get PR from sha
        slim_data["ref"] = data["ref"]
        slim_data["before"] = data["before"]
        slim_data["after"] = data["after"]
        slim_data["pusher"] = data["pusher"]

    elif event_type in ("check_suite", "check_run"):
        # To get PR from sha
        slim_data["action"] = data["action"]
        slim_data["app"] = {"id": data[event_type]["app"]["id"]}
        slim_data[event_type] = {
            "head_sha": data[event_type]["head_sha"],
            "pull_requests": [
                {
                    "number": p["number"],
                    "base": {
                        "repo": {
                            "id": p["base"]["repo"]["id"],
                            "url": p["base"]["repo"]["url"],
                        }
                    },
                }
                for p in data[event_type]["pull_requests"]
            ],
        }
        if event_type == "check_run":
            # NOTE(sileht): only used for logging purpose
            slim_data["check_run"]["name"] = data["check_run"]["name"]  # type: ignore
            slim_data["check_run"]["id"] = data["check_run"]["id"]  # type: ignore
            slim_data["check_run"]["conclusion"] = data["check_run"]["conclusion"]  # type: ignore
            slim_data["check_run"]["status"] = data["check_run"]["status"]  # type: ignore

    elif event_type == "pull_request":
        # For pull_request opened/synchronize/closed
        slim_data["action"] = data["action"]
        if slim_data["action"] == "synchronize":
            slim_data["before"] = data["before"]
            slim_data["after"] = data["after"]

    elif event_type == "issue_comment":
        # For commands runner
        slim_data["comment"] = data["comment"]

    return slim_data


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(redis_exceptions.ConnectionError),
    reraise=True,
)
async def push(
    redis: redis_utils.RedisStream,
    owner_id: github_types.GitHubAccountIdType,
    owner_login: github_types.GitHubLogin,
    repo_id: github_types.GitHubRepositoryIdType,
    tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
    pull_number: github_types.GitHubPullRequestNumber | None,
    event_type: github_types.GitHubEventType,
    data: github_types.GitHubEvent,
    priority: Priority | None = None,
    score: str | None = None,
) -> None:
    if score is not None and priority is not None:
        raise RuntimeError("score and prio should not be used at the same time")

    with tracer.trace(
        "push event",
        span_type="worker",
        resource=f"{owner_login}/{tracing_repo_name}/{pull_number}",
    ) as span:
        span.set_tags(
            {
                "gh_owner": owner_login,
                "gh_repo": tracing_repo_name,
                "gh_pull": pull_number,
            }
        )
        now = date.utcnow()

        scheduled_at = now + datetime.timedelta(seconds=WORKER_PROCESSING_DELAY)

        # NOTE(sileht): lower timestamps are processed first
        if score is None:
            if priority is None:
                priority = Priority.high

            if priority is Priority.immediate:
                delay = None
                scheduled_at = now
            else:
                delay = constants.NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST
            score = get_priority_score(priority, delay)

        event = msgpack.packb(
            {
                "event_type": event_type,
                "data": data,
                "timestamp": now.isoformat(),
                "initial_score": float(score),
            },
        )
        bucket_org_key = stream_lua.BucketOrgKeyType(f"bucket~{owner_id}")
        bucket_sources_key = stream_lua.BucketSourcesKeyType(
            f"bucket-sources~{repo_id}~{pull_number or 0}"
        )
        await stream_lua.push_pull(
            redis,
            bucket_org_key,
            bucket_sources_key,
            tracing_repo_name,
            scheduled_at,
            event,
            score,
        )
        LOG.debug(
            "pushed to worker",
            gh_owner=owner_login,
            gh_repo=tracing_repo_name,
            gh_pull=pull_number,
            event_type=event_type,
        )
