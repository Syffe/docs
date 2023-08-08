import datetime
import typing

import daiquiri
from ddtrace import tracer

from mergify_engine import github_types
from mergify_engine import redis_utils


LOG = daiquiri.getLogger(__name__)

BucketOrgKeyType = typing.NewType("BucketOrgKeyType", str)
BucketSourcesKeyType = typing.NewType("BucketSourcesKeyType", str)


def get_bucket_org_key(owner_id: github_types.GitHubAccountIdType) -> BucketOrgKeyType:
    return BucketOrgKeyType(f"bucket~{owner_id}")


def get_bucket_sources_key(
    repo_id: github_types.GitHubRepositoryIdType,
    pull_number: github_types.GitHubPullRequestNumber | None,
) -> BucketSourcesKeyType:
    return BucketSourcesKeyType(f"bucket-sources~{repo_id}~{pull_number or 0}")


PUSH_PR_SCRIPT = redis_utils.register_script(
    """
local bucket_org_key = KEYS[1]
local bucket_sources_key = KEYS[2]
local scheduled_at_timestamp = ARGV[1]
local source = ARGV[2]
local score = ARGV[3]
local repo_name = ARGV[4]

-- Add the source to the pull request sources
redis.call("XADD", bucket_sources_key, "*", "source", source, "score", score, "repo_name", repo_name)
-- Add this pull request to the org bucket if not exists
-- REDIS 6.2:
-- redis.call("ZADD", bucket_org_key, "LT", score, bucket_sources_key)
-- REDIS < 6.2:
local old_score = tonumber(redis.call("ZSCORE", bucket_org_key, bucket_sources_key))
if (old_score == nil) or (tonumber(score) < old_score) then
   redis.call("ZADD", bucket_org_key, score, bucket_sources_key)
end
-- Add the org bucket to the stream list
redis.call("ZADD", "streams", "NX", scheduled_at_timestamp, bucket_org_key)
"""
)


@tracer.wrap("stream_push_pull", span_type="worker")
async def push_pull(
    redis: redis_utils.RedisStream | redis_utils.PipelineStream,
    bucket_org_key: BucketOrgKeyType,
    bucket_sources_key: BucketSourcesKeyType,
    tracing_repo_name: github_types.GitHubRepositoryNameForTracing,
    scheduled_at: datetime.datetime,
    source: str,
    score: str,
) -> None:
    await redis_utils.run_script(
        redis,
        PUSH_PR_SCRIPT,
        (
            bucket_org_key,
            bucket_sources_key,
        ),
        (
            str(scheduled_at.timestamp()),
            source,
            score,
            tracing_repo_name,
        ),
    )


GET_PR_MESSAGES_SCRIPT = redis_utils.register_script(
    """
local bucket_org_key = KEYS[1]
local bucket_sources_key = KEYS[2]
local score_offset = tonumber(KEYS[3])
local sources = redis.call("XRANGE", bucket_sources_key, "-", "+")
local score = redis.call("ZSCORE", bucket_org_key, bucket_sources_key)
redis.call("ZADD", bucket_org_key, score + score_offset, bucket_sources_key)
return sources
"""
)


def lua_table_to_dict(table: list[bytes]) -> dict[bytes, bytes]:
    it = iter(table)
    return dict(zip(it, it, strict=True))


@tracer.wrap("stream_get_pull_messages", span_type="worker")
async def get_pull_messages(
    redis: redis_utils.RedisStream,
    bucket_org_key: BucketOrgKeyType,
    bucket_sources_key: BucketSourcesKeyType,
    score_offset: int,
) -> list[tuple[bytes, dict[bytes, bytes]]]:
    """Get all messages for a pull requests and program next run with score_offset"""

    messages = await redis_utils.run_script(
        redis,
        GET_PR_MESSAGES_SCRIPT,
        (
            bucket_org_key,
            bucket_sources_key,
            str(score_offset),
        ),
    )
    return [
        (message_id, lua_table_to_dict(message)) for message_id, message in messages
    ]


REMOVE_PR_SCRIPT = redis_utils.register_script(
    """
local bucket_org_key = KEYS[1]
local bucket_sources_key = KEYS[2]
-- Delete all sources we have handled
local step = 1000
for i = 1, #ARGV, step do
    redis.call("XDEL", bucket_sources_key, unpack(ARGV, i, math.min(i + step - 1, #ARGV)))
end
local sources = redis.call("XRANGE", bucket_sources_key, "-", "+", "COUNT", 1)
if table.getn(sources) == 0 then
    redis.call("DEL", bucket_sources_key)
    -- No new source, drop this pull request bucket
    redis.call("ZREM", bucket_org_key, bucket_sources_key)
    -- No need to clean "streams" key, CLEAN_STREAM_SCRIPT is always
    -- called at the end and it will do it
end
"""
)


@tracer.wrap("stream_remove_pull", span_type="worker")
async def remove_pull(
    redis: redis_utils.RedisStream,
    bucket_org_key: BucketOrgKeyType,
    bucket_sources_key: BucketSourcesKeyType,
    message_ids: tuple[str, ...],
) -> None:
    await redis_utils.run_script(
        redis,
        REMOVE_PR_SCRIPT,
        (
            bucket_org_key,
            bucket_sources_key,
        ),
        message_ids,
    )


# TODO(sileht): limited to 7999 keys, should be OK for now, if we have an issue
# just paginate the ZRANGE
DROP_BUCKET_SCRIPT = redis_utils.register_script(
    """
local bucket_org_key = KEYS[1]
local members = redis.call("ZRANGE", bucket_org_key, 0, -1)
redis.call("DEL", unpack(members))
redis.call("DEL", bucket_org_key)
-- No need to clean "streams" key, CLEAN_STREAM_SCRIPT is always
-- called at the end and it will do it
"""
)


@tracer.wrap("stream_drop_bucket", span_type="worker")
async def drop_bucket(
    redis: redis_utils.RedisStream,
    bucket_org_key: BucketOrgKeyType,
) -> None:
    await redis_utils.run_script(redis, DROP_BUCKET_SCRIPT, (bucket_org_key,))


# NOTE(sileht): If the stream/buckets still have events, we update the score to
# reschedule the pull later
CLEAN_STREAM_SCRIPT = redis_utils.register_script(
    """
local bucket_org_key = KEYS[1]
local scheduled_at_timestamp = ARGV[1]
redis.call("HDEL", "attempts", bucket_org_key)
if redis.call("ZCARD", bucket_org_key) == 0 then
    redis.call("ZREM", "streams", bucket_org_key)
else
    redis.call("ZADD", "streams", scheduled_at_timestamp, bucket_org_key)
end
"""
)


@tracer.wrap("stream_clean_org_bucket", span_type="worker")
async def clean_org_bucket(
    redis: redis_utils.RedisStream,
    bucket_org_key: BucketOrgKeyType,
    scheduled_at: datetime.datetime,
) -> None:
    await redis_utils.run_script(
        redis,
        CLEAN_STREAM_SCRIPT,
        (bucket_org_key,),
        (str(scheduled_at.timestamp()),),
    )
