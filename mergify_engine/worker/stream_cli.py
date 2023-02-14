import argparse
import asyncio
import datetime
import itertools

from mergify_engine import config
from mergify_engine import date
from mergify_engine import redis_utils
from mergify_engine.worker import stream
from mergify_engine.worker import stream_lua
from mergify_engine.worker import stream_services


async def async_status() -> None:
    shared_stream_tasks_per_process: int = config.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = config.SHARED_STREAM_PROCESSES
    global_shared_tasks_count: int = (
        shared_stream_tasks_per_process * shared_stream_processes
    )

    redis_links = redis_utils.RedisLinks(name="async_status")

    dedicated_worker_owner_ids = await stream.get_dedicated_worker_owner_ids_from_redis(
        redis_links.stream
    )

    def sorter(item: tuple[bytes, float]) -> str:
        org_bucket, score = item
        owner_id = stream_services.StreamService.extract_owner(
            stream_lua.BucketOrgKeyType(org_bucket.decode())
        )
        if owner_id in dedicated_worker_owner_ids:
            return f"dedicated-{owner_id}"
        else:
            return stream_services.SharedStreamService.get_shared_worker_id_for(
                owner_id, global_shared_tasks_count
            )

    org_buckets: list[tuple[bytes, float]] = sorted(
        await redis_links.stream.zrangebyscore(
            "streams", min=0, max="+inf", withscores=True
        ),
        key=sorter,
    )

    for worker_id, org_buckets_by_worker in itertools.groupby(org_buckets, key=sorter):
        for org_bucket, score in org_buckets_by_worker:
            date = datetime.datetime.utcfromtimestamp(score).isoformat(" ", "seconds")
            owner_id = org_bucket.split(b"~")[1]
            event_org_buckets = await redis_links.stream.zrange(org_bucket, 0, -1)
            count = sum([await redis_links.stream.xlen(es) for es in event_org_buckets])
            items = f"{len(event_org_buckets)} pull requests, {count} events"
            print(f"{{{worker_id}}} [{date}] {owner_id.decode()}: {items}")

    await redis_links.shutdown_all()


def status() -> None:
    asyncio.run(async_status())


async def async_reschedule_now() -> int:
    parser = argparse.ArgumentParser(description="Rescheduler for Mergify")
    parser.add_argument("owner_id", help="Organization ID")
    args = parser.parse_args()

    redis_links = redis_utils.RedisLinks(name="async_reschedule_now")
    org_buckets = await redis_links.stream.zrangebyscore("streams", min=0, max="+inf")
    expected_bucket = f"bucket~{args.owner_id}"
    for org_bucket in org_buckets:
        if org_bucket.decode().startswith(expected_bucket):
            scheduled_at = date.utcnow()
            score = scheduled_at.timestamp()
            transaction = await redis_links.stream.pipeline()
            await transaction.hdel(stream.ATTEMPTS_KEY, org_bucket)
            # TODO(sileht): Should we update bucket scores too ?
            await transaction.zadd("streams", {org_bucket.decode(): score})
            # NOTE(sileht): Do we need to cleanup the per PR attempt?
            # await transaction.hdel(ATTEMPTS_KEY, bucket_sources_key)
            await transaction.execute()
            await redis_links.shutdown_all()
            return 0
    else:
        print(f"Stream for {expected_bucket} not found")
        await redis_links.shutdown_all()
        return 1


def reschedule_now() -> int:
    return asyncio.run(async_reschedule_now())
