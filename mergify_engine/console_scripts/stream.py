import datetime
import itertools

import click

from mergify_engine import date
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.console_scripts import admin_cli
from mergify_engine.worker import shared_workers_spawner_service
from mergify_engine.worker import stream
from mergify_engine.worker import stream_lua
from mergify_engine.worker import stream_service_base


@admin_cli.async_command
async def stream_status() -> None:
    shared_stream_tasks_per_process: int = settings.SHARED_STREAM_TASKS_PER_PROCESS
    shared_stream_processes: int = settings.SHARED_STREAM_PROCESSES
    global_shared_tasks_count: int = (
        shared_stream_tasks_per_process * shared_stream_processes
    )

    redis_links = redis_utils.RedisLinks(name="async_status")

    dedicated_worker_owner_ids = await stream.get_dedicated_worker_owner_ids_from_redis(
        redis_links.stream
    )

    def sorter(item: tuple[bytes, float]) -> str:
        org_bucket, score = item
        owner_id = stream_service_base.StreamService.extract_owner(
            stream_lua.BucketOrgKeyType(org_bucket.decode())
        )
        if owner_id in dedicated_worker_owner_ids:
            return f"dedicated-{owner_id}"

        return (
            shared_workers_spawner_service.SharedStreamService.get_shared_worker_id_for(
                owner_id, global_shared_tasks_count
            )
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
            click.echo(f"{{{worker_id}}} [{date}] {owner_id.decode()}: {items}")

    await redis_links.shutdown_all()


@admin_cli.async_command
@click.argument("owner-id", required=True)
async def stream_reschedule_now(owner_id: str) -> int:
    redis_links = redis_utils.RedisLinks(name="reschedule_now")
    org_buckets = await redis_links.stream.zrangebyscore("streams", min=0, max="+inf")
    expected_bucket = f"bucket~{owner_id}"
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
            click.echo(f"Stream for {expected_bucket} rescheduled now")
            await redis_links.shutdown_all()
            return 0
    else:
        click.echo(f"Stream for {expected_bucket} not found")
        await redis_links.shutdown_all()
        return 1
