import functools
import typing

import daiquiri
import httpx
import msgpack

from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import http


LOG = daiquiri.getLogger(__name__)

EVENT_FORWARDER_REDIS_KEY = "event-forwarder"
EVENT_FORWARDER_BATCH_SIZE = 100


class ForwardInfo(typing.TypedDict):
    body: bytes
    headers: dict[str, str]


async def push(
    redis_links: redis_utils.RedisLinks,
    body: bytes,
    headers: dict[str, str],
) -> None:
    info = ForwardInfo({"body": body, "headers": headers})
    await redis_links.stream.xadd(
        EVENT_FORWARDER_REDIS_KEY,
        {"info": msgpack.packb(info)},
    )


async def forward(redis_links: redis_utils.RedisLinks) -> bool:
    if settings.GITHUB_WEBHOOK_FORWARD_URL is None:
        return False  # type: ignore[unreachable]

    async with http.AsyncClient(
        base_url=settings.GITHUB_WEBHOOK_FORWARD_URL,
    ) as client:
        return await redis_utils.process_stream(
            "event-forwarder",
            redis_links.stream,
            redis_key=EVENT_FORWARDER_REDIS_KEY,
            batch_size=EVENT_FORWARDER_BATCH_SIZE,
            event_processor=functools.partial(forward_event, client=client),
        )


async def forward_event(
    event_id: bytes,
    event: dict[bytes, bytes],
    client: http.AsyncClient,
) -> None:
    github_event = msgpack.unpackb(event[b"info"])
    try:
        await client.post(
            "",
            content=github_event["body"],
            headers={
                "X-GitHub-Event": github_event["headers"]["x-github-event"],
                "X-GitHub-Delivery": github_event["headers"]["x-github-delivery"],
                "X-Hub-Signature": github_event["headers"]["x-hub-signature"],
                "User-Agent": github_event["headers"]["user-agent"],
                "Content-Type": github_event["headers"]["content-type"],
            },
        )
    except httpx.HTTPError:
        LOG.warning(
            "Fail to forward GitHub github_event",
            github_event_type=github_event["headers"]["x-github-event"],
            github_event_id=github_event["headers"]["x-github-delivery"],
        )
