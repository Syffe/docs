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
        ids_to_delete: list[bytes] = []
        async for event_id, raw_info in redis_utils.iter_stream(
            redis_links.stream,
            EVENT_FORWARDER_REDIS_KEY,
            batch_size=EVENT_FORWARDER_BATCH_SIZE,
        ):
            ids_to_delete.append(event_id)
            info = typing.cast(ForwardInfo, msgpack.unpackb(raw_info[b"info"]))

            try:
                await client.post(
                    "",
                    content=info["body"],
                    headers={
                        "X-GitHub-Event": info["headers"]["x-github-event"],
                        "X-GitHub-Delivery": info["headers"]["x-github-delivery"],
                        "X-Hub-Signature": info["headers"]["x-hub-signature"],
                        "User-Agent": info["headers"]["user-agent"],
                        "Content-Type": info["headers"]["content-type"],
                    },
                )
            except httpx.HTTPError:
                LOG.warning(
                    "Fail to forward GitHub event",
                    event_type=info["headers"]["x-github-event"],
                    event_id=info["headers"]["x-github-delivery"],
                )

        if ids_to_delete:
            await redis_links.stream.xdel(EVENT_FORWARDER_REDIS_KEY, *ids_to_delete)

    return len(ids_to_delete) == EVENT_FORWARDER_BATCH_SIZE
