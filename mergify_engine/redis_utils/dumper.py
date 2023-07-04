import argparse
import os.path

from mergify_engine import redis_utils
from mergify_engine import utils


async def dump(argv: list[str] | None = None) -> None:
    # NOTE(Syffe): By default, this scripts downloads all cached config files,
    # though it can be used to download every other data stored in RedisCache.
    # It could be improved by adding an argument making it possible to choose
    # and download from other Redis DBs.
    parser = argparse.ArgumentParser(
        description="Download redis cached keys according the specified arguments"
    )
    parser.add_argument(
        "--path",
        default="cached_config_files",
        help="Path to downloaded files",
    )
    parser.add_argument(
        "--key",
        default="config_file/*",
        help="Key pattern to search for in Redis",
    )
    args = parser.parse_args(argv)

    redis_links = redis_utils.RedisLinks(name="script_download_redis_cached_keys")

    try:
        os.makedirs(args.path)
    except FileExistsError:
        pass

    async for redis_key in redis_links.cache.scan_iter(args.key, count=1000):
        value = await redis_links.cache.get(redis_key)
        if value is not None:
            with open(
                f"{args.path}/{redis_key.decode().replace('/', '-')}.txt", "wb"
            ) as f:
                f.write(value)

    await redis_links.shutdown_all()


@utils.make_sync_for_entrypoint
async def main() -> None:
    await dump()
