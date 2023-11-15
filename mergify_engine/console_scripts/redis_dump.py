import os.path

import click

from mergify_engine import redis_utils
from mergify_engine.console_scripts import admin_cli


@admin_cli.async_command
@click.option("--path", default="cached_config_files")
@click.option("--key", default="config_file/*")
async def dump_redis_keys(path: str, key: str) -> None:
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

    async with redis_utils.RedisLinks(
        name="script_download_redis_cached_keys",
    ) as redis_links:
        async for redis_key in redis_links.cache.scan_iter(key, count=1000):
            click.echo(f"Downloading {redis_key.decode()}...")
            value = await redis_links.cache.get(redis_key)
            if value is not None:
                with open(
                    f"{path}/{redis_key.decode().replace('/', '-')}.txt",
                    "wb",
                ) as f:
                    f.write(value)
