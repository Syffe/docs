import click

from mergify_engine import console_scripts
from mergify_engine import debug as debug_mod


@console_scripts.async_admin_command
@click.argument("url", required=True)
async def debug(url: str) -> None:
    await debug_mod.report(url)
