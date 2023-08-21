import click

from mergify_engine import debug as debug_mod
from mergify_engine.console_scripts import admin_cli


@admin_cli.async_command
@click.argument("url", required=True)
async def debug(url: str) -> None:
    await debug_mod.report(url)
