import asyncio
from collections import abc
import contextlib
import textwrap
import traceback

import click
import sqlalchemy

from mergify_engine import database
from mergify_engine import redis_utils
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.console_scripts import admin_cli


TIMEOUT = 3


@contextlib.asynccontextmanager
async def _check_connectivity_of(name: str, verbose: bool) -> abc.AsyncIterator[None]:
    click.echo(f"{name}: ", nl=False)
    try:
        async with asyncio.timeout(TIMEOUT):
            yield
    except TimeoutError:
        click.secho("connection timed out", fg="red")
    except Exception as e:
        click.secho("failed to connect", fg="red")
        error = traceback.format_exc() if verbose else str(e)
        click.echo(textwrap.indent(error, "> "))
    else:
        click.secho("connected", fg="green")


@admin_cli.async_command
@click.option(
    "-v",
    "--verbose",
    default=False,
    type=click.BOOL,
)
async def connectivity_check(verbose: bool = False) -> None:
    async with _check_connectivity_of("Redis", verbose):
        redis_links = redis_utils.RedisLinks(name="check-connectivity")
        await redis_links.cache.ping()

    async with _check_connectivity_of("Postgres", verbose):
        engine = database.get_engine()
        async with engine.connect() as connection:
            await connection.execute(sqlalchemy.text("SELECT 1"))

    async with _check_connectivity_of("GitHub server", verbose):
        async with github.AsyncGitHubClient(
            auth=github_app.GitHubBearerAuth(),
        ) as client:
            await client.item("/app")
