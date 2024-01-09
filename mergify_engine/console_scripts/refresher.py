import typing

import click

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.console_scripts import admin_cli


@admin_cli.async_command
@click.pass_context
@click.argument("url", required=True)
@click.option(
    "--action",
    default="user",
    type=click.Choice(["user", "admin", "internal"]),
)
async def refresh(
    cli_ctxt: click.Context,
    url: str,
    action: typing.Literal["user", "admin", "internal"],
) -> None:
    redis_links = redis_utils.RedisLinks(name="debug")
    try:
        (
            owner_login,
            repository_name,
            pull_request_number,
            branch,
        ) = utils.github_url_parser(url)
    except ValueError:
        cli_ctxt.fail(f"{url} is not valid")

    installation_json = await github.get_installation_from_login(owner_login)
    async with github.aget_client(installation_json) as client:
        try:
            repository = await client.item(f"/repos/{owner_login}/{repository_name}")
        except http.HTTPNotFoundError:
            cli_ctxt.fail(f"repository {owner_login}/{repository_name} not found")

    if branch is not None:
        await refresher.send_branch_refresh(
            redis_links.stream,
            repository,
            action=action,
            source="mergify support",
            ref=github_types.GitHubRefType(f"refs/heads/{branch}"),
        )
        click.echo(f"refresh of branch `{url}` has been requested")

    elif pull_request_number is not None:
        await refresher.send_pull_refresh(
            redis_links.stream,
            repository,
            action=action,
            pull_request_number=pull_request_number,
            source="mergify support",
        )
        click.echo(f"refresh of pull request `{url}` has been requested")
    else:
        cli_ctxt.fail(f"No pull request or branch to refresh found for {url}")
