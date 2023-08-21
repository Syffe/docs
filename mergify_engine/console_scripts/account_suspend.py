import typing

import click

from mergify_engine import github_types
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.console_scripts import admin_cli


async def suspended(
    verb: typing.Literal["PUT", "DELETE"], owner: github_types.GitHubLogin
) -> typing.Any:
    async with github.AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
        installation = typing.cast(
            github_types.GitHubInstallation,
            await client.item(f"/orgs/{owner}/installation"),
        )
        await client.request(verb, f"/app/installations/{installation['id']}/suspended")


@admin_cli.async_command
@click.argument("organization", required=True)
async def suspend(organization: github_types.GitHubLogin) -> None:
    await suspended("PUT", organization)
    click.echo(f"Account `{organization}` suspended")


@admin_cli.async_command
@click.argument("organization", required=True)
async def unsuspend(organization: github_types.GitHubLogin) -> None:
    await suspended("DELETE", organization)
    click.echo(f"Account `{organization}` unsuspended")
