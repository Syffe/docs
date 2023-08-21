import click

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import subscription
from mergify_engine.console_scripts import admin_cli


@admin_cli.async_command
@click.argument("owner_id", required=True)
async def clear_subscription_cache(owner_id: github_types.GitHubAccountIdType) -> None:
    async with redis_utils.RedisLinks(name="debug") as redis_links:
        await subscription.Subscription.delete_subscription(redis_links.cache, owner_id)
    click.echo(f"Subscription cache cleared for `{owner_id}`")
