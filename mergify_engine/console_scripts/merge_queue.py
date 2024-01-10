import click

from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import redis_utils
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.console_scripts import admin_cli
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import mergify as mergify_conf


@admin_cli.async_command
@click.pass_context
@click.argument("url", required=True)
async def merge_queue_reset(cli_ctxt: click.Context, url: str) -> None:
    try:
        owner_login, repo, _, _ = utils.github_url_parser(url)
    except ValueError:
        cli_ctxt.fail(f"{url} is not valid")

    if repo is None:
        cli_ctxt.fail("repository url is not valid: {args.url}")

    try:
        installation_json = await github.get_installation_from_login(owner_login)
        client = github.aget_client(installation_json)
    except exceptions.MergifyNotInstalledError:
        cli_ctxt.fail(f"* Mergify is not installed on account {owner_login}")

    # Do a dumb request just to authenticate
    await client.get("/")

    owner_id = installation_json["account"]["id"]

    redis_links = redis_utils.RedisLinks(name="debug")

    cached_sub = await subscription.Subscription.get_subscription(
        redis_links.cache,
        owner_id,
    )
    installation = context.Installation(
        installation_json,
        cached_sub,
        client,
        redis_links,
    )

    repository = await installation.get_repository_by_name(repo)

    try:
        await repository.load_mergify_config()
    except mergify_conf.InvalidRulesError as e:  # pragma: no cover
        cli_ctxt.fail(f"configuration is invalid {e!s}")

    async for convoy in merge_train.Convoy.iter_convoys(repository):
        for train in convoy.iter_trains():
            # NOTE(sileht): This is not concurrent safe, if a pull request is added/removed on the train
            # on the same moment, we will lost the change.
            await train.reset(
                queue_utils.QueueReset(
                    "Mergify support has reset the merge queue",
                ),
            )
    click.echo(f"{repository.repo['full_name']} merge queue reseted")
