import argparse

from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import service
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.queue import merge_train
from mergify_engine.queue.merge_train import train_car
from mergify_engine.rules.config import mergify as mergify_conf


@utils.make_sync_for_entrypoint
async def clear_token_cache() -> None:
    async with redis_utils.RedisLinks(name="debug") as redis_links:
        parser = argparse.ArgumentParser(
            description="Force refresh of installation token"
        )
        parser.add_argument("owner_id")
        args = parser.parse_args()
        await subscription.Subscription.delete_subscription(
            redis_links.cache, args.owner_id
        )


@utils.make_sync_for_entrypoint
async def refresher_cli() -> None:
    parser = argparse.ArgumentParser(description="Force refresh of mergify_engine")
    parser.add_argument(
        "--action", default="user", choices=["user", "admin", "internal"]
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help="https://github.com/<owner>/<repo>/pull/<pull#> or "
        "https://github.com/<owner>/<repo>/branch/<branch>",
    )

    args = parser.parse_args()

    if not args.urls:
        parser.print_help()

    async with redis_utils.RedisLinks(name="debug") as redis_links:
        for url in args.urls:
            try:
                (
                    owner_login,
                    repository_name,
                    pull_request_number,
                    branch,
                ) = utils.github_url_parser(url)
            except ValueError:
                print(f"{url} is not valid")
                continue

            installation_json = await github.get_installation_from_login(owner_login)
            async with github.aget_client(installation_json) as client:
                try:
                    repository = await client.item(
                        f"/repos/{owner_login}/{repository_name}"
                    )
                except http.HTTPNotFound:
                    print(f"repository {owner_login}/{repository_name} not found")
                    continue

            if branch is not None:
                await refresher.send_branch_refresh(
                    redis_links.stream,
                    repository,
                    action=args.action,
                    source="API",
                    ref=github_types.GitHubRefType(f"refs/heads/{branch}"),
                )

            elif pull_request_number is not None:
                await refresher.send_pull_refresh(
                    redis_links.stream,
                    repository,
                    action=args.action,
                    pull_request_number=pull_request_number,
                    source="API",
                )
            else:
                print(f"No pull request or branch to refresh found for {url}")


@utils.make_sync_for_entrypoint
async def merge_queue_reset() -> None:
    parser = argparse.ArgumentParser(description="merge queue reset for mergify")
    parser.add_argument("url", help="Pull request or repository url")
    args = parser.parse_args()
    service.setup(
        service_name="merge-queue-resetter", dump_config=False, stdout_logging_only=True
    )

    redis_links = redis_utils.RedisLinks(name="debug")

    try:
        owner_login, repo, pull_number, _ = utils.github_url_parser(args.url)
    except ValueError:
        print(f"{args.url} is not valid")
        return

    if repo is None:
        print("repository url is not valid: {args.url}")
        return

    try:
        installation_json = await github.get_installation_from_login(owner_login)
        client = github.aget_client(installation_json)
    except exceptions.MergifyNotInstalled:
        print(f"* Mergify is not installed on account {owner_login}")
        return

    # Do a dumb request just to authenticate
    await client.get("/")

    owner_id = installation_json["account"]["id"]
    cached_sub = await subscription.Subscription.get_subscription(
        redis_links.cache, owner_id
    )
    installation = context.Installation(
        installation_json, cached_sub, client, redis_links
    )

    repository = await installation.get_repository_by_name(repo)

    try:
        mergify_config = await repository.get_mergify_config()
    except mergify_conf.InvalidRules as e:  # pragma: no cover
        print(f"configuration is invalid {e!s}")
        return

    async for convoy in merge_train.Convoy.iter_convoys(
        repository, mergify_config["queue_rules"], mergify_config["partition_rules"]
    ):
        for train in convoy.iter_trains():
            # NOTE(sileht): This is not concurrent safe, if a pull request is added/removed on the train
            # on the same moment, we will lost the change.
            await train.reset(unexpected_changes=train_car.MergifySupportReset())
