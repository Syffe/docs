import argparse
import datetime
import typing

from mergify_engine import config
from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import utils
from mergify_engine.ci import dump
from mergify_engine.ci import job_registries
from mergify_engine.ci import pull_registries
from mergify_engine.ci import reports
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.dashboard import subscription


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
async def dump_handler(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mergify CI dump")
    parser.add_argument("owner", type=github_types.GitHubLogin)
    parser.add_argument("repository", type=github_types.GitHubRepositoryName)
    parser.add_argument("at", type=lambda v: datetime.date.fromisoformat(v))
    args = parser.parse_args(argv)

    database.init_sqlalchemy("dump")

    auth = github.GithubTokenAuth(
        token=config.DEV_PERSONAL_TOKEN,
    )

    async with redis_utils.RedisLinks(name="ci_dump") as redis_links:
        await dump.dump(
            redis_links,
            typing.cast(github_types.GitHubLogin, args.owner),
            typing.cast(github_types.GitHubRepositoryName, args.repository),
            args.at,
            auth=auth,
        )


@utils.make_sync_for_entrypoint
async def global_insight_handler(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mergify CI dump")
    parser.add_argument("owner", type=github_types.GitHubLogin)
    parser.add_argument("repository", type=github_types.GitHubRepositoryName)
    parser.add_argument("start_at", type=lambda v: datetime.date.fromisoformat(v))
    parser.add_argument("end_at", type=lambda v: datetime.date.fromisoformat(v))
    args = parser.parse_args(argv)

    database.init_sqlalchemy("ci")

    query = reports.Query(
        owner=args.owner,
        repository=None if args.repository == "*" else args.repository,
        start_at=args.start_at,
        end_at=args.end_at,
    )

    action = reports.Report(
        job_registries.PostgresJobRegistry(),
        pull_registries.PostgresPullRequestRegistry(),
        query,
    )
    result = await action.run()
    print(json.dumps(result))
