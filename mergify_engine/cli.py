import argparse
import datetime
import time
import typing

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import refresher
from mergify_engine import service
from mergify_engine import settings
from mergify_engine import utils
from mergify_engine.ci import dump
from mergify_engine.ci import job_registries
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
    service.setup("ci-dump")

    parser = argparse.ArgumentParser(description="Mergify CI dump")
    parser.add_argument("owner", type=github_types.GitHubLogin)
    parser.add_argument("repository", type=github_types.GitHubRepositoryName)
    parser.add_argument("at", type=lambda v: datetime.date.fromisoformat(v))
    args = parser.parse_args(argv)

    auth: github.GithubAppInstallationAuth | github.GithubTokenAuth

    if settings.TESTING_DEV_PERSONAL_TOKEN:
        auth = github.GithubTokenAuth(
            token=settings.TESTING_DEV_PERSONAL_TOKEN,
        )
    else:
        auth = github.GithubAppInstallationAuth(
            await github.get_installation_from_login(
                typing.cast(github_types.GitHubLogin, args.owner)
            )
        )

    gh_client = github.AsyncGithubInstallationClient(auth=auth)

    async with redis_utils.RedisLinks(name="ci_dump") as redis_links:
        await dump.dump(
            redis_links,
            gh_client,
            typing.cast(github_types.GitHubLogin, args.owner),
            typing.cast(github_types.GitHubRepositoryName, args.repository),
            args.at,
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

    query = reports.CategoryQuery(
        owner=args.owner,
        repository=None if args.repository == "*" else args.repository,
        start_at=args.start_at,
        end_at=args.end_at,
    )

    action = reports.CategoryReport(
        job_registries.PostgresJobRegistry(),
        query,
    )
    started_at = time.monotonic()
    result = await action.run()
    ended_at = time.monotonic()

    print(result)
    print(f"processed in {ended_at-started_at} seconds")
