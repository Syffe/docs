import asyncio
import datetime
import itertools
import pprint

import daiquiri

from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.engine import actions_runner
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import mergify as mergify_conf


LOG = daiquiri.getLogger(__name__)


async def get_repositories_setuped(
    token: str, install_id: int
) -> list[github_types.GitHubRepository]:  # pragma: no cover
    repositories = []
    url = f"{settings.GITHUB_REST_API_URL}/user/installations/{install_id}/repositories"
    token = f"token {token}"
    async with http.AsyncClient(
        headers={
            "Authorization": token,
            "Accept": "application/vnd.github.machine-man-preview+json",
        },
    ) as session:
        while True:
            response = await session.get(url)
            if response.status_code == 200:
                repositories.extend(response.json()["repositories"])
                if "next" in response.links:
                    url = response.links["next"]["url"]
                    continue
                return repositories

            response.raise_for_status()


async def report_worker_status(
    redis_links: redis_utils.RedisLinks, owner: github_types.GitHubLogin
) -> None:
    stream_name = f"stream~{owner}".encode()
    streams: list[tuple[bytes, float]] = await redis_links.stream.zrangebyscore(
        "streams", min=0, max="+inf", withscores=True
    )

    for pos, item in enumerate(streams):  # noqa: B007
        if item[0] == stream_name:
            break
    else:
        print("* WORKER: Installation not queued to process")
        return

    planned = datetime.datetime.utcfromtimestamp(streams[pos][1]).isoformat()

    attempts_raw = await redis_links.stream.hget("attempts", stream_name)
    if attempts_raw is None:
        attempts = 0
    else:
        attempts = int(attempts_raw)
    print(
        "* WORKER: Installation queued, "
        f" pos: {pos}/{len(streams)},"
        f" next_run: {planned},"
        f" attempts: {attempts}"
    )

    size = await redis_links.stream.xlen(stream_name)
    print(f"* WORKER PENDING EVENTS for this installation: {size}")


async def report_queue(title: str, train: merge_train.Train) -> None:
    pulls = await train.get_pulls()
    if not pulls:
        return

    print(f"* {title} {train.convoy.ref}")

    async def _get_config(
        p: github_types.GitHubPullRequestNumber,
    ) -> tuple[github_types.GitHubPullRequestNumber, int]:
        return p, (await train.get_config(p))["priority"]

    pulls_priorities: dict[github_types.GitHubPullRequestNumber, int] = dict(
        await asyncio.gather(*(_get_config(p) for p in pulls))
    )

    for priority, grouped_pulls in itertools.groupby(
        pulls, key=lambda p: pulls_priorities[p]
    ):
        try:
            fancy_priority = queue.PriorityAliases(priority).name
        except ValueError:
            fancy_priority = str(priority)
        formatted_pulls = ", ".join(f"#{p}" for p in grouped_pulls)
        print(f"** {formatted_pulls} (priority: {fancy_priority})")


async def report(
    url: str,
) -> context.Context | github.AsyncGitHubInstallationClient | None:
    redis_links = redis_utils.RedisLinks(name="debug")

    try:
        owner_login, repo, pull_number, _ = utils.github_url_parser(url)
    except ValueError:
        print(f"{url} is not valid")
        return None

    try:
        installation_json = await github.get_installation_from_login(owner_login)
        client = github.aget_client(installation_json)
    except exceptions.MergifyNotInstalled:
        print(f"* Mergify is not installed on account {owner_login}")
        return None

    # Do a dumb request just to authenticate
    await client.get("/")

    print(f"* INSTALLATION ID: {installation_json['id']}")

    owner_id = installation_json["account"]["id"]
    cached_sub = await subscription.Subscription.get_subscription(
        redis_links.cache, owner_id
    )
    db_sub = await subscription.Subscription._retrieve_subscription_from_db(
        redis_links.cache, owner_id
    )

    print("* Features (db):")
    for v in sorted(f.value for f in db_sub.features):
        print(f"  - {v}")
    print("* Features (cache):")
    for v in sorted(f.value for f in cached_sub.features):
        print(f"  - {v}")

    installation = context.Installation(
        installation_json, cached_sub, client, redis_links
    )

    print(f"* ENGINE-CACHE SUB DETAIL: {cached_sub.reason}")
    print(f"* DASHBOARD SUB DETAIL: {db_sub.reason}")

    await report_worker_status(redis_links, owner_login)

    if repo is None:
        await redis_links.shutdown_all()
        return client

    repository = await installation.get_repository_by_name(repo)

    print(f"* REPOSITORY IS {'PRIVATE' if repository.repo['private'] else 'PUBLIC'}")

    print(f"* DEFAULT BRANCH: {utils.extract_default_branch(repository.repo)}")

    print("* BRANCH PROTECTION RULES:")
    branch_protection_rules = await repository.get_all_branch_protection_rules()
    pprint.pprint(branch_protection_rules, width=160)

    print("* CONFIGURATION:")
    mergify_config = None
    config_file = await repository.get_mergify_config_file()
    if not config_file:
        print(".mergify.yml is missing")
    else:
        print(f"Config filename: {config_file['path']}")
        print(config_file["decoded_content"])
        try:
            mergify_config = await repository.get_mergify_config()
        except mergify_conf.InvalidRules as e:  # pragma: no cover
            print(f"configuration is invalid {e!s}")

    if pull_number is None:
        if mergify_config is None:
            return client

        async for convoy in merge_train.Convoy.iter_convoys(
            repository, mergify_config["queue_rules"], mergify_config["partition_rules"]
        ):
            for train in convoy.iter_trains():
                await report_queue("TRAIN", train)

        await redis_links.shutdown_all()
        return client

    repository = await installation.get_repository_by_name(
        github_types.GitHubRepositoryName(repo)
    )
    try:
        ctxt = await repository.get_pull_request_context(
            github_types.GitHubPullRequestNumber(int(pull_number))
        )
    except http.HTTPNotFound:
        print(f"Pull request `{url}` does not exist")
        await redis_links.shutdown_all()
        return client

    # FIXME queues could also be printed if no pull number given
    # TODO(sileht): display train if any
    if mergify_config is not None:
        convoy = await merge_train.Convoy.from_context(
            ctxt,
            mergify_config["queue_rules"],
            mergify_config["partition_rules"],
        )
        for train in convoy.iter_trains():
            print(
                f"* TRAIN (partition:{train.partition_name}): {', '.join([f'#{p}' for p in await train.get_pulls()])}"
            )

    print("* PULL REQUEST:")
    attrs = condition_value_querier.PullRequest(ctxt)
    pr_data = {attr: await getattr(attrs, attr) for attr in sorted(attrs)}
    pprint.pprint(pr_data, width=160)

    is_behind = await ctxt.is_behind
    print(f"is_behind: {is_behind}")

    print(f"mergeable_state: {ctxt.pull['mergeable_state']}")

    print("* MERGIFY LAST CHECKS:")
    for c in await ctxt.pull_engine_check_runs:
        print(
            f"[{c['name']}]: {c['conclusion']} | {c['output'].get('title')} | {c['html_url']}"
        )
        print(
            "> "
            + "\n> ".join(
                ("No Summary",)
                if c["output"]["summary"] is None
                else c["output"]["summary"].split("\n")
            )
        )

    if mergify_config is not None:
        print("* MERGIFY LIVE MATCHES:")
        pull_request_rules = mergify_config["pull_request_rules"]
        match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
        summary_title, summary = await actions_runner.gen_summary(
            ctxt, pull_request_rules, match
        )
        print(f"[Summary]: success | {summary_title}")
        print("> " + "\n> ".join(summary.strip().split("\n")))

    await redis_links.shutdown_all()
    return ctxt
