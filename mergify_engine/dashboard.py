from __future__ import annotations

import typing
from urllib import parse

import first

from mergify_engine import settings


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine import github_types
    from mergify_engine.queue import merge_train


def get_dashboard_repo_url(
    owner: github_types.GitHubLogin,
    repo: github_types.GitHubRepositoryName,
) -> str:
    return f"{settings.DASHBOARD_UI_FRONT_URL}/github/{owner}/repo/{repo}"


async def get_queues_url_from_context(
    ctxt: context.Context,
    convoy: merge_train.Convoy | None = None,
    open_pr_details: bool = True,
) -> str:
    # NOTE(Kontrolix): Import here to avoid circular import
    from mergify_engine.rules.config import partition_rules as partr_config

    repo_url = get_dashboard_repo_url(
        ctxt.pull["base"]["repo"]["owner"]["login"], ctxt.pull["base"]["repo"]["name"]
    )
    url = f"{repo_url}/queues"

    params = {"branch": str(ctxt.pull["base"]["ref"])}

    if convoy:
        embarked_pull = first.first(
            await convoy.find_embarked_pull_with_train(ctxt.pull["number"])
        )
        if embarked_pull:
            if (
                embarked_pull.train.partition_name
                != partr_config.DEFAULT_PARTITION_NAME
            ):
                url += f"/partitions/{embarked_pull.train.partition_name}"

            params["queues"] = str(
                embarked_pull.convoy_embarked_pull.embarked_pull.config["name"]
            )

    if open_pr_details:
        params["pull"] = str(ctxt.pull["number"])

    url += f"?{parse.urlencode(params)}"

    return url


def get_eventlogs_url(
    login: github_types.GitHubLogin,
    repo_name: github_types.GitHubRepositoryName,
    pull_request_number: github_types.GitHubPullRequestNumber | None = None,
) -> str:
    url = f"{get_dashboard_repo_url(login, repo_name)}/event-logs"
    if pull_request_number:
        url += f"?pullRequestNumber={pull_request_number}"
    return url
