from __future__ import annotations

import datetime
import typing

import daiquiri

from mergify_engine import condition_value_querier
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)

DELAYED_REFRESH_KEY = "delayed-refresh"

STOP_REFRESH_PULL_REQUEST_CLOSED_WITH_TIME_CONDITIONS_SINCE = datetime.timedelta(days=7)


def _redis_key(
    repository: context.Repository,
    pull_number: github_types.GitHubPullRequestNumber,
) -> str:
    return f"{repository.installation.owner_id}~{repository.installation.owner_login}~{repository.repo['id']}~{repository.repo['name']}~{pull_number}"


async def _get_current_refresh_datetime(
    repository: context.Repository,
    pull_number: github_types.GitHubPullRequestNumber,
) -> datetime.datetime | None:
    score = await repository.installation.redis.cache.zscore(
        DELAYED_REFRESH_KEY,
        _redis_key(repository, pull_number),
    )
    if score is not None:
        return date.fromtimestamp(float(score))
    return None


async def _set_current_refresh_datetime(
    repository: context.Repository,
    pull_number: github_types.GitHubPullRequestNumber,
    at: datetime.datetime,
) -> None:
    await repository.installation.redis.cache.zadd(
        DELAYED_REFRESH_KEY,
        {_redis_key(repository, pull_number): at.timestamp()},
    )


async def plan_next_refresh(
    ctxt: context.Context,
    _rules: (
        list[prr_config.EvaluatedPullRequestRule] | list[qr_config.EvaluatedQueueRule]
    ),
    pull_request: condition_value_querier.BasePullRequest,
    only_if_earlier: bool = False,
) -> None:
    refresh_time_conditions = (
        ctxt.pull["closed_at"] is None
        or (date.utcnow() - date.fromisoformat(ctxt.pull["closed_at"]))
        < STOP_REFRESH_PULL_REQUEST_CLOSED_WITH_TIME_CONDITIONS_SINCE
    )

    conditions = []
    for rule in _rules:
        conditions.extend(
            rule.get_conditions_used_by_evaluator().condition.copy().conditions,
        )

        if isinstance(rule, prr_config.PullRequestRule):
            for action in rule.actions.values():
                if action.config.get("success_conditions"):
                    conditions.extend(
                        action.config["success_conditions"].condition.copy().conditions,
                    )

    rule_conditions = conditions_mod.BaseRuleConditions(conditions)

    if not refresh_time_conditions:
        for condition in rule_conditions.walk():
            attr = condition.get_attribute_name()
            # Replace time conditions with an always true condition, so
            # they will become date.DT_MAX when parsed by
            # filter.NearDatetimeFilter
            if attr == "schedule":
                condition.make_always_true()

    f = filter.NearDatetimeFilter(rule_conditions.extract_raw_filter_tree())
    live_resolvers.configure_filter(ctxt.repository, f)
    try:
        plan_refresh_at = await f(pull_request)
    except live_resolvers.LiveResolutionFailureError:
        plan_refresh_at = None

    if plan_refresh_at is None or plan_refresh_at >= date.DT_MAX:
        if only_if_earlier:
            return

        zset_subkey = _redis_key(ctxt.repository, ctxt.pull["number"])
        removed = await ctxt.redis.cache.zrem(DELAYED_REFRESH_KEY, zset_subkey)
        if removed is not None and removed > 0:
            ctxt.log.info("unplan to refresh pull request")
    else:
        if only_if_earlier:
            current = await _get_current_refresh_datetime(
                ctxt.repository,
                ctxt.pull["number"],
            )
            if current is not None and plan_refresh_at >= current:
                return

        await _set_current_refresh_datetime(
            ctxt.repository,
            ctxt.pull["number"],
            plan_refresh_at,
        )
        ctxt.log.info(
            "plan to refresh pull request",
            refresh_planned_at=plan_refresh_at.isoformat(),
            refresh_time_conditions=refresh_time_conditions,
        )


async def plan_refresh_at_least_at(
    repository: context.Repository,
    pull_number: github_types.GitHubPullRequestNumber,
    at: datetime.datetime,
) -> None:
    current = await _get_current_refresh_datetime(repository, pull_number)

    if current is not None and current < at:
        return

    await _set_current_refresh_datetime(repository, pull_number, at)
    repository.log.info(
        "override plan to refresh pull request",
        refresh_planned_at=at.isoformat(),
        gh_pull=pull_number,
    )


async def get_list_of_refresh_to_send(
    redis_links: redis_utils.RedisLinks,
) -> list[bytes]:
    score = date.utcnow().timestamp()
    return await redis_links.cache.zrangebyscore(DELAYED_REFRESH_KEY, "-inf", score)


async def send(redis_links: redis_utils.RedisLinks) -> None:
    keys = await get_list_of_refresh_to_send(redis_links)
    if not keys:
        return

    pipe = typing.cast(redis_utils.PipelineStream, await redis_links.stream.pipeline())
    keys_to_delete = set()
    for subkey in keys:
        (
            owner_id_str,
            owner_login_str,
            repository_id_str,
            repository_name_str,
            pull_request_number_str,
        ) = subkey.decode().split("~")
        owner_id = github_types.GitHubAccountIdType(int(owner_id_str))
        repository_id = github_types.GitHubRepositoryIdType(int(repository_id_str))
        pull_request_number = github_types.GitHubPullRequestNumber(
            int(pull_request_number_str),
        )
        repository_name = github_types.GitHubRepositoryName(repository_name_str)
        owner_login = github_types.GitHubLogin(owner_login_str)

        LOG.info(
            "sending delayed pull request refresh",
            gh_owner=owner_login,
            gh_repo=repository_name,
            gh_pull=pull_request_number,
            action="internal",
            source="delayed-refresh",
        )

        await worker_pusher.push(
            pipe,
            owner_id,
            owner_login,
            repository_id,
            repository_name,
            pull_request_number,
            "refresh",
            {
                "action": "internal",
                "ref": None,
                "source": "delayed-refresh",
            },  # type: ignore[typeddict-item]
            priority=worker_pusher.Priority.medium,
        )
        keys_to_delete.add(subkey)

    await pipe.execute()
    await redis_links.cache.zrem(DELAYED_REFRESH_KEY, *keys_to_delete)
