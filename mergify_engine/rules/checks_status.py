from __future__ import annotations

import typing

from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers


if typing.TYPE_CHECKING:
    import logging


def get_conditions_with_ignored_attributes(
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    attribute_prefixes: tuple[str, ...],
) -> (
    rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions
):
    conditions = conditions.copy()
    for condition in conditions.walk():
        attr = condition.get_attribute_name()
        if attr.startswith(attribute_prefixes):
            condition.make_always_true()
    return conditions


async def conditions_without_some_attributes_match_p(
    log: logging.LoggerAdapter[logging.Logger],
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    attribute_prefixes: tuple[str, ...],
) -> bool:
    conditions = get_conditions_with_ignored_attributes(conditions, attribute_prefixes)
    await conditions(pulls)
    log.debug(
        "does_conditions_without_some_attributes_match ?",
        attribute_prefixes=attribute_prefixes,
        match=conditions.match,
        summary=conditions.get_summary(),
    )
    return conditions.match


async def _get_checks_result(
    repository: context.Repository,
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
) -> check_api.Conclusion:
    # NOTE(sileht): we replace BinaryFilter by IncompleteChecksFilter to ensure
    # all required CIs have finished. IncompleteChecksFilter return 3 states
    # instead of just True/False, this allows us to known if a condition can
    # change in the future or if its a final state.
    tree = conditions.extract_raw_filter_tree()
    results: dict[int, filter.TernaryFilterResult] = {}

    for pull in pulls:
        f = filter.IncompleteChecksFilter(
            tree,
            pending_checks=await getattr(pull, "check-pending"),
            all_checks=await pull.check,  # type: ignore[attr-defined]
        )
        live_resolvers.configure_filter(repository, f)

        ret = await f(pull)
        if ret in (
            filter.UnknownOnlyAttribute,
            filter.UnknownOrTrueAttribute,
            # NOTE(sileht): Impossible since since root conditions is always an AND, but better safe than sorry
            filter.UnknownOrFalseAttribute,
        ):
            return check_api.Conclusion.PENDING

        pr_number = await pull.number  # type: ignore[attr-defined]
        results[pr_number] = ret

    if all(results.values()):
        return check_api.Conclusion.SUCCESS
    return check_api.Conclusion.FAILURE


async def get_rule_checks_status(
    log: logging.LoggerAdapter[logging.Logger],
    repository: context.Repository,
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    *,
    wait_for_schedule_to_match: bool = False,
) -> check_api.Conclusion:
    if conditions.match:
        return check_api.Conclusion.SUCCESS

    only_checks_does_not_match = await conditions_without_some_attributes_match_p(
        log, pulls, conditions, ("check-", "status-")
    )
    if only_checks_does_not_match:
        result = await _get_checks_result(repository, pulls, conditions)
        if result == check_api.Conclusion.SUCCESS:
            log.error(
                "_get_checks_result() unexpectedly returned check_api.Conclusion.SUCCESS "
                "while conditions.match is false",
                tree=conditions.extract_raw_filter_tree(),
            )
            # So don't merge broken stuff
            return check_api.Conclusion.PENDING
        return result

    if wait_for_schedule_to_match:
        schedule_match = await conditions_without_some_attributes_match_p(
            log, pulls, conditions, ("check-", "status-", "schedule")
        )
        # NOTE(sileht): when something not related to checks does not match
        # we now also remove schedule from the tree, if it match
        # afterwards, it means that a schedule didn't match yet
        if schedule_match:
            # NOTE(sileht): now we look at the CIs result and if it fail
            # we fail too, otherwise we wait for the schedule to match
            result_without_schedule = await _get_checks_result(
                repository,
                pulls,
                get_conditions_with_ignored_attributes(conditions, ("schedule",)),
            )
            if result_without_schedule == check_api.Conclusion.FAILURE:
                return result_without_schedule
            return check_api.Conclusion.PENDING

        return check_api.Conclusion.FAILURE

    return check_api.Conclusion.FAILURE
