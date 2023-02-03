from __future__ import annotations

import logging

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers


def get_conditions_with_ignored_attributes(
    rule: rules.EvaluatedPullRequestRule | rules.EvaluatedQueueRule,
    attribute_prefixes: tuple[str, ...],
) -> (
    rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions
):
    conditions = rule.conditions.copy()
    for condition in conditions.walk():
        attr = condition.get_attribute_name()
        if attr.startswith(attribute_prefixes):
            condition.update("number>0")
    return conditions


async def conditions_without_some_attributes_match_p(
    log: "logging.LoggerAdapter[logging.Logger]",
    pulls: list[context.BasePullRequest],
    rule: "rules.EvaluatedPullRequestRule | rules.EvaluatedQueueRule",
    attribute_prefixes: tuple[str, ...],
) -> bool:
    conditions = get_conditions_with_ignored_attributes(rule, attribute_prefixes)
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
    pulls: list[context.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
) -> check_api.Conclusion:
    # NOTE(sileht): we replace BinaryFilter by IncompleteChecksFilter to ensure
    # all required CIs have finished. IncompleteChecksFilter return 3 states
    # instead of just True/False, this allows us to known if a condition can
    # change in the future or if its a final state.
    tree = conditions.extract_raw_filter_tree()
    results: dict[int, filter.IncompleteChecksResult] = {}

    for pull in pulls:
        f = filter.IncompleteChecksFilter(
            tree,
            pending_checks=await getattr(pull, "check-pending"),
            all_checks=await pull.check,  # type: ignore[attr-defined]
        )
        live_resolvers.configure_filter(repository, f)

        ret = await f(pull)
        if ret is filter.IncompleteCheck:
            return check_api.Conclusion.PENDING

        pr_number = await pull.number  # type: ignore[attr-defined]
        results[pr_number] = ret

    if all(results.values()):
        return check_api.Conclusion.SUCCESS
    else:
        return check_api.Conclusion.FAILURE


async def get_rule_checks_status(
    log: "logging.LoggerAdapter[logging.Logger]",
    repository: context.Repository,
    pulls: list[context.BasePullRequest],
    rule: rules.EvaluatedPullRequestRule | rules.EvaluatedQueueRule,
    *,
    wait_for_schedule_to_match: bool = False,
) -> check_api.Conclusion:
    if rule.conditions.match:
        return check_api.Conclusion.SUCCESS

    only_checks_does_not_match = await conditions_without_some_attributes_match_p(
        log, pulls, rule, ("check-", "status-")
    )
    if only_checks_does_not_match:
        result = await _get_checks_result(repository, pulls, rule.conditions)
        if result == check_api.Conclusion.SUCCESS:
            log.error(
                "_get_checks_result() unexpectly returned check_api.Conclusion.SUCCESS "
                "while rule.conditions.match is false",
                tree=rule.conditions.extract_raw_filter_tree(),
            )
            # So don't merge broken stuff
            return check_api.Conclusion.PENDING
        return result
    else:
        if wait_for_schedule_to_match:
            schedule_match = await conditions_without_some_attributes_match_p(
                log, pulls, rule, ("check-", "status-", "schedule", "current-")
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
                    get_conditions_with_ignored_attributes(
                        rule, ("schedule", "current-")
                    ),
                )
                if result_without_schedule == check_api.Conclusion.FAILURE:
                    return result_without_schedule
                return check_api.Conclusion.PENDING
            else:
                return check_api.Conclusion.FAILURE
        else:
            return check_api.Conclusion.FAILURE
