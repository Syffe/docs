from __future__ import annotations

import logging

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers


async def get_rule_checks_status(
    log: logging.LoggerAdapter[logging.Logger],
    repository: context.Repository,
    pulls: list[context.BasePullRequest],
    rule: rules.EvaluatedRule | rules.EvaluatedQueueRule,
    *,
    unmatched_conditions_return_failure: bool = True,
) -> check_api.Conclusion:
    if rule.conditions.match:
        return check_api.Conclusion.SUCCESS

    conditions_without_checks = rule.conditions.copy()
    for condition_without_check in conditions_without_checks.walk():
        attr = condition_without_check.get_attribute_name()
        if attr.startswith("check-") or attr.startswith("status-"):
            condition_without_check.update("number>0")

    # NOTE(sileht): Something unrelated to checks unmatch?
    await conditions_without_checks(pulls)
    log.debug(
        "something unrelated to checks doesn't match? %s",
        conditions_without_checks.get_summary(),
    )
    if not conditions_without_checks.match:
        if unmatched_conditions_return_failure:
            return check_api.Conclusion.FAILURE
        else:
            return check_api.Conclusion.PENDING

    # NOTE(sileht): we replace BinaryFilter by IncompleteChecksFilter to ensure
    # all required CIs have finished. IncompleteChecksFilter return 3 states
    # instead of just True/False, this allows us to known if a condition can
    # change in the future or if its a final state.
    tree = rule.conditions.extract_raw_filter_tree()
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
            log.debug("found an incomplete check")
            return check_api.Conclusion.PENDING

        pr_number = await pull.number  # type: ignore[attr-defined]
        results[pr_number] = ret

    if all(results.values()):
        # This can't occur!, we should have returned SUCCESS earlier.
        log.error(
            "filter.IncompleteChecksFilter unexpectly returned true",
            tree=tree,
            results=results,
        )
        # So don't merge broken stuff
        return check_api.Conclusion.PENDING
    else:
        return check_api.Conclusion.FAILURE
