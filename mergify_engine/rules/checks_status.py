from __future__ import annotations

import typing

from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.rules import conditions as rules_conditions
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers


if typing.TYPE_CHECKING:
    import logging

    from mergify_engine.queue.merge_train import TrainCarOutcome


async def _get_ci_conclusion(
    repository: context.Repository,
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    other_unknown_attributes: tuple[str, ...] = (),
) -> check_api.Conclusion:
    # NOTE(sileht): we replace BinaryFilter by IncompleteChecksFilter to ensure
    # all required CIs have finished. IncompleteChecksFilter return 3 states
    # instead of just True/False, this allows us to known if a condition can
    # change in the future or if its a final state.
    tree = conditions.extract_raw_filter_tree()
    results: dict[github_types.GitHubPullRequestNumber, filter.TernaryFilterResult] = {}

    for pull in pulls:
        f = filter.IncompleteChecksFilter(
            tree,
            pending_checks=typing.cast(
                list[str],
                await pull.get_attribute_value("check-pending"),
            ),
            all_checks=typing.cast(list[str], await pull.get_attribute_value("check")),
            other_unknown_attributes=other_unknown_attributes,
        )
        live_resolvers.configure_filter(repository, f)

        ret = await f(pull)
        if isinstance(ret, filter.UnknownType):
            return check_api.Conclusion.PENDING

        pr_number = typing.cast(
            github_types.GitHubPullRequestNumber,
            await pull.get_attribute_value("number"),
        )
        results[pr_number] = ret

    if all(results.values()):
        return check_api.Conclusion.SUCCESS
    return check_api.Conclusion.FAILURE


async def get_outcome_from_conditions(
    log: logging.LoggerAdapter[logging.Logger],
    repository: context.Repository,
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    # Maybe create a sub type for TrainCarOutcome used here.
) -> TrainCarOutcome:
    # Circular import
    from mergify_engine.queue.merge_train import TrainCarOutcome

    if conditions.match:
        return TrainCarOutcome.WAITING_FOR_MERGE

    # NOTE(charly): check conditions with CI conclusion unknown. If it fails for
    # one pull request, CI is not the root cause.
    match_with_ci_unknown = await _get_ternary_filter_results(
        repository,
        pulls,
        conditions,
        ("check-", "status-"),
    )
    # NOTE(charly): all pull requests have either an unknown CI match, or a
    # match
    if not any(m is False for m in match_with_ci_unknown.values()):
        # CI is the root cause of the condition unmatch, so get its conclusion
        # to return the right outcome
        ci_conclusion = await _get_ci_conclusion(repository, pulls, conditions)

        if ci_conclusion == check_api.Conclusion.SUCCESS:
            log.error(
                "_get_checks_result() unexpectedly returned check_api.Conclusion.SUCCESS "
                "while conditions.match is false",
                tree=conditions.extract_raw_filter_tree(),
            )
            # So don't merge broken stuff
            return TrainCarOutcome.WAITING_FOR_CI

        if ci_conclusion == check_api.Conclusion.PENDING:
            return TrainCarOutcome.WAITING_FOR_CI

        if ci_conclusion == check_api.Conclusion.FAILURE:
            return TrainCarOutcome.CHECKS_FAILED

        raise RuntimeError("Unexpected _get_checks_result() return value")

    # NOTE(charly): check conditions with CI conclusion and schedule unknown. If
    # it fails for one pull request, CI and schedule are not the root cause,
    # another condition has failed.
    match_with_ci_and_schedule_unknown = await _get_ternary_filter_results(
        repository,
        pulls,
        conditions,
        ("check-", "status-", "schedule", "current-datetime"),
    )
    # NOTE(charly): all pull requests have either an unknown CI/schedule match,
    # or a match
    if not any(m is False for m in match_with_ci_and_schedule_unknown.values()):
        # NOTE(sileht): now we look at the CI conclusion while ignoring schedule
        # conditions
        ci_conclusion_without_schedule = await _get_ci_conclusion(
            repository,
            pulls,
            conditions.copy(
                ignore_conditions_starting_with=("schedule", "current-datetime"),
            ),
        )

        if ci_conclusion_without_schedule == check_api.Conclusion.FAILURE:
            # Checks failed outside of schedule
            return TrainCarOutcome.CHECKS_FAILED

        if ci_conclusion_without_schedule == check_api.Conclusion.PENDING:
            # Checks running outside of schedule
            return TrainCarOutcome.WAITING_FOR_CI

        if ci_conclusion_without_schedule == check_api.Conclusion.SUCCESS:
            # Checks succeeded outside of schedule, we wait for the schedule
            return TrainCarOutcome.WAITING_FOR_SCHEDULE

        raise RuntimeError("Unexpected _get_checks_result() return value")

    return TrainCarOutcome.CONDITIONS_FAILED


async def _get_ternary_filter_results(
    repository: context.Repository,
    pulls: list[condition_value_querier.BasePullRequest],
    conditions: rules_conditions.PullRequestRuleConditions
    | rules_conditions.QueueRuleMergeConditions,
    unknown_attribute_prefixes: tuple[str, ...],
) -> dict[github_types.GitHubPullRequestNumber, filter.TernaryFilterResult]:
    tree = conditions.extract_raw_filter_tree()
    results: dict[github_types.GitHubPullRequestNumber, filter.TernaryFilterResult] = {}

    for pull in pulls:
        pr_number = typing.cast(
            github_types.GitHubPullRequestNumber,
            await pull.get_attribute_value("number"),
        )
        filter_ = _create_ternary_filter(repository, tree, unknown_attribute_prefixes)
        match = await filter_(pull)
        results[pr_number] = match

    return results


def _create_ternary_filter(
    repository: context.Repository,
    tree: filter.TreeT,
    unknown_attribute_prefixes: tuple[str, ...],
) -> filter.TernaryFilter:
    filter_ = filter.UnknownAttributesFilter(
        tree,
        unknown_attribute_prefixes=unknown_attribute_prefixes,
    )
    live_resolvers.configure_filter(repository, filter_)
    return filter_
