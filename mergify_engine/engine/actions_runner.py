import asyncio
import base64
import datetime
import html
import re
import typing

from datadog import statsd  # type: ignore[attr-defined]
import ddtrace

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions
from mergify_engine.rules.config import pull_request_rules as prr_config


NOT_APPLICABLE_TEMPLATE = """<details>
<summary>Rules not applicable to this pull request:</summary>
%s
</details>"""

REQUIRE_BRANCH_PROTECTION_DEPRECATION_GHES = """
:bangbang: **Action Required** :bangbang:
> **The configuration uses the deprecated option `require_branch_protection` in the queue_rules.**
> It must be replaced with the `queue_rules` option `branch_protection_injection_mode`.
> The `require_branch_protection` option will be removed on a future version.
> For more information and examples on how to use the `branch_protection_injection_mode` option: https://docs.mergify.com/actions/queue/#queue-rules
"""

REQUIRE_BRANCH_PROTECTION_DEPRECATION_SAAS = """
:bangbang: **Action Required** :bangbang:
> **The configuration uses the deprecated option `require_branch_protection` in the queue_rules.**
> It must be replaced with the `queue_rules` option `branch_protection_injection_mode`.
> A brownout for `require_branch_protection` is planned on November 21st, 2023.
> The option will be removed on December 20th, 2023.
> For more information and examples on how to use the `branch_protection_injection_mode` option: https://docs.mergify.com/actions/queue/#queue-rules
"""


async def get_already_merged_summary(
    ctxt: context.Context,
    match: prr_config.PullRequestRulesEvaluator,
) -> str:
    has_user_defined_rules = any(not r.hidden for r in match.rules)
    if not ctxt.pull["merged"] or not has_user_defined_rules:
        return ""

    mergify_bot = await github.GitHubAppInfo.get_bot(
        ctxt.repository.installation.redis.cache,
    )
    if (
        ctxt.pull["merged_by"] is not None
        and ctxt.pull["merged_by"]["id"] == mergify_bot["id"]
    ):
        pull_attrs = condition_value_querier.PullRequest(ctxt)
        for rule in match.matching_rules:
            if "merge" in rule.actions or "queue" in rule.actions:
                # NOTE(sileht): Replace all -merged -closed by closed/merged and
                # check it the rule still match if not it has been merged manually
                custom_conditions = rule.conditions.copy()
                for condition in custom_conditions.walk():
                    attr = condition.get_attribute_name()
                    if attr == "merged":
                        condition.update({"=": ("merged", True)})
                    elif attr == "closed":
                        condition.update({"=": ("closed", True)})

                await custom_conditions([pull_attrs])
                if custom_conditions.match:
                    # We already have a fully detailled status in the rule
                    # associated with the action queue/merge
                    return ""

        # NOTE(sileht): This looks impossible because the pull request hasn't been
        # merged by our engine. If this pull request was a slice of another one,
        # GitHub closes it automatically and put as merged_by the merger of the
        # other one.
        return (
            "⚠️ The pull request has been closed by GitHub "
            "because its commits are also part of another pull request\n\n"
        )

    if ctxt.pull["merged_by"] is None:
        merged_by = "???"
    else:
        merged_by = ctxt.pull["merged_by"]["login"]

    return f"⚠️ The pull request has been merged by @{merged_by}\n\n"


def _sanitize_action_config(config_key: str, config_value: typing.Any) -> typing.Any:
    if "bot_account" in config_key and isinstance(config_value, dict):
        return config_value["login"]
    if isinstance(config_value, conditions.PullRequestRuleConditions):
        return yaml.LiteralYamlString(config_value.get_summary().strip())
    if isinstance(config_value, set):
        return list(config_value)
    if isinstance(config_value, str) and "\n" in config_value:
        return yaml.LiteralYamlString(config_value)
    if isinstance(config_value, re.Pattern):
        return config_value.pattern
    return config_value


async def gen_summary_rules(
    ctxt: context.Context,
    _rules: list[prr_config.EvaluatedPullRequestRule],
    display_action_configs: bool,
) -> str:
    summary = ""
    for rule in _rules:
        escaped_rule_name = html.escape(rule.name)
        if rule.hidden:
            continue
        if rule.disabled is None:
            summary += f"###{' ✅' if rule.conditions.match else ''} Rule: {escaped_rule_name} ({', '.join(rule.actions)})\n"
        else:
            summary += (
                f"### Rule: ~~{escaped_rule_name} ({', '.join(rule.actions)})~~\n"
            )
            summary += f":no_entry_sign: **Disabled: {html.escape(rule.disabled['reason'])}**\n"
        summary += rule.conditions.get_summary()
        summary += "\n\n"
        if display_action_configs:
            for action_name, action in rule.actions.items():
                summary += f"**{action_name} action configuration:**\n"
                summary += "```\n"
                summary += yaml.safe_dump(
                    {
                        k: _sanitize_action_config(k, v)
                        for k, v in action.executor.config.items()
                        if k not in action.executor.config_hidden_from_simulator
                    },
                    default_flow_style=False,
                ).replace("```", "\\`\\`\\`")
                summary += "```"
                summary += "\n\n"
    return summary


async def gen_summary(
    ctxt: context.Context,
    pull_request_rules: prr_config.PullRequestRules,
    match: prr_config.PullRequestRulesEvaluator,
    display_action_configs: bool = False,
) -> tuple[str, str]:
    summary = ""
    summary += await get_already_merged_summary(ctxt, match)

    mergify_config_file = await ctxt.repository.get_mergify_config_file()
    has_require_branch_protection_in_config_file = (
        mergify_config_file is not None
        and "require_branch_protection" in mergify_config_file["decoded_content"]
    )
    if has_require_branch_protection_in_config_file:
        if settings.SAAS_MODE:
            summary += REQUIRE_BRANCH_PROTECTION_DEPRECATION_SAAS
        else:
            summary += REQUIRE_BRANCH_PROTECTION_DEPRECATION_GHES

    matching_rules_to_display = match.matching_rules[:]
    not_applicable_base_changeable_attributes_rules_to_display = []
    for rule in match.matching_rules:
        if rule in match.not_applicable_base_changeable_attributes_rules:
            matching_rules_to_display.remove(rule)
            not_applicable_base_changeable_attributes_rules_to_display.append(rule)

    summary += await gen_summary_rules(ctxt, match.faulty_rules, display_action_configs)
    summary += await gen_summary_rules(
        ctxt,
        matching_rules_to_display,
        display_action_configs,
    )
    if ctxt.subscription.has_feature(subscription.Features.SHOW_SPONSOR):
        summary += constants.MERGIFY_OPENSOURCE_SPONSOR_DOC

    summary += "<hr />\n"

    ignored_rules = list(filter(lambda x: not x.hidden, match.ignored_rules))
    ignored_rules_count = len(ignored_rules)
    not_applicable_base_changeable_attributes_rules_to_display_count = len(
        not_applicable_base_changeable_attributes_rules_to_display,
    )
    not_applicable_count = (
        ignored_rules_count
        + not_applicable_base_changeable_attributes_rules_to_display_count
    )
    if not_applicable_count > 0:
        summary += "<details>\n"
        if not_applicable_count == 1:
            summary += (
                f"<summary>{not_applicable_count} not applicable rule</summary>\n\n"
            )
        else:
            summary += (
                f"<summary>{not_applicable_count} not applicable rules</summary>\n\n"
            )

        if ignored_rules_count > 0:
            summary += await gen_summary_rules(
                ctxt,
                ignored_rules,
                display_action_configs,
            )

        if not_applicable_base_changeable_attributes_rules_to_display_count > 0:
            summary += await gen_summary_rules(
                ctxt,
                not_applicable_base_changeable_attributes_rules_to_display,
                display_action_configs,
            )

        summary += "</details>\n"

    completed_rules = len(
        list(filter(lambda rule: rule.conditions.match, match.matching_rules)),
    )
    potential_rules = len(matching_rules_to_display) - completed_rules
    faulty_rules = len(match.faulty_rules)

    if pull_request_rules.has_user_rules():
        summary_title = []
        if faulty_rules == 1:
            summary_title.append(f"{faulty_rules} faulty rule")
        elif faulty_rules > 1:
            summary_title.append(f"{faulty_rules} faulty rules")

        if completed_rules == 1:
            summary_title.append(f"{completed_rules} rule matches")
        elif completed_rules > 1:
            summary_title.append(f"{completed_rules} rules match")

        if potential_rules == 1:
            summary_title.append(f"{potential_rules} potential rule")
        elif potential_rules > 1:
            summary_title.append(f"{potential_rules} potential rules")

        if completed_rules == 0 and potential_rules == 0 and faulty_rules == 0:
            summary_title.append("no rules match, no planned actions")
    else:
        summary_title = ["no rules configured, just listening for commands"]

    title = " and ".join(summary_title)
    return title, summary


async def get_summary_check_result(
    ctxt: context.Context,
    pull_request_rules: prr_config.PullRequestRules,
    match: prr_config.PullRequestRulesEvaluator,
    summary_check: github_types.CachedGitHubCheckRun | None,
    conclusions: dict[str, check_api.Conclusion],
    previous_conclusions: dict[str, check_api.Conclusion],
) -> check_api.Result | None:
    summary_title, summary = await gen_summary(
        ctxt,
        pull_request_rules,
        match,
    )

    summary = serialize_conclusions(conclusions) + "\n" + summary
    summary += constants.MERGIFY_PULL_REQUEST_DOC

    summary_changed = (
        not summary_check
        or summary_check["output"]["title"] != summary_title
        or summary_check["output"]["summary"] != summary
        # Even the check-run content didn't change we must report the same content to
        # update the check_suite
        or ctxt.user_refresh_requested()
        or ctxt.admin_refresh_requested()
    )

    if summary_changed:
        ctxt.log.info(
            "summary changed",
            conclusions=conclusions,
            previous_conclusions=previous_conclusions,
        )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            title=summary_title,
            summary=summary,
        )

    ctxt.log.info(
        "summary unchanged",
        conclusions=conclusions,
        previous_conclusions=previous_conclusions,
    )
    # NOTE(sileht): Here we run the engine, but nothing changed so we didn't
    # update GitHub.
    # In pratice, only the started_at and the ended_at is
    # not up2date but we don't really care since no action ran
    return None


async def exec_action(
    action: str,
    method_name: typing.Literal["run", "cancel"],
    executor: actions.ActionExecutorProtocol,
) -> check_api.Result:
    try:
        if method_name == "run":
            method = executor.run
        elif method_name == "cancel":
            method = executor.cancel
        else:
            raise RuntimeError("wrong method_name")
        result = await method()
    except asyncio.CancelledError:
        raise
    except Exception as e:  # pragma: no cover
        # Forward those to worker
        if (
            exceptions.should_be_ignored(e)
            or exceptions.need_retry_in(e)
            or isinstance(e, exceptions.UnprocessablePullRequest)
        ):
            raise
        # NOTE(sileht): the action fails, this is a bug!!!, so just set the
        # result as pending and retry in 5 minutes...
        executor.ctxt.log.error(
            "action failed",
            action=action,
            rule=executor.rule,
            exc_info=True,
        )
        await delayed_refresh.plan_refresh_at_least_at(
            executor.ctxt.repository,
            executor.ctxt.pull["number"],
            date.utcnow() + datetime.timedelta(minutes=5),
        )
        return check_api.Result(
            check_api.Conclusion.PENDING,
            f"Action '{action}' has unexpectedly failed, Mergify team is working on it, the state will be refreshed automatically.",
            "",
        )
    else:
        return result


def load_conclusions_line(
    ctxt: context.Context,
    summary_check: github_types.CachedGitHubCheckRun | None,
) -> str | None:
    if summary_check is not None and summary_check["output"]["summary"] is not None:
        lines = summary_check["output"]["summary"].splitlines()
        if not lines:
            ctxt.log.error("got summary without content", summary_check=summary_check)
            return None
        if lines[-1].startswith("<!-- ") and lines[-1].endswith(" -->"):
            return lines[-1]
        if lines[0].startswith("<!-- ") and lines[0].endswith(" -->"):
            return lines[0]
    return None


def load_conclusions(
    ctxt: context.Context,
    summary_check: github_types.CachedGitHubCheckRun | None,
) -> dict[str, check_api.Conclusion]:
    line = load_conclusions_line(ctxt, summary_check)
    if line:
        return {
            name: check_api.Conclusion(conclusion)
            for name, conclusion in yaml.safe_load(
                base64.b64decode(utils.strip_comment_tags(line).encode()).decode(),
            ).items()
        }

    if not ctxt.has_been_opened():
        ctxt.log.warning(
            "previous conclusion not found in summary",
            summary_check=summary_check,
        )
    return {}


def serialize_conclusions(conclusions: dict[str, check_api.Conclusion]) -> str:
    return (
        "<!-- "
        + base64.b64encode(
            yaml.safe_dump(
                {name: conclusion.value for name, conclusion in conclusions.items()},
            ).encode(),
        ).decode()
        + " -->"
    )


def get_previous_conclusion(
    previous_conclusions: dict[str, check_api.Conclusion],
    name: str,
    checks: dict[str, github_types.CachedGitHubCheckRun],
) -> check_api.Conclusion:
    if name in previous_conclusions:
        return previous_conclusions[name]
    # NOTE(sileht): fallback on posted check-run in case we lose the Summary
    # somehow
    if name in checks:
        return check_api.Conclusion(checks[name]["conclusion"])
    return check_api.Conclusion.NEUTRAL


async def run_actions(
    ctxt: context.Context,
    match: prr_config.PullRequestRulesEvaluator,
    checks: dict[str, github_types.CachedGitHubCheckRun],
    previous_conclusions: dict[str, check_api.Conclusion],
) -> dict[str, check_api.Conclusion]:
    """
    What action.run() and action.cancel() return should be reworked a bit. Currently the
    meaning is not really clear, it could be:
    - None - (succeed but no dedicated report is posted with check api
    - (None, "<title>", "<summary>") - (action is pending, for merge/backport/...)
    - ("success", "<title>", "<summary>")
    - ("failure", "<title>", "<summary>")
    - ("neutral", "<title>", "<summary>")
    - ("cancelled", "<title>", "<summary>")
    """

    user_refresh_requested = ctxt.user_refresh_requested()
    admin_refresh_requested = ctxt.admin_refresh_requested()
    actions_ran = set()
    conclusions = {}

    # NOTE(sileht): We put first rules with missing conditions to do cancellation first.
    # In case of a canceled merge action and another that need to be run. We want first
    # to remove the PR from the queue and then add it back with the new config and not the
    # reverse
    matching_rules = sorted(
        match.matching_rules,
        key=lambda rule: rule.conditions.match,
    )

    method_name: typing.Literal["run", "cancel"]

    for rule in matching_rules:
        for action, action_obj in rule.actions.items():
            check_name = rule.get_check_name(action)

            done_by_another_action = (
                actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES in action_obj.flags
                and action in actions_ran
            )

            if not rule.conditions.match or rule.disabled is not None:
                method_name = "cancel"
                expected_conclusions = [
                    check_api.Conclusion.NEUTRAL,
                    check_api.Conclusion.CANCELLED,
                ]
            else:
                method_name = "run"
                expected_conclusions = [
                    check_api.Conclusion.SUCCESS,
                    check_api.Conclusion.FAILURE,
                ]
                actions_ran.add(action)

            previous_conclusion = get_previous_conclusion(
                previous_conclusions,
                check_name,
                checks,
            )

            conclusion_is_final = (
                actions.ActionFlag.SUCCESS_IS_FINAL_STATE in action_obj.flags
                and previous_conclusion == check_api.Conclusion.SUCCESS
            )
            conclusion_is_the_expected_one = (
                previous_conclusion in expected_conclusions or conclusion_is_final
            )

            need_to_be_run = (
                actions.ActionFlag.ALWAYS_RUN in action_obj.flags
                or admin_refresh_requested
                or (
                    user_refresh_requested
                    and previous_conclusion
                    in (check_api.Conclusion.FAILURE, check_api.Conclusion.CANCELLED)
                )
                or not conclusion_is_the_expected_one
            )

            # TODO(sileht): refactor it to store the whole report in the check summary,
            # not just the conclusions

            if not need_to_be_run:
                report = check_api.Result(
                    previous_conclusion,
                    "Already in expected state",
                    "",
                )
                message = "ignored, already in expected state"

            elif done_by_another_action:
                # NOTE(sileht) We can't run two action merge for example,
                # This assumes the other action produce a report
                report = check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    f"Another {action} action already ran",
                    "",
                )
                message = "ignored, another has already been run"

            else:
                with ddtrace.tracer.trace(
                    f"action.{action}",
                    resource=str(ctxt),
                ) as span:
                    # NOTE(sileht): check state change so we have to run "run" or "cancel"
                    report = await exec_action(
                        action,
                        method_name,
                        action_obj.executor,
                    )
                    span.set_tags({"conclusion": str(report.conclusion)})

                message = "executed"

            conclusions[check_name] = report.conclusion

            if (
                report.conclusion is not check_api.Conclusion.PENDING
                and method_name == "run"
            ):
                statsd.increment("engine.actions.count", tags=[f"name:{action}"])

            if (
                need_to_be_run
                and report.conclusion not in action_obj.executor.silenced_conclusion
            ):
                external_id = (
                    check_api.USER_CREATED_CHECKS
                    if actions.ActionFlag.ALLOW_RETRIGGER_MERGIFY in action_obj.flags
                    else None
                )
                try:
                    await check_api.set_check_run(
                        ctxt,
                        check_name,
                        report,
                        external_id=external_id,
                        details_url=report.details_url,
                    )
                except Exception as e:
                    if exceptions.should_be_ignored(e):
                        ctxt.log.debug(
                            "Fail to post check `%s`",
                            check_name,
                            exc_info=True,
                        )
                    elif exceptions.need_retry_in(e):
                        raise
                    else:
                        ctxt.log.error(
                            "Fail to post check `%s`",
                            check_name,
                            exc_info=True,
                        )

            ctxt.log.info(
                "action evaluation: `%s` %s: %s/%s -> %s",
                action,
                message,
                method_name,
                previous_conclusion.value,
                conclusions[check_name].value,
                report=report,
                rule_summary=rule.conditions.get_summary(),
                previous_conclusion=previous_conclusion.value,
                conclusion=conclusions[check_name].value,
                action=action,
                check_name=check_name,
            )

    return conclusions


async def cleanup_pending_actions_with_no_associated_rules(
    ctxt: context.Context,
    current_conclusions: dict[str, check_api.Conclusion],
    previous_conclusions: dict[str, check_api.Conclusion],
) -> None:
    check_to_cancel = set()
    is_queued = False
    was_queued = False
    check_runs = [c["name"] for c in await ctxt.pull_engine_check_runs]

    for check_name, conclusion in current_conclusions.items():
        if (
            check_name.endswith(" (queue)")
            and conclusion == check_api.Conclusion.PENDING
        ):
            is_queued = True
            break

    signal_trigger = ""
    for check_name, conclusion in previous_conclusions.items():
        if check_name in current_conclusions:
            continue

        if (
            check_name.endswith(" (queue)")
            and conclusion == check_api.Conclusion.PENDING
        ):
            signal_trigger = check_name.removesuffix(" (queue)")
            was_queued = True

        if check_name in check_runs:
            check_to_cancel.add(check_name)

    for check_name in check_to_cancel:
        ctxt.log.debug("action removal cleanup", check_name=check_name)
        await check_api.set_check_run(ctxt, check_name, actions.CANCELLED_CHECK_REPORT)

    if not is_queued and was_queued:
        ctxt.log.debug("action removal cleanup, cleanup queue")
        await merge_train.Convoy.force_remove_pull(
            ctxt.repository,
            ctxt.pull["number"],
            signal_trigger,
            queue_utils.PrDequeued(ctxt.pull["number"], " by workflow automation"),
        )


async def handle(ctxt: context.Context) -> check_api.Result | None:
    pull_request_rules = ctxt.repository.mergify_config["pull_request_rules"]
    if pull_request_rules.has_user_rules() and not ctxt.subscription.has_feature(
        subscription.Features.WORKFLOW_AUTOMATION,
    ):
        return check_api.Result(
            check_api.Conclusion.ACTION_REQUIRED,
            "Cannot use the pull request workflow automation",
            ctxt.subscription.missing_feature_reason(
                ctxt.pull["base"]["repo"]["owner"]["login"],
            ),
        )

    with ddtrace.tracer.trace("pull_request_rules_evaluation", resource=str(ctxt)):
        try:
            match = await pull_request_rules.get_pull_request_rules_evaluator(ctxt)
        except actions.InvalidDynamicActionConfiguration as e:
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "The current Mergify configuration is invalid",
                e.get_summary(),
            )

    pull_attrs = condition_value_querier.PullRequest(ctxt)
    await delayed_refresh.plan_next_refresh(ctxt, match.matching_rules, pull_attrs)

    if not ctxt.sources:
        # NOTE(sileht): Only comment/command, don't need to go further
        return None

    ctxt.log.info(
        "actions runner",
        sources=ctxt.sources,
        configuration_changed=ctxt.configuration_changed,
    )
    ctxt.log.info(
        "ignored pull request rules",
        pull_request_rules=[
            {
                "name": rule.name,
                "rule_summary": rule.conditions.get_summary(),
                "actions": list(rule.actions),
            }
            for rule in match.ignored_rules
        ],
    )

    summary_check = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
    previous_conclusions = load_conclusions(ctxt, summary_check)

    checks = {c["name"]: c for c in await ctxt.pull_engine_check_runs}
    conclusions = await run_actions(ctxt, match, checks, previous_conclusions)
    await cleanup_pending_actions_with_no_associated_rules(
        ctxt,
        conclusions,
        previous_conclusions,
    )

    return await get_summary_check_result(
        ctxt,
        pull_request_rules,
        match,
        summary_check,
        conclusions,
        previous_conclusions,
    )
