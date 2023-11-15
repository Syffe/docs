from __future__ import annotations

import dataclasses
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.rules import conditions
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import types
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import pull_request_rules as prr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context


def CheckRunJinja2(v: typing.Any) -> str | None:
    return types.Jinja2(
        v,
        {
            "check_rule_name": "Rule name example",
            "check_status": "success",
            "check_succeed": True,
            "check_succeeded": True,
            "check_conditions": "the expected condition conditions",
        },
    )


class PostCheckExecutorConfig(typing.TypedDict):
    title: str
    summary: str
    always_show: bool
    success_conditions: conditions.PullRequestRuleConditions | None
    neutral_conditions: conditions.PullRequestRuleConditions | None


@dataclasses.dataclass
class PostCheckExecutor(
    actions.ActionExecutor["PostCheckAction", PostCheckExecutorConfig],
):
    config: PostCheckExecutorConfig

    @classmethod
    async def create(
        cls,
        action: PostCheckAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> PostCheckExecutor:
        if not ctxt.subscription.has_feature(subscription.Features.CUSTOM_CHECKS):
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                "Custom checks are disabled",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"],
                ),
            )

        pull_attrs = condition_value_querier.PullRequest(ctxt)

        success_conditions = action.config.get("success_conditions")
        neutral_conditions = action.config.get("neutral_conditions")

        # TODO(sileht): Don't run it if conditions contains the rule itself, as it can
        # created an endless loop of events.
        if success_conditions is None and neutral_conditions is None:
            success_conditions = rule.conditions
        else:
            if success_conditions is not None:
                success_conditions = success_conditions.copy()
                live_resolvers.apply_configure_filter(
                    ctxt.repository,
                    success_conditions,
                )
                await success_conditions([pull_attrs])

            if neutral_conditions is not None:
                neutral_conditions = neutral_conditions.copy()
                live_resolvers.apply_configure_filter(
                    ctxt.repository,
                    neutral_conditions,
                )
                await neutral_conditions([pull_attrs])

        if success_conditions is not None and success_conditions.match:
            status = "success"
        elif neutral_conditions is not None and neutral_conditions.match:
            status = "neutral"
        else:
            status = "failure"

        success_conditions_summary = (
            "" if success_conditions is None else success_conditions.get_summary()
        )

        extra_variables: dict[str, str | bool] = {
            "check_rule_name": rule.name,
            "check_status": status,
            "check_succeeded": status == "success",
            "check_conditions": success_conditions_summary,
            # Backward compat
            "check_succeed": status == "success",
        }
        try:
            title = await pull_attrs.render_template(
                action.config["title"],
                extra_variables,
            )
        except condition_value_querier.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                "Invalid title template",
                str(rmf),
            )

        try:
            summary = await pull_attrs.render_template(
                action.config["summary"],
                extra_variables,
            )
        except condition_value_querier.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                "Invalid summary template",
                str(rmf),
            )
        return cls(
            ctxt,
            rule,
            PostCheckExecutorConfig(
                {
                    "title": title,
                    "summary": summary,
                    "success_conditions": success_conditions,
                    "neutral_conditions": neutral_conditions,
                    "always_show": action.config["success_conditions"] is None
                    and action.config["neutral_conditions"] is None,
                },
            ),
        )

    async def run(self) -> check_api.Result:
        success_conditions = self.config["success_conditions"]
        neutral_conditions = self.config["neutral_conditions"]

        if success_conditions is not None and success_conditions.match:
            conclusion = check_api.Conclusion.SUCCESS
        elif neutral_conditions is not None and neutral_conditions.match:
            conclusion = check_api.Conclusion.NEUTRAL
        else:
            conclusion = check_api.Conclusion.FAILURE

        check = await self.ctxt.get_engine_check_run(
            self.rule.get_check_name("post_check"),
        )
        if not check or check["conclusion"] != conclusion.value:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                self.ctxt.pull["base"]["ref"],
                "action.post_check",
                signals.EventPostCheckMetadata(
                    {
                        "conclusion": conclusion.value,
                        "title": self.config["title"],
                        "summary": self.config["summary"],
                    },
                ),
                self.rule.get_signal_trigger(),
            )
        return check_api.Result(
            conclusion,
            self.config["title"],
            self.config["summary"],
            log_details={
                "success_conditions": "no success conditions"
                if success_conditions is None
                else success_conditions.get_summary(),
                "neutral_conditions": "no neutral conditions"
                if neutral_conditions is None
                else neutral_conditions.get_summary(),
            },
        )

    async def cancel(self) -> check_api.Result:
        if self.config["always_show"]:
            return await self.run()
        return actions.CANCELLED_CHECK_REPORT

    @property
    def silenced_conclusion(self) -> tuple[check_api.Conclusion, ...]:
        if self.config["always_show"]:
            return ()
        return (check_api.Conclusion.CANCELLED,)


class PostCheckAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN | actions.ActionFlag.ALLOW_RETRIGGER_MERGIFY
    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required(
            "title",
            default="'{{ check_rule_name }}'{% if check_status == 'success' %} succeeded{% elif check_status == 'failure' %} failed{% endif %}",
        ): CheckRunJinja2,
        voluptuous.Required(
            "summary",
            default="{{ check_conditions }}",
        ): CheckRunJinja2,
        voluptuous.Required("success_conditions", default=None): voluptuous.Any(
            None,
            voluptuous.All(
                [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                voluptuous.Coerce(conditions.PullRequestRuleConditions),
            ),
        ),
        voluptuous.Required("neutral_conditions", default=None): voluptuous.Any(
            None,
            voluptuous.All(
                [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                voluptuous.Coerce(conditions.PullRequestRuleConditions),
            ),
        ),
    }

    executor_class = PostCheckExecutor
