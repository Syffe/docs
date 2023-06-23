import dataclasses
import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.rules import conditions
from mergify_engine.rules import types
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.rules.config import pull_request_rules as prr_config


def CheckRunJinja2(v: typing.Any) -> str | None:
    return types.Jinja2(
        v,
        {
            "check_rule_name": "Rule name example",
            "check_succeed": True,
            "check_succeeded": True,
            "check_conditions": "the expected condition conditions",
        },
    )


class PostCheckExecutorConfig(typing.TypedDict):
    title: str
    summary: str
    always_show: bool
    check_conditions: conditions.PullRequestRuleConditions


@dataclasses.dataclass
class PostCheckExecutor(
    actions.ActionExecutor["PostCheckAction", PostCheckExecutorConfig]
):
    config: PostCheckExecutorConfig

    @classmethod
    async def create(
        cls,
        action: "PostCheckAction",
        ctxt: "context.Context",
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> "PostCheckExecutor":
        if not ctxt.subscription.has_feature(subscription.Features.CUSTOM_CHECKS):
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                "Custom checks are disabled",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        # TODO(sileht): Don't run it if conditions contains the rule itself, as it can
        # created an endless loop of events.
        if action.config["success_conditions"] is None:
            check_conditions = rule.conditions
        else:
            check_conditions = action.config["success_conditions"].copy()
            rules.apply_configure_filter(ctxt.repository, check_conditions)
            await check_conditions([ctxt.pull_request])

        extra_variables: dict[str, str | bool] = {
            "check_rule_name": rule.name,
            "check_succeeded": check_conditions.match,
            "check_conditions": check_conditions.get_summary(),
            # Backward compat
            "check_succeed": check_conditions.match,
        }
        try:
            title = await ctxt.pull_request.render_template(
                action.config["title"],
                extra_variables,
            )
        except context.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, "Invalid title template", str(rmf)
            )

        try:
            summary = await ctxt.pull_request.render_template(
                action.config["summary"], extra_variables
            )
        except context.RenderTemplateFailure as rmf:
            raise actions.InvalidDynamicActionConfiguration(
                rule, action, "Invalid summary template", str(rmf)
            )
        return cls(
            ctxt,
            rule,
            PostCheckExecutorConfig(
                {
                    "title": title,
                    "summary": summary,
                    "check_conditions": check_conditions,
                    "always_show": action.config["success_conditions"] is None,
                }
            ),
        )

    async def _run(
        self, check_conditions: conditions.PullRequestRuleConditions
    ) -> check_api.Result:
        if check_conditions.match:
            conclusion = check_api.Conclusion.SUCCESS
        else:
            conclusion = check_api.Conclusion.FAILURE

        check = await self.ctxt.get_engine_check_run(
            self.rule.get_check_name("post_check")
        )
        if not check or check["conclusion"] != conclusion.value:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                "action.post_check",
                signals.EventPostCheckMetadata(
                    {
                        "conclusion": conclusion.value,
                        "title": self.config["title"],
                        "summary": self.config["summary"],
                    }
                ),
                self.rule.get_signal_trigger(),
            )
        return check_api.Result(
            conclusion,
            self.config["title"],
            self.config["summary"],
            log_details={"conditions": check_conditions.get_summary()},
        )

    async def run(self) -> check_api.Result:
        return await self._run(self.config["check_conditions"])

    async def cancel(self) -> check_api.Result:
        if self.config["always_show"]:
            return await self._run(self.config["check_conditions"])
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
            default="'{{ check_rule_name }}' {% if check_succeeded %}succeeded{% else %}failed{% endif %}",
        ): CheckRunJinja2,
        voluptuous.Required(
            "summary", default="{{ check_conditions }}"
        ): CheckRunJinja2,
        voluptuous.Required("success_conditions", default=None): voluptuous.Any(
            None,
            voluptuous.All(
                [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                voluptuous.Coerce(conditions.PullRequestRuleConditions),
            ),
        ),
    }

    executor_class = PostCheckExecutor
