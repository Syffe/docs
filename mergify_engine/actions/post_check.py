# -*- encoding: utf-8 -*-
#
#  Copyright © 2020 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.dashboard import subscription
from mergify_engine.rules import conditions
from mergify_engine.rules import types


def CheckRunJinja2(v: typing.Any) -> typing.Optional[str]:
    return types.Jinja2(
        v,
        {
            "check_rule_name": "whatever",
            "check_succeed": True,
            "check_conditions": "the expected condition conditions",
        },
    )


class PostCheckAction(actions.Action):

    flags = (
        actions.ActionFlag.ALWAYS_RUN
        | actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        | actions.ActionFlag.ALLOW_RETRIGGER_MERGIFY
    )
    validator = {
        voluptuous.Required(
            "title",
            default="'{{ check_rule_name }}' {% if check_succeed %}succeed{% else %}failed{% endif %}",  # noqa:FS003
        ): CheckRunJinja2,
        voluptuous.Required(
            "summary", default="{{ check_conditions }}"
        ): CheckRunJinja2,
        voluptuous.Required("success_conditions", default=None): voluptuous.Any(
            None,
            voluptuous.All(
                [voluptuous.Coerce(rules.RuleConditionSchema)],
                voluptuous.Coerce(conditions.PullRequestRuleConditions),
            ),
        ),
    }

    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
        if self.config["success_conditions"] is None:
            return ()
        else:
            return (check_api.Conclusion.CANCELLED,)

    async def _run(
        self,
        ctxt: context.Context,
        rule: rules.EvaluatedRule,
        check_conditions: conditions.PullRequestRuleConditions,
    ) -> check_api.Result:
        # TODO(sileht): Don't run it if conditions contains the rule itself, as it can
        # created an endless loop of events.

        if not ctxt.subscription.has_feature(subscription.Features.CUSTOM_CHECKS):
            return check_api.Result(
                check_api.Conclusion.ACTION_REQUIRED,
                "Custom checks are disabled",
                ctxt.subscription.missing_feature_reason(
                    ctxt.pull["base"]["repo"]["owner"]["login"]
                ),
            )

        extra_variables: typing.Dict[str, typing.Union[str, bool]] = {
            "check_rule_name": rule.name,
            "check_succeeded": check_conditions.match,
            "check_conditions": check_conditions.get_summary(),
            # Backward compat
            "check_succeed": check_conditions.match,
        }
        try:
            title = await ctxt.pull_request.render_template(
                self.config["title"],
                extra_variables,
            )
        except context.RenderTemplateFailure as rmf:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Invalid title template",
                str(rmf),
            )

        try:
            summary = await ctxt.pull_request.render_template(
                self.config["summary"], extra_variables
            )
        except context.RenderTemplateFailure as rmf:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Invalid summary template",
                str(rmf),
            )
        if check_conditions.match:
            conclusion = check_api.Conclusion.SUCCESS
        else:
            conclusion = check_api.Conclusion.FAILURE

        check = await ctxt.get_engine_check_run(rule.get_check_name("post_check"))
        if (
            not check
            or check["conclusion"] != conclusion.value
            or check["output"]["title"] != title
            or check["output"]["summary"] != summary
        ):
            await signals.send(
                ctxt.repository,
                ctxt.pull["number"],
                "action.post_check",
                signals.EventPostCheckMetadata(
                    {
                        "conclusion": conclusion.value,
                        "title": title,
                        "summary": summary,
                    }
                ),
                rule.get_signal_trigger(),
            )

        return check_api.Result(conclusion, title, summary)

    async def run(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:
        if self.config["success_conditions"] is None:
            check_conditions = rule.conditions
        else:
            check_conditions = self.config["success_conditions"].copy()
            await check_conditions([ctxt.pull_request])
        return await self._run(ctxt, rule, check_conditions)

    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        if self.config["success_conditions"] is None:
            return await self._run(ctxt, rule, rule.conditions)
        else:
            return actions.CANCELLED_CHECK_REPORT
