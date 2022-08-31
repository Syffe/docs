# -*- encoding: utf-8 -*-
#
#  Copyright © 2018 Mehdi Abaakouk <sileht@sileht.net>
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
from mergify_engine.clients import http
from mergify_engine.rules import conditions


class DeleteHeadBranchExecutorConfig(typing.TypedDict):
    force: bool


class DeleteHeadBranchExecutor(
    actions.ActionExecutor["DeleteHeadBranchAction", DeleteHeadBranchExecutorConfig]
):
    @classmethod
    async def create(
        cls,
        action: "DeleteHeadBranchAction",
        ctxt: "context.Context",
        rule: "rules.EvaluatedRule",
    ) -> "DeleteHeadBranchExecutor":
        return cls(
            ctxt, rule, DeleteHeadBranchExecutorConfig(force=action.config["force"])
        )

    async def run(self) -> check_api.Result:
        if self.ctxt.pull_from_fork:
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Pull request come from fork", ""
            )

        if not self.config["force"]:
            pulls_using_this_branch = [
                pull
                async for pull in self.ctxt.client.items(
                    f"{self.ctxt.base_url}/pulls",
                    resource_name="pulls",
                    page_limit=20,
                    params={"base": self.ctxt.pull["head"]["ref"]},
                )
            ] + [
                pull
                async for pull in self.ctxt.client.items(
                    f"{self.ctxt.base_url}/pulls",
                    resource_name="pulls",
                    page_limit=5,
                    params={"head": self.ctxt.pull["head"]["label"]},
                )
                if pull["number"] is not self.ctxt.pull["number"]
            ]
            if pulls_using_this_branch:
                pulls_using_this_branch_formatted = "\n".join(
                    f"* Pull request #{p['number']}" for p in pulls_using_this_branch
                )
                return check_api.Result(
                    check_api.Conclusion.NEUTRAL,
                    "Not deleting the head branch",
                    f"Branch `{self.ctxt.pull['head']['ref']}` was not deleted "
                    f"because it is used by:\n{pulls_using_this_branch_formatted}",
                )

        try:
            existed = await self.ctxt.repository.delete_branch_if_exists(
                self.ctxt.pull["head"]["ref"]
            )
        except http.HTTPClientSideError as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Unable to delete the head branch",
                f"GitHub error: [{e.status_code}] `{e.message}`",
            )

        if not existed:
            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                f"Branch `{self.ctxt.pull['head']['ref']}` does not exist",
                "",
            )
        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            "action.delete_head_branch",
            signals.EventNoMetadata(),
            self.rule.get_signal_trigger(),
        )
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            f"Branch `{self.ctxt.pull['head']['ref']}` has been deleted",
            "",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT

    @property
    def silenced_conclusion(self) -> typing.Tuple[check_api.Conclusion, ...]:
        return ()


class DeleteHeadBranchAction(actions.Action):
    flags = (
        actions.ActionFlag.DISALLOW_RERUN_ON_OTHER_RULES
        | actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
    )
    validator = {voluptuous.Required("force", default=False): bool}
    executor_class = DeleteHeadBranchExecutor

    async def get_conditions_requirements(
        self, ctxt: context.Context
    ) -> list[conditions.RuleConditionNode]:
        return [
            conditions.RuleCondition(
                "closed", description=":pushpin: delete_head_branch requirement"
            )
        ]
