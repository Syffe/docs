from __future__ import annotations

import dataclasses
import typing
import urllib.parse

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import condition_value_querier
from mergify_engine import settings
from mergify_engine import signals
from mergify_engine.clients import http
from mergify_engine.rules import types


if typing.TYPE_CHECKING:
    from mergify_engine import context
    from mergify_engine.rules.config import pull_request_rules as prr_config

MAX_DISPATCHED_EVENTS: int = 10


class GhaExecutorDispatchConfig(typing.TypedDict):
    workflow: str
    ref: str
    inputs: dict[str, str | int | bool]


class GhaExecutorConfig(typing.TypedDict):
    workflows: list[GhaExecutorDispatchConfig]


@dataclasses.dataclass
class GhaExecutor(actions.ActionExecutor["GhaAction", GhaExecutorConfig]):
    @staticmethod
    async def _get_workflow_dispatch_config(
        action: GhaAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> list[GhaExecutorDispatchConfig]:
        pull_attrs = condition_value_querier.PullRequest(ctxt)
        workflows = []

        for wf in action.config["workflow"]["dispatch"]:
            if (ref := wf["ref"]) is None:
                ref = ctxt.repository.repo["default_branch"]

            inputs: dict[str, str | int | bool] = {}
            for ik, iv in wf["inputs"].items():
                if isinstance(iv, str):
                    try:
                        inputs[ik] = await pull_attrs.render_template(iv)
                    except condition_value_querier.RenderTemplateFailureError as rmf:
                        raise actions.InvalidDynamicActionConfigurationError(
                            rule,
                            action,
                            "Invalid input value",
                            str(rmf),
                        )
                else:
                    inputs[ik] = iv

            workflows.append(
                GhaExecutorDispatchConfig(
                    {
                        "workflow": wf["workflow"],
                        "ref": ref,
                        "inputs": inputs,
                    },
                ),
            )
        return workflows

    @classmethod
    async def create(
        cls,
        action: GhaAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> GhaExecutor:
        try:
            config = GhaExecutorConfig(
                workflows=await cls._get_workflow_dispatch_config(action, ctxt, rule),
            )
        except KeyError as e:
            raise actions.InvalidDynamicActionConfigurationError(
                rule,
                action,
                "Workflow not found",
                f"Workflow `{e.args[0]}` was not found in the repository or is inactive.",
            )

        return cls(ctxt, rule, config)

    async def run(self) -> check_api.Result:
        results = [await self._dispatch_workflow(wf) for wf in self.config["workflows"]]

        # only one workflow was dispatched -> its result can be returned directly
        if len(results) == 1:
            return results[0]

        success_workflows: list[check_api.Result] = []
        failed_workflows: list[check_api.Result] = []
        for r in results:
            result_list = (
                success_workflows
                if r.conclusion == check_api.Conclusion.SUCCESS
                else failed_workflows
            )
            result_list.append(r)

        if any(failed_workflows):
            summary = (
                f"Workflow{'s'*(len(failed_workflows)>1)} dispatch failed:\n- "
                + "\n- ".join(wf.summary for wf in failed_workflows)
            )
            if any(success_workflows):
                summary += (
                    f"\n\nWorkflow{'s'*(len(success_workflows)>1)} successfully dispatched:\n- "
                    + "\n- ".join(f"`{r.summary}`" for r in success_workflows)
                )
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                f"Some workflow{'s'*(len(failed_workflows)>1)} dispatch{'es'*(len(failed_workflows)>1)} failed",
                summary,
            )

        dispatched_workflows = [
            f"`{wf['workflow']}`" for wf in self.config["workflows"]
        ]
        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "Workflows dispatched",
            f"Successfully dispatched workflows {', '.join(dispatched_workflows)}.",
        )

    async def _dispatch_workflow(
        self,
        wf: GhaExecutorDispatchConfig,
    ) -> check_api.Result:
        base_err_msg = f"Failed to dispatch workflow `{wf['workflow']}`. "

        try:
            await self.ctxt.client.post(
                f"{self.ctxt.base_url}/actions/workflows/{wf['workflow']}/dispatches",
                json={
                    "ref": wf["ref"],
                    "inputs": wf["inputs"],
                },
            )
        except http.HTTPForbiddenError as e:
            if not self.ctxt.github_actions_controllable():
                err_msg = (
                    base_err_msg
                    + "The new Mergify permissions must be accepted to dispatch actions.\n"
                    + f"You can accept them at {settings.DASHBOARD_UI_FRONT_URL}"
                )
            else:
                err_msg = base_err_msg + e.message

            return check_api.Result(
                check_api.Conclusion.FAILURE,
                "Unauthorized to dispatch workflow",
                err_msg,
            )
        except http.HTTPClientSideError as e:
            return check_api.Result(
                check_api.Conclusion.FAILURE,
                f"Failed to dispatch workflow `{wf['workflow']}`",
                base_err_msg + e.message + ("" if e.message.endswith(".") else "."),
            )

        await signals.send(
            self.ctxt.repository,
            self.ctxt.pull["number"],
            self.ctxt.pull["base"]["ref"],
            "action.github_actions",
            signals.EventGithubActionsMetadata(
                workflow=wf["workflow"],
                inputs=wf["inputs"],
            ),
            self.rule.get_signal_trigger(),
        )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "Workflow dispatched successfully",
            wf["workflow"],
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class GhaAction(actions.Action):
    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("workflow"): {
            voluptuous.Required("dispatch", default=list): voluptuous.All(
                [
                    {
                        voluptuous.Required("workflow"): voluptuous.All(
                            str,
                            urllib.parse.quote,
                        ),
                        voluptuous.Required("ref", default=None): voluptuous.Any(
                            str,
                            None,
                        ),
                        voluptuous.Required("inputs", default={}): voluptuous.All(
                            {str: voluptuous.Any(types.Jinja2, int, bool)},
                            voluptuous.Length(max=10),
                        ),
                    },
                ],
                voluptuous.Length(max=MAX_DISPATCHED_EVENTS),
            ),
        },
    }
    executor_class = GhaExecutor
