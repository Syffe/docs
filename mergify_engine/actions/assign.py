from __future__ import annotations

import typing

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import signals
from mergify_engine.actions import utils as actions_utils
from mergify_engine.clients import http
from mergify_engine.rules import types
from mergify_engine.rules.config import pull_request_rules as prr_config


if typing.TYPE_CHECKING:
    from mergify_engine import context


class AssignExecutorConfig(typing.TypedDict):
    users_to_add: set[str]
    users_to_remove: set[str]


class AssignExecutor(actions.ActionExecutor["AssignAction", AssignExecutorConfig]):
    @classmethod
    async def create(
        cls,
        action: AssignAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> AssignExecutor:
        # NOTE: "users" is deprecated, but kept as legacy code for old config
        add_users = action.config["users"] + action.config["add_users"]
        users_to_add_parsed = {
            user
            for user in await actions_utils.render_users_template(ctxt, add_users)
            if not user.endswith("[bot]")
        }
        users_to_remove_parsed = await actions_utils.render_users_template(
            ctxt, action.config["remove_users"]
        )
        return cls(
            ctxt,
            rule,
            AssignExecutorConfig(
                {
                    "users_to_add": users_to_add_parsed,
                    "users_to_remove": users_to_remove_parsed,
                }
            ),
        )

    async def run(self) -> check_api.Result:
        assignees_to_add = list(
            self.config["users_to_add"]
            - {a["login"] for a in self.ctxt.pull["assignees"]}
        )
        if assignees_to_add:
            try:
                await self.ctxt.client.post(
                    f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/assignees",
                    json={"assignees": assignees_to_add},
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                return check_api.Result(
                    check_api.Conclusion.PENDING,
                    "Unable to add assignees",
                    f"GitHub error: [{e.status_code}] `{e.message}`",
                )

        assignees_to_remove = list(
            self.config["users_to_remove"]
            & {a["login"] for a in self.ctxt.pull["assignees"]}
        )
        if assignees_to_remove:
            try:
                await self.ctxt.client.request(
                    "DELETE",
                    f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/assignees",
                    json={"assignees": assignees_to_remove},
                )
            except http.HTTPClientSideError as e:  # pragma: no cover
                return check_api.Result(
                    check_api.Conclusion.PENDING,
                    "Unable to remove assignees",
                    f"GitHub error: [{e.status_code}] `{e.message}`",
                )

        if assignees_to_add or assignees_to_remove:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                self.ctxt.pull["base"]["ref"],
                "action.assign",
                signals.EventAssignMetadata(
                    {"added": assignees_to_add, "removed": assignees_to_remove}
                ),
                self.rule.get_signal_trigger(),
            )

            return check_api.Result(
                check_api.Conclusion.SUCCESS,
                "Users added/removed from assignees",
                "",
            )

        return check_api.Result(
            check_api.Conclusion.SUCCESS,
            "No users added/removed from assignees",
            "",
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        return actions.CANCELLED_CHECK_REPORT


class AssignAction(actions.Action):
    validator: typing.ClassVar[actions.ValidatorT] = {
        # NOTE: "users" is deprecated, but kept as legacy code for old config
        voluptuous.Required("users", default=list): [types.Jinja2],
        voluptuous.Required("add_users", default=list): [types.Jinja2],
        voluptuous.Required("remove_users", default=list): [types.Jinja2],
    }

    flags = actions.ActionFlag.ALWAYS_RUN
    executor_class = AssignExecutor
