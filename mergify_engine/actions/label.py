from __future__ import annotations

import typing
from urllib import parse

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import signals
from mergify_engine.clients import http
from mergify_engine.rules.config import pull_request_rules as prr_config


class LabelExecutorConfig(typing.TypedDict):
    add: list[str]
    remove: list[str]
    remove_all: bool
    toggle: list[str]


class LabelExecutor(actions.ActionExecutor["LabelAction", LabelExecutorConfig]):
    @classmethod
    async def _render_label(
        cls,
        action: LabelAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
        label: str,
    ) -> str:
        try:
            return await ctxt.pull_request.render_template(label)
        except context.RenderTemplateFailure as rtf:
            raise actions.InvalidDynamicActionConfiguration(
                rule,
                action,
                f"Invalid template in label '{label}'",
                str(rtf),
            )

    @classmethod
    async def create(
        cls,
        action: LabelAction,
        ctxt: context.Context,
        rule: prr_config.EvaluatedPullRequestRule,
    ) -> LabelExecutor:
        add_labels = [
            await cls._render_label(action, ctxt, rule, label)
            for label in action.config["add"]
        ]
        remove_labels = [
            await cls._render_label(action, ctxt, rule, label)
            for label in action.config["remove"]
        ]
        toggle_labels = [
            await cls._render_label(action, ctxt, rule, label)
            for label in action.config["toggle"]
        ]

        return cls(
            ctxt,
            rule,
            LabelExecutorConfig(
                {
                    "add": add_labels,
                    "remove": remove_labels,
                    "remove_all": action.config["remove_all"],
                    "toggle": toggle_labels,
                }
            ),
        )

    async def _add_labels_to_pull(self, labels: list[str]) -> set[str]:
        missing_labels = {
            label.lower() for label in labels
        } - self.ctxt.pull_labels_names
        if missing_labels:
            await self.ctxt.client.post(
                f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/labels",
                json={"labels": list(missing_labels)},
            )
            labels_by_name = {
                _l["name"].lower(): _l for _l in await self.ctxt.repository.get_labels()
            }
            self.ctxt.pull["labels"].extend(
                [labels_by_name[label_name] for label_name in missing_labels]
            )
            return {labels_by_name[label_name]["name"] for label_name in missing_labels}
        return set()

    async def _remove_labels_from_pull(self, labels: list[str]) -> set[str]:
        labels_removed: set[str] = set()
        for label in labels:
            if label.lower() in self.ctxt.pull_labels_names:
                label_escaped = parse.quote(label, safe="")
                try:
                    await self.ctxt.client.delete(
                        f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/labels/{label_escaped}"
                    )
                except http.HTTPClientSideError as e:
                    self.ctxt.log.warning(
                        "failed to delete label",
                        label=label,
                        status_code=e.status_code,
                        error_message=e.message,
                    )
                    continue

                self.ctxt.pull["labels"] = [
                    _l
                    for _l in self.ctxt.pull["labels"]
                    if _l["name"].lower() != label.lower()
                ]
                labels_removed.add(label)

        return labels_removed

    async def run(self) -> check_api.Result:
        labels_added: set[str] = set()
        labels_removed: set[str] = set()

        if self.config["toggle"]:
            for label in self.config["toggle"]:
                await self.ctxt.repository.ensure_label_exists(label)
            labels_added = await self._add_labels_to_pull(self.config["toggle"])

        if self.config["add"]:
            for label in self.config["add"]:
                await self.ctxt.repository.ensure_label_exists(label)

            labels_added |= await self._add_labels_to_pull(self.config["add"])

        if self.config["remove_all"]:
            if self.ctxt.pull["labels"]:
                await self.ctxt.client.delete(
                    f"{self.ctxt.base_url}/issues/{self.ctxt.pull['number']}/labels"
                )
                labels_removed = self.ctxt.pull_labels_names
                self.ctxt.pull["labels"] = []

        elif self.config["remove"]:
            labels_removed = await self._remove_labels_from_pull(self.config["remove"])

        if labels_added or labels_removed:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                "action.label",
                signals.EventLabelMetadata(
                    {"added": sorted(labels_added), "removed": sorted(labels_removed)}
                ),
                self.rule.get_signal_trigger(),
            )

            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Labels added/removed", ""
            )

        return check_api.Result(
            check_api.Conclusion.SUCCESS, "No label to add or remove", ""
        )

    async def cancel(self) -> check_api.Result:  # pragma: no cover
        if not self.config["toggle"]:
            return actions.CANCELLED_CHECK_REPORT

        labels_removed = await self._remove_labels_from_pull(self.config["toggle"])

        if labels_removed:
            await signals.send(
                self.ctxt.repository,
                self.ctxt.pull["number"],
                "action.label",
                signals.EventLabelMetadata(
                    {"added": [], "removed": sorted(labels_removed)}
                ),
                self.rule.get_signal_trigger(),
            )
            return check_api.Result(check_api.Conclusion.SUCCESS, "Labels removed", "")

        return check_api.Result(check_api.Conclusion.SUCCESS, "No label to remove", "")


class LabelAction(actions.Action):
    flags = actions.ActionFlag.ALWAYS_RUN

    validator: typing.ClassVar[actions.ValidatorT] = {
        voluptuous.Required("add", default=list): [str],
        voluptuous.Required("remove", default=list): [str],
        voluptuous.Required("remove_all", default=False): bool,
        voluptuous.Required("toggle", default=list): [str],
    }
    executor_class = LabelExecutor
