from urllib import parse

import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.clients import http


class LabelAction(actions.BackwardCompatAction):
    flags = (
        actions.ActionFlag.ALLOW_ON_CONFIGURATION_CHANGED
        | actions.ActionFlag.ALWAYS_RUN
    )

    validator = {
        voluptuous.Required("add", default=list): [str],
        voluptuous.Required("remove", default=list): [str],
        voluptuous.Required("remove_all", default=False): bool,
        voluptuous.Required("toggle", default=list): [str],
    }

    async def _add_labels_to_pull(
        self, ctxt: context.Context, labels: list[str]
    ) -> set[str]:
        missing_labels = {label.lower() for label in labels} - ctxt.pull_labels_names
        if missing_labels:
            await ctxt.client.post(
                f"{ctxt.base_url}/issues/{ctxt.pull['number']}/labels",
                json={"labels": list(missing_labels)},
            )
            labels_by_name = {
                _l["name"].lower(): _l for _l in await ctxt.repository.get_labels()
            }
            ctxt.pull["labels"].extend(
                [labels_by_name[label_name] for label_name in missing_labels]
            )
            return {labels_by_name[label_name]["name"] for label_name in missing_labels}
        return set()

    async def _remove_labels_from_pull(
        self, ctxt: context.Context, labels: list[str]
    ) -> set[str]:
        labels_removed: set[str] = set()
        for label in labels:
            if label.lower() in ctxt.pull_labels_names:
                label_escaped = parse.quote(label, safe="")
                try:
                    await ctxt.client.delete(
                        f"{ctxt.base_url}/issues/{ctxt.pull['number']}/labels/{label_escaped}"
                    )
                except http.HTTPClientSideError as e:
                    ctxt.log.warning(
                        "failed to delete label",
                        label=label,
                        status_code=e.status_code,
                        error_message=e.message,
                    )
                    continue

                ctxt.pull["labels"] = [
                    _l
                    for _l in ctxt.pull["labels"]
                    if _l["name"].lower() != label.lower()
                ]
                labels_removed.add(label)

        return labels_removed

    async def run(
        self, ctxt: context.Context, rule: rules.EvaluatedRule
    ) -> check_api.Result:
        labels_added: set[str] = set()
        labels_removed: set[str] = set()

        if self.config["toggle"]:
            for label in self.config["toggle"]:
                await ctxt.repository.ensure_label_exists(label)
            labels_added = await self._add_labels_to_pull(ctxt, self.config["toggle"])

        if self.config["add"]:
            for label in self.config["add"]:
                await ctxt.repository.ensure_label_exists(label)

            labels_added |= await self._add_labels_to_pull(ctxt, self.config["add"])

        if self.config["remove_all"]:
            if ctxt.pull["labels"]:
                await ctxt.client.delete(
                    f"{ctxt.base_url}/issues/{ctxt.pull['number']}/labels"
                )
                labels_removed = ctxt.pull_labels_names
                ctxt.pull["labels"] = []

        elif self.config["remove"]:
            labels_removed = await self._remove_labels_from_pull(
                ctxt, self.config["remove"]
            )

        if labels_added or labels_removed:
            await signals.send(
                ctxt.repository,
                ctxt.pull["number"],
                "action.label",
                signals.EventLabelMetadata(
                    {"added": sorted(labels_added), "removed": sorted(labels_removed)}
                ),
                rule.get_signal_trigger(),
            )

            return check_api.Result(
                check_api.Conclusion.SUCCESS, "Labels added/removed", ""
            )
        else:
            return check_api.Result(
                check_api.Conclusion.SUCCESS, "No label to add or remove", ""
            )

    async def cancel(
        self, ctxt: context.Context, rule: "rules.EvaluatedRule"
    ) -> check_api.Result:  # pragma: no cover
        if not self.config["toggle"]:
            return actions.CANCELLED_CHECK_REPORT

        labels_removed = await self._remove_labels_from_pull(
            ctxt, self.config["toggle"]
        )

        if labels_removed:
            await signals.send(
                ctxt.repository,
                ctxt.pull["number"],
                "action.label",
                signals.EventLabelMetadata(
                    {"added": [], "removed": sorted(labels_removed)}
                ),
                rule.get_signal_trigger(),
            )
            return check_api.Result(check_api.Conclusion.SUCCESS, "Labels removed", "")

        return check_api.Result(check_api.Conclusion.SUCCESS, "No label to remove", "")
