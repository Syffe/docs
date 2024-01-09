from __future__ import annotations

import dataclasses
import functools
import operator
import typing

import voluptuous

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine.clients import github
from mergify_engine.clients import http


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine.rules.config import partition_rules as partr_config
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config

MERGIFY_BUILTIN_CONFIG_YAML = """
pull_request_rules:
  - name: delete backport/copy branch (Mergify rule)
    hidden: true
    conditions:
      - author={author}
      - head~=^mergify/(bp|copy)/
    actions:
        delete_head_branch:
"""


async def get_mergify_builtin_config(
    redis_cache: redis_utils.RedisCache,
) -> voluptuous.Schema:
    mergify_bot = await github.GitHubAppInfo.get_bot(redis_cache)
    return rules.UserConfigurationSchema(
        rules.YamlSchema(
            MERGIFY_BUILTIN_CONFIG_YAML.format(author=mergify_bot["login"]),
        ),
    )


class Defaults(typing.TypedDict):
    actions: dict[str, typing.Any]


class MergifyConfig(typing.TypedDict):
    extends: github_types.GitHubRepositoryName | None
    pull_request_rules: prr_config.PullRequestRules
    queue_rules: qr_config.QueueRules
    partition_rules: partr_config.PartitionRules
    commands_restrictions: dict[str, prr_config.CommandsRestrictions]
    defaults: Defaults
    raw_config: typing.Any
    _checks_to_retry_on_failure: dict[str, int]


def merge_raw_configs(
    extended_config: dict[str, typing.Any],
    dest_config: dict[str, typing.Any],
) -> None:
    for rule_to_merge in ("pull_request_rules", "queue_rules", "partition_rules"):
        dest_rule_names = [rule["name"] for rule in dest_config.get(rule_to_merge, [])]

        for source_rule in extended_config.get(rule_to_merge, []):
            if source_rule["name"] not in dest_rule_names:
                if rule_to_merge in dest_config:
                    dest_config[rule_to_merge].append(source_rule)
                else:
                    dest_config[rule_to_merge] = [source_rule]

    for commands_restriction in extended_config.get("commands_restrictions", {}):
        dest_config["commands_restrictions"].setdefault(
            commands_restriction,
            extended_config["commands_restrictions"][commands_restriction],
        )


def merge_defaults(extended_defaults: Defaults, dest_defaults: Defaults) -> None:
    for action_name, action in extended_defaults.get("actions", {}).items():
        dest_actions = dest_defaults.setdefault("actions", {})
        dest_action = dest_actions.setdefault(action_name, {})
        for effect_name, effect_value in action.items():
            dest_action.setdefault(effect_name, effect_value)


def merge_config_with_defaults(
    config: dict[str, typing.Any],
    defaults: Defaults,
) -> None:
    if defaults_actions := defaults.get("actions"):
        for rule in config.get("pull_request_rules", []):
            actions = rule["actions"]

            for action_name, action in actions.items():
                if action_name not in defaults_actions:
                    continue
                if defaults_actions[action_name] is None:
                    continue

                if action is None:
                    rule["actions"][action_name] = defaults_actions[action_name]
                else:
                    merged_action = defaults_actions[action_name] | action
                    rule["actions"][action_name].update(merged_action)


@dataclasses.dataclass
class InvalidRulesError(Exception):
    error: voluptuous.Invalid
    filename: str

    @staticmethod
    def _format_path_item(path_item: typing.Any) -> str:
        if isinstance(path_item, int):
            return f"item {path_item}"
        return str(path_item)

    @classmethod
    def format_error(cls, error: voluptuous.Invalid) -> str:
        msg = str(error.msg)

        if error.error_type:
            msg += f" for {error.error_type}"

        if error.path:
            path = " → ".join(map(cls._format_path_item, error.path))
            msg += f" @ {path}"
        # Only include the error message if it has been provided
        # voluptuous set it to the `message` otherwise
        if error.error_message != error.msg:
            msg += f"\n```\n{error.error_message}\n```"
        return msg

    @classmethod
    def _walk_error(
        cls,
        root_error: voluptuous.Invalid,
    ) -> abc.Generator[voluptuous.Invalid, None, None]:
        if isinstance(root_error, voluptuous.MultipleInvalid):
            for error1 in root_error.errors:
                yield from cls._walk_error(error1)
        else:
            yield root_error

    @property
    def errors(self) -> list[voluptuous.Invalid]:
        return list(self._walk_error(self.error))

    def __str__(self) -> str:
        if len(self.errors) >= 2:
            return "* " + "\n* ".join(sorted(map(self.format_error, self.errors)))
        return self.format_error(self.errors[0])

    def get_annotations(self, path: str) -> list[github_types.GitHubAnnotation]:
        return functools.reduce(
            operator.add,
            (
                error.get_annotations(path)
                for error in self.errors
                if hasattr(error, "get_annotations")
            ),
            [],
        )


async def get_mergify_config_from_file(
    repository_ctxt: context.Repository,
    config_file: context.MergifyConfigFile,
    allow_extend: bool = True,
) -> MergifyConfig:
    try:
        config = rules.YamlSchema(config_file["decoded_content"])
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, config_file["path"])

    # Allow an empty file
    if config is None:
        config = {}

    # Validate defaults
    return await get_mergify_config_from_dict(
        repository_ctxt,
        config,
        config_file["path"],
        allow_extend,
    )


async def get_mergify_config_from_dict(
    repository_ctxt: context.Repository,
    config: dict[str, typing.Any],
    error_path: str,
    allow_extend: bool = True,
) -> MergifyConfig:
    try:
        rules.UserConfigurationSchema(config)
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, error_path)

    defaults = config.pop("defaults", {})

    extended_path = config.get("extends")
    if extended_path is not None:
        if not allow_extend:
            raise InvalidRulesError(
                voluptuous.Invalid(
                    "Maximum number of extended configuration reached. Limit is 1.",
                    ["extends"],
                ),
                error_path,
            )
        config_to_extend = await get_mergify_extended_config(
            repository_ctxt,
            extended_path,
            error_path,
        )
        # NOTE(jules): Anchor and shared elements can't be shared between files
        # because they are computed by rules.YamlSchema already.
        merge_defaults(config_to_extend["defaults"], defaults)
        merge_raw_configs(config_to_extend["raw_config"], config)

    merge_config_with_defaults(config, defaults)

    try:
        final_config = rules.UserConfigurationSchema(config)
        final_config["defaults"] = defaults
        final_config["raw_config"] = config
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, error_path)
    else:
        return typing.cast(MergifyConfig, final_config)


async def get_mergify_extended_config(
    repository_ctxt: context.Repository,
    extended_path: github_types.GitHubRepositoryName,
    error_path: str,
) -> MergifyConfig:
    try:
        extended_repository_ctxt = (
            await repository_ctxt.installation.get_repository_by_name(extended_path)
        )
    except http.HTTPNotFoundError as e:
        exc = InvalidRulesError(
            voluptuous.Invalid(
                f"Extended configuration repository `{extended_path}` was not found. This repository doesn't exist or Mergify is not installed on it.",
                ["extends"],
                str(e),
            ),
            error_path,
        )
        raise exc from e

    if extended_repository_ctxt.repo["id"] == repository_ctxt.repo["id"]:
        raise InvalidRulesError(
            voluptuous.Invalid(
                "Only configuration from other repositories can be extended.",
                ["extends"],
            ),
            error_path,
        )

    mergify_installed = await extended_repository_ctxt.is_mergify_installed()
    if not mergify_installed["installed"]:
        raise InvalidRulesError(
            voluptuous.Invalid(
                f"Extended configuration repository `{extended_path}` doesn't have Mergify installed on it. Mergify needs to be enabled on extended repositories to be able to detect configuration changes properly.",
                ["extends"],
                mergify_installed["error"],
            ),
            error_path,
        )

    config_file = await extended_repository_ctxt.get_mergify_config_file()
    if config_file is None:
        raise InvalidRulesError(
            voluptuous.Invalid(
                f"Extended configuration repository `{extended_path}` doesn't have a Mergify configuration file.",
                ["extends"],
            ),
            error_path,
        )

    return await get_mergify_config_from_file(
        repository_ctxt,
        config_file,
        allow_extend=False,
    )
