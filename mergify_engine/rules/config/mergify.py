from __future__ import annotations

import dataclasses
import functools
import operator
import re
import typing

import voluptuous

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine import utils
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

EXTENDED_REPOSITORY_NAME = re.compile(
    rf"\s*extends:\s*(?P<repository_name>\.?{utils.MERGIFY_REPOSITORY_NAME_CHARACTER_GROUP})",
)


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
    extended_anchors: dict[str, typing.Any]
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


async def get_fully_extended_mergify_config(
    repository_ctxt: context.Repository,
    repo_name: github_types.GitHubRepositoryName,
    config_file: context.MergifyConfigFile,
) -> MergifyConfig:
    config_to_extend = await get_mergify_extended_config(
        repository_ctxt,
        repo_name,
        config_file["path"],
    )
    try:
        config = rules.YamlSchemaWithExtendedAnchors(
            (config_file["decoded_content"], config_to_extend["extended_anchors"]),
        )
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, config_file["path"])

    # Allow an empty file
    if config is None:
        config = {}
    return await get_mergify_config_from_dict(
        config=config,
        error_path=config_file["path"],
        extended_config=config_to_extend,
    )


async def get_mergify_config_with_extracted_anchors_from_extended_file(
    config_file: context.MergifyConfigFile,
) -> MergifyConfig:
    config = anchors = None

    try:
        parsed_config = rules.YamlAnchorsExtractorSchema(
            config_file["decoded_content"],
        )
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, config_file["path"])

    if parsed_config is not None:
        config, anchors = parsed_config

    # Allow an empty file
    if config is None:
        config = {}

    return await get_mergify_config_from_dict(
        config=config,
        error_path=config_file["path"],
        anchors=anchors,
    )


async def get_mergify_config_from_base_file(
    config_file: context.MergifyConfigFile,
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
        config=config,
        error_path=config_file["path"],
    )


def get_extended_repository_name(
    raw_config: str,
) -> github_types.GitHubRepositoryName | None:
    # NOTE(Syffe): we clean the conf of all possible characters used in YAML to set a value to a key.
    # afterward we search for the extends pattern and the repository name.
    # This separation of cleaning then searching is done for maintenance and readability purposes.
    cleaned_config = re.sub(r"(?<=[>|])-|>|\||\'|\"|\\", "", raw_config)
    extended_repository_name_pattern = EXTENDED_REPOSITORY_NAME.search(
        cleaned_config,
    )
    if extended_repository_name_pattern is not None:
        return extended_repository_name_pattern.group("repository_name")
    return None

    # extended_repository_name_pattern = EXTENDED_REPOSITORY_NAME.search(
    #     raw_config,
    # )
    # if extended_repository_name_pattern is not None:
    #     # NOTE(Syffe): we always match several groups, the one we want is the second, so number 1
    #     extended_repository_name = rules.types.GitHubRepositoryName(
    #         extended_repository_name_pattern.groupdict("repo_name"),
    #     )
    #     return github_types.GitHubRepositoryName(extended_repository_name)
    # return None


async def get_mergify_config_from_file(
    repository_ctxt: context.Repository,
    config_file: context.MergifyConfigFile,
    called_from_extend: bool = False,
) -> MergifyConfig:
    extended_repository_name = get_extended_repository_name(
        config_file["decoded_content"],
    )
    if extended_repository_name is not None:
        if called_from_extend:
            # NOTE (Syffe): We don't want to allow infinite extends. This code is reached when we are calling
            # get_mergify_config_from_file from get_fully_extended_mergify_config, which means
            # that is an extended_repository_name exists here, we are in a case where the user is trying
            # to chain several configuration extensions.
            raise InvalidRulesError(
                voluptuous.Invalid(
                    "Maximum number of extended configuration reached. Limit is 1.",
                    ["extends"],
                ),
                config_file["path"],
            )

        return await get_fully_extended_mergify_config(
            repository_ctxt,
            extended_repository_name,
            config_file,
        )

    if called_from_extend:
        return await get_mergify_config_with_extracted_anchors_from_extended_file(
            config_file,
        )

    return await get_mergify_config_from_base_file(config_file)


async def get_mergify_config_from_dict(
    config: dict[str, typing.Any],
    error_path: str,
    anchors: dict[str, typing.Any] | None = None,
    extended_config: MergifyConfig | None = None,
) -> MergifyConfig:
    try:
        rules.UserConfigurationSchema(config)
    except voluptuous.Invalid as e:
        raise InvalidRulesError(e, error_path)

    defaults = config.pop("defaults", {})

    if extended_config is not None:
        merge_defaults(extended_config["defaults"], defaults)
        merge_raw_configs(extended_config["raw_config"], config)
        anchors = extended_config["extended_anchors"]

    merge_config_with_defaults(config, defaults)
    try:
        final_config = rules.UserConfigurationSchema(config)
        final_config["defaults"] = defaults
        final_config["raw_config"] = config
        final_config["extended_anchors"] = anchors if anchors is not None else {}

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
        called_from_extend=True,
    )
