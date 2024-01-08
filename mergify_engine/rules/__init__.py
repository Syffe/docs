from __future__ import annotations

import contextlib
import typing

import daiquiri
from ddtrace import tracer
import voluptuous
import yaml

from mergify_engine import actions as actions_mod
from mergify_engine import github_types
from mergify_engine.rules import types
from mergify_engine.rules.config import defaults as defaults_config
from mergify_engine.yaml import yaml as mergify_yaml


if typing.TYPE_CHECKING:
    from collections import abc


LOG = daiquiri.getLogger(__name__)


class YAMLInvalid(voluptuous.Invalid):  # type: ignore[misc]
    def __str__(self) -> str:
        return f"{self.msg} at {self.path}"

    def get_annotations(self, path: str) -> list[github_types.GitHubAnnotation]:
        if self.path:
            error_path = self.path[0]
            return [
                {
                    "path": path,
                    "start_line": error_path.line,
                    "end_line": error_path.line,
                    "start_column": error_path.column,
                    "end_column": error_path.column,
                    "annotation_level": "failure",
                    "message": self.error_message,
                    "title": self.msg,
                },
            ]
        return []


@contextlib.contextmanager
def convert_yaml_error_into_voluptuous_error() -> abc.Generator[None, None, None]:
    try:
        yield
    except yaml.MarkedYAMLError as e:
        path = []
        if e.problem_mark is not None:
            path.append(
                types.LineColumnPath(
                    e.problem_mark.line + 1,
                    e.problem_mark.column + 1,
                ),
            )
        raise YAMLInvalid(
            message="Invalid YAML",
            error_message=str(e),
            path=path,
        )

    except yaml.YAMLError as e:
        raise YAMLInvalid(message="Invalid YAML", error_message=str(e))


@tracer.wrap("yaml.load")
def YAML(v: str) -> typing.Any:
    with convert_yaml_error_into_voluptuous_error():
        return mergify_yaml.safe_load(v)


@tracer.wrap("yaml.anchor_extractor_load")
def YAMLAnchorExtractor(v: str) -> typing.Any:
    with convert_yaml_error_into_voluptuous_error():
        return mergify_yaml.anchor_extractor_load(v)


@tracer.wrap("yaml.extended_anchors_load")
def YAMLExtendedAnchors(
    yaml_content: str,
    anchors: dict[str, typing.Any],
) -> typing.Any:
    with convert_yaml_error_into_voluptuous_error():
        return mergify_yaml.extended_anchors_load(yaml_content, anchors)


def UserConfigurationSchema(
    config: dict[str, typing.Any],
) -> voluptuous.Schema:
    # Circular import
    from mergify_engine.rules.config import partition_rules as partr_config
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config

    schema = {
        voluptuous.Required("extends", default=None): voluptuous.Any(
            None,
            types.GitHubRepositoryName,
        ),
        voluptuous.Required(
            "pull_request_rules",
            default=[],
        ): prr_config.get_pull_request_rules_schema(),
        voluptuous.Required(
            "queue_rules",
            default=[
                {
                    "name": "default",
                    "priority_rules": [],
                    "merge_conditions": [],
                    "queue_conditions": [],
                },
            ],
        ): qr_config.QueueRulesSchema,
        voluptuous.Required(
            "partition_rules",
            default=[],
        ): partr_config.PartitionRulesSchema,
        voluptuous.Required("commands_restrictions", default={}): {
            voluptuous.Required(
                name,
                default={},
            ): prr_config.CommandsRestrictionsSchema(command)
            for name, command in actions_mod.get_commands().items()
        },
        voluptuous.Required(
            "defaults",
            default={},
        ): defaults_config.get_defaults_schema(),
        voluptuous.Required(
            "_checks_to_retry_on_failure",
            default={},
        ): voluptuous.Schema({str: int}),
        voluptuous.Remove("shared"): voluptuous.Any(dict, list, str, int, float, bool),
    }

    return voluptuous.Schema(schema)(config)


YamlSchema: abc.Callable[[str], typing.Any] = voluptuous.Schema(voluptuous.Coerce(YAML))
YamlAnchorsExtractorSchema: abc.Callable[[str], typing.Any] = voluptuous.Schema(
    voluptuous.Coerce(YAMLAnchorExtractor),
)
# NOTE(Syffe): The parameter is a tuple containing the yaml content to parse/load
# and the list of anchors that will be extended into the file.
YamlSchemaWithExtendedAnchors: abc.Callable[
    [tuple[str, dict[str, typing.Any]]],
    typing.Any,
] = voluptuous.Schema(
    voluptuous.Coerce(
        lambda v: YAMLExtendedAnchors(
            yaml_content=v[0],
            anchors=v[1],
        ),
    ),
)
