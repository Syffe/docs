import logging
import typing

import first
import pydantic
import yaml

from mergify_engine import dependabot_types


def get_dependabot_consolidated_data_from_commit_msg(
    log: "logging.LoggerAdapter[logging.Logger]",
    commit_msg: str,
) -> typing.Optional[dependabot_types.DependabotAttributes]:
    """
    Returned dict example:
    {
        'dependency-name': 'bootstrap',
        'dependency-type': 'direct:development',
        'update-type': 'version-update:semver-minor',
     }
    """
    try:
        yaml_str = commit_msg[commit_msg.index("---\n") : commit_msg.rindex("...\n")]
    except (IndexError, ValueError):
        log.error(
            "Cannot parse dependabot commit message correctly",
            commit_message=commit_msg,
            exc_info=True,
        )
        return None

    try:
        data_from_yaml = yaml.safe_load(yaml_str)
    except (yaml.parser.ParserError, yaml.scanner.ScannerError):
        log.error(
            "Cannot parse dependabot commit message correctly",
            commit_message=commit_msg,
            exc_info=True,
        )
        return None

    try:
        dependabot_data = dependabot_types.DependabotYamlMessageSchema.parse_obj(
            data_from_yaml
        )
    except pydantic.ValidationError:
        log.error(
            "Cannot parse dependabot commit message correctly",
            commit_message=commit_msg,
            exc_info=True,
        )
        return None

    return first.first(dependabot_data.updated_dependencies)
