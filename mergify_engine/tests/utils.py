from unittest import mock

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import rules


async def load_mergify_config(content: str) -> rules.MergifyConfig:
    file = context.MergifyConfigFile(
        type="file",
        content="whatever",
        sha=github_types.SHAType("azertyuiop"),
        path=github_types.GitHubFilePath("whatever"),
        decoded_content=content,
    )

    return await rules.get_mergify_config_from_file(mock.MagicMock(), file)
