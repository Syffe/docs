import datetime

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import utils


GITHUB_PULL_REQUEST_BODY_MAX_SIZE = 65535
MERGIFY_CONFIG_FILENAMES: list[github_types.GitHubFilePath] = [
    github_types.GitHubFilePath(".mergify.yml"),
    github_types.GitHubFilePath(".mergify/config.yml"),
    github_types.GitHubFilePath(".github/mergify.yml"),
]

SUMMARY_NAME = "Summary"

MERGE_QUEUE_BRANCH_PREFIX = "mergify/merge-queue/"
# Payload to put in the body of the main comment of a pull request
# for us to be able to know that this pull request is a merge queue pull request
MERGE_QUEUE_BODY_INFO = utils.MergifyCommentHiddenPayload({"merge-queue-pr": True})

MERGE_QUEUE_OLD_SUMMARY_NAME = "Queue: Embarked in merge train"
MERGE_QUEUE_SUMMARY_NAME = "Queue: Embarked in merge queue"
CONFIGURATION_CHANGED_CHECK_NAME = "Configuration changed"
CONFIGURATION_DELETED_CHECK_NAME = "Configuration has been deleted"
CONFIGURATION_MUTIPLE_FOUND_SUMMARY_TITLE = (
    "Multiple Mergify configurations have been found in the repository"
)
INITIAL_SUMMARY_TITLE = "Your rules are under evaluation"

CHECKS_TIMEOUT_CONDITION_LABEL = "checks-are-on-time"

MERGIFY_OPENSOURCE_SPONSOR_DOC = (
    "<hr />\n"
    ":sparkling_heart:&nbsp;&nbsp;Mergify is proud to provide this service "
    "for free to open source projects.\n\n"
    ":rocket:&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!\n"
)

MERGIFY_PULL_REQUEST_DOC = f"""
<details>
<summary>Mergify commands and options</summary>

<br />

More conditions and actions can be found in the [documentation](https://docs.mergify.com/).

You can also trigger Mergify actions by commenting on this pull request:

- `@Mergifyio refresh` will re-evaluate the rules
- `@Mergifyio rebase` will rebase this PR on its base branch
- `@Mergifyio update` will merge the base branch into this PR
- `@Mergifyio backport <destination>` will backport this PR on `<destination>` branch

Additionally, on Mergify [dashboard]({settings.DASHBOARD_UI_FRONT_URL}) you can:

- look at your merge queues
- generate the Mergify configuration with the config editor.

Finally, you can contact us on https://mergify.com
</details>
"""

DEPENDABOT_PULL_REQUEST_AUTHOR_LOGIN = "dependabot[bot]"


# usual delay to wait between two processing of the same PR
NORMAL_DELAY_BETWEEN_SAME_PULL_REQUEST = datetime.timedelta(seconds=30)
# minimun delay to wait between two processing of the same PR
MIN_DELAY_BETWEEN_SAME_PULL_REQUEST = datetime.timedelta(seconds=3)

NEW_MERGIFY_PERMISSIONS_MUST_BE_ACCEPTED = f"""The new Mergify permissions must be accepted to rebase pull request with `.github/workflows` changes.
You can accept them at {settings.DASHBOARD_UI_FRONT_URL}.
"""

# NOTE(Kontrolix): 1536 is the output dimension of OpenAI Embedding at this time
# https://platform.openai.com/docs/guides/embeddings/second-generation-models
OPENAI_EMBEDDING_DIMENSION = 1536
