import datetime

from . import github_types


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
MERGE_QUEUE_BODY_INFO = {
    "merge-queue-pr": True,
}
MERGE_QUEUE_SUMMARY_NAME = "Queue: Embarked in merge train"
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
MERGIFY_MERGE_QUEUE_PULL_REQUEST_DOC = """

---

More informations about Mergify merge queue can be found in the [documentation](https://docs.mergify.com/actions/queue.html).

<details>
<summary>Mergify commands</summary>

<br />

You can also trigger Mergify actions by commenting on this pull request:

- `@Mergifyio refresh` will re-evaluate the queue rules

Additionally, on Mergify [dashboard](https://dashboard.mergify.com) you can:

- look at your merge queues
- generate the Mergify configuration with the config editor.

Finally, you can contact us on https://mergify.com
</details>
"""

MERGIFY_PULL_REQUEST_DOC = """
<details>
<summary>Mergify commands and options</summary>

<br />

More conditions and actions can be found in the [documentation](https://docs.mergify.com/).

You can also trigger Mergify actions by commenting on this pull request:

- `@Mergifyio refresh` will re-evaluate the rules
- `@Mergifyio rebase` will rebase this PR on its base branch
- `@Mergifyio update` will merge the base branch into this PR
- `@Mergifyio backport <destination>` will backport this PR on `<destination>` branch

Additionally, on Mergify [dashboard](https://dashboard.mergify.com/) you can:

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


DEPRECATED_CURRENT_CONDITIONS_NAMES = (
    "current-time",
    "current-day-of-week",
    "current-day",
    "current-month",
    "current-year",
    "current-timestamp",
)
DEPRECATED_CURRENT_CONDITIONS_MESSAGE = f"""⚠️  The following conditions are deprecated and must be replaced with the `schedule` condition: {', '.join([f"`{n}`" for n in DEPRECATED_CURRENT_CONDITIONS_NAMES])}.
A brownout day is planned for the whole day of January 11th, 2023.
Those conditions will be removed on February 11th, 2023.

For more informations and examples on how to use the `schedule` condition: https://docs.mergify.com/conditions/#attributes, https://docs.mergify.com/configuration/#time
"""

DEPRECATED_RANDOM_USER_PICK = """⚠️  This pull request got {verb} on behalf of a random user of the organization.
This behavior will change on the 1st February 2023, Mergify will pick the author of the pull request instead.

To get the future behavior now, you can configure `bot_account` options (e.g.: `bot_account: {{ author }}` or `update_bot_account: {{ author }}`.

Or you can create a dedicated github account for squash and rebase operations, and use it in different `bot_account` options.
"""
