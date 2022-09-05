import enum
import typing

import daiquiri
import voluptuous

from mergify_engine import github_types


if typing.TYPE_CHECKING:
    from mergify_engine import rules

LOG = daiquiri.getLogger(__name__)


class PriorityAliases(enum.Enum):
    low = 1000
    medium = 2000
    high = 3000


def Priority(v: typing.Union[int, str]) -> int:
    if isinstance(v, int):
        return v

    return PriorityAliases[v].value


MAX_PRIORITY: int = 10000
# NOTE(sileht): We use the max priority as an offset to order queue
QUEUE_PRIORITY_OFFSET: int = MAX_PRIORITY

PrioritySchema = voluptuous.All(
    voluptuous.Any("low", "medium", "high", int),
    voluptuous.Coerce(Priority),
    int,
    voluptuous.Range(min=1, max=MAX_PRIORITY),
)


class PullQueueConfig(typing.TypedDict):
    strict_method: typing.Literal["merge", "rebase"]
    update_method: typing.Literal["merge", "rebase"]
    priority: int
    effective_priority: int
    bot_account: typing.Optional[github_types.GitHubLogin]
    update_bot_account: typing.Optional[github_types.GitHubLogin]
    name: "rules.QueueName"
