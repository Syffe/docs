import dataclasses
import datetime
import typing

from mergify_engine import github_types
from mergify_engine import queue


if typing.TYPE_CHECKING:
    from mergify_engine.queue.merge_train.train import Train


@dataclasses.dataclass
class EmbarkedPull:
    train: "Train" = dataclasses.field(repr=False)
    user_pull_request_number: github_types.GitHubPullRequestNumber
    config: queue.PullQueueConfig
    queued_at: datetime.datetime

    class Serialized(typing.TypedDict):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime

    class OldSerialized(typing.NamedTuple):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime

    @classmethod
    def deserialize(
        cls,
        train: "Train",
        data: "EmbarkedPull.Serialized | EmbarkedPull.OldSerialized",
    ) -> "EmbarkedPull":
        if isinstance(data, (tuple, list)):
            user_pull_request_number = data[0]
            config = data[1]
            queued_at = data[2]

            return cls(
                train=train,
                user_pull_request_number=user_pull_request_number,
                config=config,
                queued_at=queued_at,
            )
        else:
            return cls(
                train=train,
                user_pull_request_number=data["user_pull_request_number"],
                config=data["config"],
                queued_at=data["queued_at"],
            )

    def serialized(self) -> "EmbarkedPull.Serialized":
        return self.Serialized(
            user_pull_request_number=self.user_pull_request_number,
            config=self.config,
            queued_at=self.queued_at,
        )
