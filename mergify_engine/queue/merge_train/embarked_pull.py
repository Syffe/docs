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
    restart_reason: str | None = None

    class Serialized(typing.TypedDict):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime
        restart_reason: str | None

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
        # Backward compat, introduced in 7.6.0
        if isinstance(data, tuple | list):
            user_pull_request_number = data[0]
            config = data[1]
            queued_at = data[2]

            return cls(
                train=train,
                user_pull_request_number=user_pull_request_number,
                config=config,
                queued_at=queued_at,
                restart_reason=None,
            )

        # Backward compat, introduced in 7.8.0
        if checks_end_meta := data.get("checks_end_metadata"):
            restart_reason = (
                typing.cast(dict[str, str], checks_end_meta).get("abort_reason") or None
            )
        else:
            restart_reason = data.get("restart_reason")

        return cls(
            train=train,
            user_pull_request_number=data["user_pull_request_number"],
            config=data["config"],
            queued_at=data["queued_at"],
            restart_reason=restart_reason,
        )

    def serialized(self) -> "EmbarkedPull.Serialized":
        return self.Serialized(
            user_pull_request_number=self.user_pull_request_number,
            config=self.config,
            queued_at=self.queued_at,
            restart_reason=self.restart_reason,
        )
