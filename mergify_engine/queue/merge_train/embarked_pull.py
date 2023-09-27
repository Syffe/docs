import dataclasses
import datetime
import typing

from mergify_engine import github_types
from mergify_engine import queue


if typing.TYPE_CHECKING:
    from mergify_engine import signals
    from mergify_engine.queue.merge_train.train import Train


class QueueChecksEndMetadata(typing.TypedDict, total=False):
    aborted: bool
    abort_status: typing.Literal["DEFINITIVE", "REEMBARKED"]
    abort_reason: str


@dataclasses.dataclass
class EmbarkedPull:
    train: "Train" = dataclasses.field(repr=False)
    user_pull_request_number: github_types.GitHubPullRequestNumber
    config: queue.PullQueueConfig
    queued_at: datetime.datetime
    checks_end_metadata: QueueChecksEndMetadata = dataclasses.field(
        default_factory=lambda: QueueChecksEndMetadata()
    )

    class Serialized(typing.TypedDict):
        user_pull_request_number: github_types.GitHubPullRequestNumber
        config: queue.PullQueueConfig
        queued_at: datetime.datetime
        checks_end_metadata: QueueChecksEndMetadata

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
        if isinstance(data, tuple | list):
            user_pull_request_number = data[0]
            config = data[1]
            queued_at = data[2]

            return cls(
                train=train,
                user_pull_request_number=user_pull_request_number,
                config=config,
                queued_at=queued_at,
            )

        checks_end_metadata = QueueChecksEndMetadata(
            **data.setdefault("checks_end_metadata", {})
        )

        return cls(
            train=train,
            user_pull_request_number=data["user_pull_request_number"],
            config=data["config"],
            queued_at=data["queued_at"],
            checks_end_metadata=checks_end_metadata,
        )

    def serialized(self) -> "EmbarkedPull.Serialized":
        return self.Serialized(
            user_pull_request_number=self.user_pull_request_number,
            config=self.config,
            queued_at=self.queued_at,
            checks_end_metadata=self.checks_end_metadata,
        )

    def associate_queue_checks_end_metadata(
        self, metadata: "signals.EventQueueChecksEndMetadata"
    ) -> None:
        self.checks_end_metadata = QueueChecksEndMetadata(
            aborted=metadata["aborted"],
            abort_reason=metadata["abort_reason"] or "",
            abort_status=metadata["abort_status"],  # type: ignore[typeddict-item]
        )
