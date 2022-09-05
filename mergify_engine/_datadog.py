import typing

from datadog import statsd

from mergify_engine import github_types
from mergify_engine import signals


if typing.TYPE_CHECKING:
    from mergify_engine import context


class Signal(signals.SignalBase):
    async def __call__(
        self,
        repository: "context.Repository",
        pull_request: github_types.GitHubPullRequestNumber,
        event: signals.EventName,
        metadata: signals.EventMetadata,
        trigger: str,
    ) -> None:
        if event.startswith("action"):
            statsd.increment("engine.signals.action.count", tags=[f"event:{event[7:]}"])
