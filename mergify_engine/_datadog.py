import typing

from datadog import statsd  # type: ignore[attr-defined]

from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import subscription


if typing.TYPE_CHECKING:
    from mergify_engine import context


class Signal(signals.SignalBase):
    async def __call__(
        self,
        repository: "context.Repository",
        _pull_request: github_types.GitHubPullRequestNumber | None,
        _base_ref: github_types.GitHubRefType | None,
        event: signals.EventName,
        _metadata: signals.EventMetadata,
        _trigger: str,
    ) -> None:
        if event.startswith("action"):
            tags = [
                f"event:{event.partition('action.')[-1]}",
            ]
            sub = repository.installation.subscription.has_feature(
                subscription.Features.PRIVATE_REPOSITORY,
            )
            # NOTE(Syffe): We detect non-Open Source users in order to
            # not overload the quantity of metrics sent to Datadog
            if sub:
                tags.append(
                    f"github_login:{repository.installation.owner_login}",
                )

            statsd.increment(
                "engine.signals.action.count",
                tags=tags,
            )
