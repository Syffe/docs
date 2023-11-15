import typing

from mergify_engine import github_types
from mergify_engine.tests.functional.commands import test_dequeue


class TestUnqueueCommand(test_dequeue.TestDequeueCommand):
    async def create_comment(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        message: str,
        as_: typing.Literal["integration", "fork", "admin"] = "integration",
    ) -> int:
        return await super().create_comment(
            pull_number,
            message.replace("dequeue", "unqueue"),
            as_,
        )
