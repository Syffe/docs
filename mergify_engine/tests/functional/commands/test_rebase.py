import base64
import typing
from unittest import mock

import pytest

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandRebase(base.FunctionalTestBase):
    async def test_command_rebase_ok(self) -> None:
        await self.setup_repo(yaml.dump({}), files={"TESTING": "foobar\n"})
        p1 = await self.create_pr(files={"TESTING": "foobar\n\n\np1"}, as_="admin")
        p2 = await self.create_pr(files={"TESTING": "p2\n\nfoobar\n"}, as_="admin")
        await self.merge_pull(p1["number"])

        await self.create_comment_as_admin(p2["number"], "@mergifyio rebase")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        oldsha = p2["head"]["sha"]

        p2_merged = await self.merge_pull(p2["number"])
        assert oldsha != p2_merged["pull_request"]["head"]["sha"]

        f = typing.cast(
            github_types.GitHubContentFile,
            await self.client_integration.item(
                f"{self.url_origin}/contents/TESTING?ref={self.main_branch_name}",
            ),
        )
        data = base64.b64decode(bytearray(f["content"], "utf-8"))
        assert data == b"p2\n\nfoobar\n\n\np1"

    @pytest.mark.subscription(
        subscription.Features.MERGE_QUEUE,
        subscription.Features.WORKFLOW_AUTOMATION,
    )
    async def test_rebase_pr_in_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("opened")

        await self.create_comment_as_admin(p["number"], "@mergifyio rebase")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert (
            "It's not possible to rebase this pull request because it is queued for merge"
            in comment["comment"]["body"]
        )

    async def test_command_rebase_permission_error(self) -> None:
        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "rebase": {"conditions": ["sender-permission>=read"]},
                    },
                },
            ),
            files={"TESTING": "foobar\n"},
        )
        p1 = await self.create_pr(files={"TESTING": "foobar\n\n\np1"}, as_="admin")
        p2 = await self.create_pr(files={"TESTING": "p2\n\nfoobar\n"}, as_="admin")

        await self.merge_pull(p1["number"])

        # The repo is a fork with "maintainer_can_modify" authorization, but the
        # user is not a maintainer of the fork
        with mock.patch.object(context.Context, "pull_from_fork", True), mock.patch(
            "mergify_engine.branch_updater.pre_rebase_check",
            return_value=True,
        ):
            await self.create_comment_as_fork(p2["number"], "@mergifyio rebase")
            await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p2["number"]), "created")
        assert (
            "`mergify-test2` does not have write access to the forked repository."
            in comment["comment"]["body"]
        )
