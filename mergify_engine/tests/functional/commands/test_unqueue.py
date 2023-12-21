from first import first

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import utils
from mergify_engine import yaml
from mergify_engine.engine import commands_runner
from mergify_engine.queue import merge_train
from mergify_engine.tests.functional import base


class TestUnqueueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_unqueue_compat(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        await self.run_engine()
        q = await self.get_train()
        base_sha = await q.get_base_sha()
        await self.assert_merge_queue_contents(
            q,
            base_sha,
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    base_sha,
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio requeue")
        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )

        comments = await self.get_issue_comments(p1["number"])
        assert (
            comments[-1]["body"]
            == f"""> requeue

#### ☑️ This pull request is already queued



{utils.serialize_hidden_payload(commands_runner.CommandPayload({"command": "requeue", "conclusion": "neutral", "action_is_running": True}))}"""
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio unqueue")
        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )

        await self.assert_merge_queue_contents(q, None, [])

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert check["output"]["title"] is not None
        assert check["output"]["summary"] is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )
        assert check["output"]["summary"].startswith(
            f"Pull request #{p1['number']} has been dequeued by a `dequeue` command",
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == f"Pull request #{p1['number']} has been dequeued"
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio requeue")
        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert check["conclusion"] is None
        assert check["output"]["title"] is not None
        assert check["output"]["title"].startswith("The pull request is embarked with")

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        await self.create_status(p1)
        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]
