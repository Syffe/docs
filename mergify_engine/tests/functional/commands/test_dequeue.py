from first import first

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import utils
from mergify_engine import yaml
from mergify_engine.engine import commands_runner
from mergify_engine.queue import merge_train
from mergify_engine.tests.functional import base


class TestDequeueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_dequeue_no_partitions(self) -> None:
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

        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
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

    async def test_dequeue_pr_in_1_partition(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projB",
                    "conditions": [
                        "files~=^projB/",
                    ],
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
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")
        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")

        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )

        await self.wait_for_pull_request("closed", draft_pr_p1["number"])
        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        for train in convoy.iter_trains():
            assert len(train._cars) == 0

    async def test_dequeue_pr_in_2_partitions(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projB",
                    "conditions": [
                        "files~=^projB/",
                    ],
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
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test.txt": "testB",
            },
        )
        p2 = await self.create_pr(
            files={
                "projB/test2.txt": "testB2",
            },
        )

        await self.add_label(p2["number"], "queue")
        await self.run_engine()
        await self.wait_for_pull_request("opened")

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1_projA = await self.wait_for_pull_request("opened")
        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")

        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=p1["number"],
        )

        await self.wait_for_pull_request("closed", draft_pr_p1_projA["number"])

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        for train in convoy.iter_trains():
            if train.partition_name == "projA":
                assert len(train._cars) == 0
            else:
                assert len(train._cars) == 1

    async def test_dequeue_with_partition_rules(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [
                        "files~=projA/",
                    ],
                },
                {
                    "name": "projB",
                    "conditions": [
                        "files~=projB/",
                    ],
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
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test1.txt": "testA",
            },
        )
        p2 = await self.create_pr(
            files={
                "projA/test2.txt": "testA",
                "projB/test2.txt": "testB",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        # Test unqueue pr in only 1 partition
        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p1["number"])
        await self.wait_for_issue_comment(str(p1["number"]), "created")

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        assert len(convoy._trains[0]._cars) == 0
        assert len(convoy._trains[1]._cars) == 0

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        draft_pr_p2 = await self.wait_for_pull_request("opened")

        # Test unqueue pr in 2 partitions
        await self.create_comment_as_admin(p2["number"], "@mergifyio dequeue")
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p2["number"])
        await self.wait_for_issue_comment(str(p2["number"]), "created")

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        assert len(convoy._trains[0]._cars) == 0
        assert len(convoy._trains[1]._cars) == 0

    async def test_dequeue_command_on_pr_waiting_for_queue_match(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": [
                        "label=default",
                    ],
                },
                {
                    "name": "specialq",
                    "routing_conditions": [
                        "label=special",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p1["number"]), "created")
        assert """#### 🟠 Waiting for conditions to match

<details>

- [ ] any of: [🔀 queue conditions]
  - [ ] all of: [📌 queue conditions of queue `default`]
    - [ ] `label=default`
  - [ ] all of: [📌 queue conditions of queue `specialq`]
    - [ ] `label=special`
- [X] `-draft` [📌 queue requirement]
- [X] `-mergify-configuration-changed` [📌 queue -> allow_merging_configuration_change setting requirement]

</details>""" in comment["comment"]["body"]

        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
        await self.run_engine()

        edited_comment = await self.wait_for_issue_comment(str(p1["number"]), "edited")
        assert (
            "This `queue` command has been cancelled by a `dequeue` command"
            in edited_comment["comment"]["body"]
        )
        assert '"conclusion": "cancelled"' in edited_comment["comment"]["body"]

        unqueue_comment = await self.wait_for_issue_comment(
            str(p1["number"]),
            "created",
        )
        assert (
            "#### ✅ The pull request is not waiting to be queued anymore."
            in unqueue_comment["comment"]["body"]
        )

        # Same test as above but with a queue name as parameter of the command
        p2 = await self.create_pr()
        await self.create_comment_as_admin(p2["number"], "@mergifyio queue default")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p2["number"]), "created")
        assert """#### 🟠 Waiting for conditions to match

<details>

- [ ] any of: [🔀 queue conditions]
  - [ ] all of: [📌 queue conditions of queue `default`]
    - [ ] `label=default`
- [X] `-draft` [📌 queue requirement]
- [X] `-mergify-configuration-changed` [📌 queue -> allow_merging_configuration_change setting requirement]

</details>""" in comment["comment"]["body"]

        await self.create_comment_as_admin(p2["number"], "@mergifyio dequeue")
        await self.run_engine()

        edited_comment = await self.wait_for_issue_comment(str(p2["number"]), "edited")
        assert (
            "This `queue` command has been cancelled by a `dequeue` command"
            in edited_comment["comment"]["body"]
        )
        assert '"conclusion": "cancelled"' in edited_comment["comment"]["body"]

        unqueue_comment = await self.wait_for_issue_comment(
            str(p2["number"]),
            "created",
        )
        assert (
            "#### ✅ The pull request is not waiting to be queued anymore."
            in unqueue_comment["comment"]["body"]
        )
