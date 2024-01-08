from unittest import mock
from urllib import parse

from first import first

from mergify_engine import context
from mergify_engine import settings
import mergify_engine.queue.merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class TestQueueWithPartitionRules(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_pr_matches_only_one_partition_with_several_partitions_and_queues(
        self,
    ) -> None:
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
                    "actions": {"queue": {}},
                },
            ],
            "queue_rules": [
                {
                    "name": "urgent",
                    "queue_conditions": [
                        "label=urgent-projA",
                        "partition-name=projA",
                        "status-success=continuous-integration/fake-ci-A",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-global-ci",
                    ],
                    "allow_inplace_checks": True,
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        {
                            "or": [
                                "partition-name!=projA",
                                "status-success=continuous-integration/fake-ci-A",
                            ],
                        },
                        {
                            "or": [
                                "partition-name!=projB",
                                "status-success=continuous-integration/fake-ci-B",
                            ],
                        },
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-global-ci",
                    ],
                    "allow_inplace_checks": True,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.add_label(p1["number"], "urgent-projA")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Automatic merge (queue)",
        )
        assert check is None

        await self.create_status(p1, "continuous-integration/fake-ci-B")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Automatic merge (queue)",
        )

        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projB` partition queue to be merged"
        )

        convoy = await self.get_convoy()
        cars = convoy.get_train_cars_by_pull(
            await self.repository_ctxt.get_pull_request_context(p1["number"]),
        )

        assert cars is not None
        assert len(cars) == 1
        assert cars[0].get_queue_name() == "default"

        await self.create_status(p1, "continuous-integration/fake-global-ci")
        await self.run_engine()

        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_pr_queued_in_multiple_partitions_and_different_queues(
        self,
    ) -> None:
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
                    "actions": {"queue": {}},
                },
            ],
            "queue_rules": [
                {
                    "name": "urgent",
                    "queue_conditions": [
                        "label=urgent-projA",
                        "partition-name=projA",
                        "status-success=continuous-integration/fake-ci-A",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-global-ci",
                    ],
                    "allow_inplace_checks": True,
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        {
                            "or": [
                                "partition-name!=projA",
                                "status-success=continuous-integration/fake-ci-A",
                            ],
                        },
                        {
                            "or": [
                                "partition-name!=projB",
                                "status-success=continuous-integration/fake-ci-B",
                            ],
                        },
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-global-ci",
                    ],
                    "allow_inplace_checks": True,
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

        await self.add_label(p1["number"], "queue")
        await self.add_label(p1["number"], "urgent-projA")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Automatic merge (queue)",
        )
        assert check is None

        await self.create_status(p1, "continuous-integration/fake-ci-A")
        await self.create_status(p1, "continuous-integration/fake-ci-B")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Automatic merge (queue)",
        )

        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 1st in projB"
        )

        convoy = await self.get_convoy()
        cars = convoy.get_train_cars_by_pull(
            await self.repository_ctxt.get_pull_request_context(p1["number"]),
        )

        assert cars is not None
        assert len(cars) == 2
        assert cars[0].get_queue_name() == "urgent"
        assert cars[1].get_queue_name() == "urgent"

        await self.create_status(p1, "continuous-integration/fake-global-ci")
        await self.run_engine()

        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_multi_partition_pull_request_reporting_when_pr_in_only_one_partition(
        self,
    ) -> None:
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
                    "allow_inplace_checks": True,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr(
            files={
                "projA/test.txt": "test",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projA` partition queue to be merged"
        )

        await self.create_status(p1, state="failure")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="failure",
        )

        assert check_run_p1["check_run"]["output"]["summary"] is not None
        assert (
            "The queue conditions cannot be satisfied due to failing checks"
            in check_run_p1["check_run"]["output"]["summary"]
        )
        assert (
            "Required conditions for merge:\n\n- [ ] `status-success=continuous-integration/fake-ci`"
            in check_run_p1["check_run"]["output"]["summary"]
        )
        assert (
            "Check-runs and statuses of the embarked pull request"
            in check_run_p1["check_run"]["output"]["summary"]
        )

    async def test_multi_partition_pull_request_reporting_when_pr_in_several_partitions(
        self,
    ) -> None:
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
                    "allow_inplace_checks": True,
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

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 1st in projB"
        )

        await self.create_status(p1, state="failure")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="failure",
        )

        assert check_run_p1["check_run"]["output"]["summary"] is not None
        assert "**Partition projA**:" in check_run_p1["check_run"]["output"]["summary"]
        assert "**Partition projB**:" in check_run_p1["check_run"]["output"]["summary"]
        assert (
            "The queue conditions cannot be satisfied due to failing checks"
            in check_run_p1["check_run"]["output"]["summary"]
        )
        assert (
            "Required conditions for merge:\n\n- [ ] `status-success=continuous-integration/fake-ci`"
            in check_run_p1["check_run"]["output"]["summary"]
        )
        assert (
            "Check-runs and statuses of the embarked pull request"
            in check_run_p1["check_run"]["output"]["summary"]
        )

    async def test_no_partition_reset_after_pr_manual_merge_have_reset_one_partition(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_iter_trains_from_partition_names = (
            mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names
        )
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names",
            side_effect=real_iter_trains_from_partition_names,
            autospec=True,
        ) as iter_train_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"projB/test.txt": "test"})
            p2 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p3 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()
            await self.merge_pull(p1["number"])
            await self.run_engine()
            p4 = await self.create_pr(files={"projA/tutu.txt": "tutu"})
            p5 = await self.create_pr(files={"projB/tutu.txt": "tutu"})
            await self.add_label(p4["number"], "queue")
            await self.add_label(p5["number"], "queue")
            await self.run_engine()
            assert reset_mock.call_count == 1
            assert reset_partitions_mock.call_count == 4
            iter_train_mock.assert_any_call(mock.ANY, ["projB"])

    async def test_all_partitions_reset_when_manual_force_push_on_base_branch_without_pr_and_with_fallback_partition(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_reset = mergify_engine.queue.merge_train.Train.reset
        with mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"toto.txt": "toto"})
            p2 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p3 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            await self.git("fetch", "origin", self.main_branch_name)
            await self.git(
                "checkout",
                "-b",
                "random",
                f"origin/{self.main_branch_name}",
            )
            (self.git.repository / "random_file.txt").open("wb").close()
            await self.git("add", "random_file.txt")
            await self.git("commit", "--no-edit", "-m", "random update")
            await self.git(
                "push",
                "-f",
                "--quiet",
                "origin",
                f"random:{self.main_branch_name}",
            )
            await self.wait_for_push(branch_name=self.main_branch_name)
            await self.run_engine()
            assert reset_mock.call_count == 3

    async def test_all_partitions_reset_when_manual_force_push_on_base_branch_without_pr_and_no_fallback_partition(
        self,
    ) -> None:
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
        real_reset = mergify_engine.queue.merge_train.Train.reset
        with mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p2 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            await self.git("fetch", "origin", self.main_branch_name)
            await self.git(
                "checkout",
                "-b",
                "random",
                f"origin/{self.main_branch_name}",
            )
            (self.git.repository / "random_file.txt").open("wb").close()
            await self.git("add", "random_file.txt")
            await self.git("commit", "--no-edit", "-m", "random update")
            await self.git(
                "push",
                "-f",
                "--quiet",
                "origin",
                f"random:{self.main_branch_name}",
            )
            await self.wait_for_push(branch_name=self.main_branch_name)
            await self.run_engine()

            assert reset_mock.call_count == 2

    async def test_all_partitions_reset_when_pr_manual_merge_with_no_fallback_partition(
        self,
    ) -> None:
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
        real_reset = mergify_engine.queue.merge_train.Train.reset
        with mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"test.txt": "test"})
            p2 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p3 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()
            await self.merge_pull(p1["number"])
            await self.run_engine()
            assert reset_mock.call_count == 2

    async def test_fallback_partition_reset_when_pr_manual_merge_dont_match_any_partitions(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_iter_trains_from_partition_names = (
            mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names
        )
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names",
            side_effect=real_iter_trains_from_partition_names,
            autospec=True,
        ) as iter_train_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"test.txt": "test"})
            p2 = await self.create_pr(files={"tutu.txt": "test"})
            p3 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p4 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.add_label(p4["number"], "queue")
            await self.run_engine()
            await self.merge_pull(p1["number"])
            await self.run_engine()
            assert reset_mock.call_count == 1
            assert reset_partitions_mock.call_count == 3
            iter_train_mock.assert_any_call(mock.ANY, ["fallback_partition"])

    async def test_partition_reset_when_pr_manual_merge_matches_one_partition(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_iter_trains_from_partition_names = (
            mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names
        )
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names",
            side_effect=real_iter_trains_from_partition_names,
            autospec=True,
        ) as iter_train_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"projB/test.txt": "test"})
            p2 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p3 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()
            await self.merge_pull(p1["number"])
            await self.run_engine()
            assert reset_mock.call_count == 1
            assert reset_partitions_mock.call_count == 2
            iter_train_mock.assert_any_call(mock.ANY, ["projB"])

    async def test_partition_reset_when_pr_manual_merge_matches_several_partitions(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_iter_trains_from_partition_names = (
            mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names
        )
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Convoy.iter_trains_from_partition_names",
            side_effect=real_iter_trains_from_partition_names,
            autospec=True,
        ) as iter_train_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(
                files={
                    "projA/test.txt": "testA",
                    "projB/test.txt": "testB",
                },
            )
            p2 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p3 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()
            await self.merge_pull(p1["number"])
            await self.run_engine()
            assert reset_mock.call_count == 2
            assert reset_partitions_mock.call_count == 2
            iter_train_mock.assert_any_call(mock.ANY, ["projA"])
            iter_train_mock.assert_any_call(mock.ANY, ["projB"])

    async def test_no_partition_reset_after_queued_merged_pr_with_partition_and_fallback_partition(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p2 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            draft_pr = await self.wait_for_pull_request("opened")
            await self.create_status(draft_pr["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", draft_pr["number"])
            p1_closed = await self.wait_for_pull_request("closed", p1["number"])
            assert p1_closed["pull_request"]["merged"]
            assert reset_partitions_mock.call_count == 0
            assert reset_mock.call_count == 0

    async def test_no_partition_reset_after_queued_merged_pr_with_partition_and_no_fallback_partition(
        self,
    ) -> None:
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
        real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition = mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition
        real_reset = mergify_engine.queue.merge_train.Train.reset

        with mock.patch(
            "mergify_engine.queue.merge_train.Train.get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition",
            side_effect=real_get_unexpected_base_branch_pushed_after_manually_merged_pr_with_fallback_partition,
            autospec=True,
        ) as reset_partitions_mock, mock.patch(
            "mergify_engine.queue.merge_train.Train.reset",
            side_effect=real_reset,
            autospec=True,
        ) as reset_mock:
            p1 = await self.create_pr(files={"projA/toto.txt": "toto"})
            p2 = await self.create_pr(files={"projB/toto.txt": "toto"})
            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.run_engine()

            draft_pr = await self.wait_for_pull_request("opened")
            await self.create_status(draft_pr["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", draft_pr["number"])
            p1_closed = await self.wait_for_pull_request("closed", p1["number"])
            assert p1_closed["pull_request"]["merged"]
            assert reset_partitions_mock.call_count == 0
            assert reset_mock.call_count == 0

    async def test_queue_fallback_partition_if_pr_dont_match_any_partitions_inplace(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
                    "allow_inplace_checks": True,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr(
            files={
                "test.txt": "test",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `fallback_partition` partition queue to be merged"
        )

        await self.create_status(p1)
        await self.run_engine()

        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_queue_fallback_partition_if_pr_dont_match_any_partitions_no_inplace(
        self,
    ) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "fallback_partition",
                    "fallback_partition": True,
                },
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
                "test.txt": "test",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `fallback_partition` partition queue to be merged"
        )

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_queue_multiple_partition_only_1_match(self) -> None:
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

        p1 = await self.create_pr(files={"projA/test.txt": "test"})

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projA` partition queue to be merged"
        )

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_queue_1_pr_multiple_partitions_match_inplace(self) -> None:
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
                    "allow_inplace_checks": True,
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

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 1st in projB"
        )

        await self.create_status(p1)
        await self.run_engine()

        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

    async def test_queue_1_pr_multiple_partitions_match_no_inplace(self) -> None:
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

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 1st in projB"
        )

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        for train in convoy.iter_trains():
            assert len(train._cars) == 0

    async def test_two_prs_in_differents_partitions_merged_no_inplace(self) -> None:
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
                "projA/test.txt": "testA",
            },
        )

        p2 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projA` partition queue to be merged"
        )
        draft_pr_1 = await self.wait_for_pull_request("opened")

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        check_run_p2 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projB` partition queue to be merged"
        )
        draft_pr_2 = await self.wait_for_pull_request("opened")

        await self.create_status(draft_pr_1["pull_request"])
        await self.run_engine()

        p_closed_nbs = [p1["number"], p2["number"]]
        await self.wait_for_pull_request("closed", draft_pr_1["number"])
        p_closed = await self.wait_for_pull_request("closed")
        p_closed_nbs.pop(p_closed_nbs.index(p_closed["number"]))
        assert p_closed["pull_request"]["merged"]

        await self.create_status(draft_pr_2["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_2["number"])
        p_closed = await self.wait_for_pull_request("closed")
        assert p_closed["number"] == p_closed_nbs[0]
        assert p_closed["pull_request"]["merged"]

    async def test_two_prs_in_differents_partitions_merged_inplace(self) -> None:
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
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
            },
        )

        p2 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            },
        )

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 2
        assert convoy._trains[0].partition_name == "projA"
        assert convoy._trains[0].find_embarked_pull(p1["number"]) != (None, None)
        assert convoy._trains[1].partition_name == "projB"
        assert convoy._trains[1].find_embarked_pull(p2["number"]) != (None, None)

        await self.create_status(p1)
        await self.create_status(p2)
        await self.run_engine()

        p_closed = [
            await self.wait_for_pull_request("closed", merged=True),
            await self.wait_for_pull_request("closed", merged=True),
        ]
        assert sorted([p["number"] for p in p_closed]) == [p1["number"], p2["number"]]

    async def test_queue_1_pr_multiple_partitions_no_inplace_fail(self) -> None:
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

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 1st in projB"
        )

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"], state="failure")
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])

    async def test_pr_queued_in_two_partitions_need_to_wait_for_other_partition_before_merge(
        self,
    ) -> None:
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
                        {
                            "or": [
                                "partition-name!=projA",
                                "status-success=continuous-integration/fake-ci-A",
                            ],
                        },
                        {
                            "or": [
                                "partition-name!=projB",
                                "status-success=continuous-integration/fake-ci-B",
                            ],
                        },
                    ],
                    "allow_inplace_checks": False,
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test2.txt": "testB2",
            },
        )

        p2 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            },
        )

        # Make sure p2 is queued before p1
        await self.add_label(p2["number"], "queue")
        await self.run_engine()
        check_run_p2 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projB` partition queue to be merged"
        )
        assert (
            check_run_p2["check_run"]["details_url"]
            == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{p2['base']['repo']['owner']['login']}/repo/{p2['base']['repo']['name']}/queues/partitions/projB?branch={parse.quote(p2['base']['ref'], safe='')}&queues=default&pull={p2['number']}"
        )
        draft_pr_p2 = await self.wait_for_pull_request("opened")

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 2nd in projB"
        )

        assert (
            check_run_p1["check_run"]["details_url"]
            == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{p2['base']['repo']['owner']['login']}/repo/{p2['base']['repo']['name']}/queues/partitions/projA?branch={parse.quote(p1['base']['ref'], safe='')}&queues=default&pull={p1['number']}"
        )

        draft_pr_p1_projA = await self.wait_for_pull_request("opened")
        await self.create_status(
            draft_pr_p1_projA["pull_request"],
            "continuous-integration/fake-ci-A",
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p1_projA["number"])

        # p1 still needs to wait for p2 in projB to be merged, then
        # create draft pr for p1 in projB then merge it

        await self.create_status(
            draft_pr_p2["pull_request"],
            "continuous-integration/fake-ci-B",
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p2["number"])
        p2_closed = await self.wait_for_pull_request("closed", p2["number"])
        assert p2_closed["pull_request"]["merged"]

        draft_pr_p1_projB = await self.wait_for_pull_request("opened")
        await self.create_status(
            draft_pr_p1_projB["pull_request"],
            "continuous-integration/fake-ci-B",
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p1_projB["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

        check_run_p1 = await self.wait_for_check_run(
            action="completed",
            name="Rule: Automatic merge (queue)",
        )
        assert (
            check_run_p1["check_run"]["details_url"]
            == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{p2['base']['repo']['owner']['login']}/repo/{p2['base']['repo']['name']}/queues?branch={parse.quote(p1['base']['ref'], safe='')}&pull={p1['number']}"
        )

    async def test_fastforward_merge_forbidden_with_partition_rules(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projA",
                    "conditions": [
                        "files~=^projA/",
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
                    "queue_branch_merge_method": "fast-forward",
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr(files={"projA/test.txt": "testA"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Automatic merge (queue)",
            conclusion="cancelled",
        )
        check = check_run["check_run"]
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "Invalid merge method with partition rules in use"
        )
        assert (
            check["output"]["summary"]
            == "Cannot use `fast-forward` merge method when using partition rules"
        )

    async def test_trains_with_no_partitions_then_chaning_conf_to_use_partitions(
        self,
    ) -> None:
        rules = {
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

        p1 = await self.create_pr(files={"projA/test.txt": "testA"})

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 1
        assert convoy._trains[0].partition_name == partr_config.DEFAULT_PARTITION_NAME
        assert len(convoy._trains[0]._cars) == 1

        # Update conf with new partition rules
        rules.update(
            {
                "partition_rules": [
                    {
                        "name": "projA",
                        "conditions": [
                            "files~=^projA/",
                        ],
                    },
                ],
            },
        )

        p2 = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.merge_pull(p2["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        # draft_pr_p1 should be closed and reopened and be part of partition
        # `projA`
        await self.wait_for_pull_request("closed", draft_pr_p1["number"])
        await self.wait_for_pull_request("opened")

        # The config was changed, need to clear the cache of the `self.repository_ctxt`
        # to get the correct partition_rules
        self.clear_repository_ctxt_caches()
        await self.reload_repository_ctxt_configuration()

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 1
        assert convoy._trains[0].partition_name == "projA"
        assert len(convoy._trains[0]._cars) == 1
