from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestQueueWithPartitionRules(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"projA/test.txt": "test"})

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test.txt": "testB",
            }
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test.txt": "testB",
            }
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
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

    async def test_two_prs_in_differents_partitions_merged(self) -> None:
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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
            }
        )

        p2 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            }
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projA` partition queue to be merged"
        )
        draft_pr_1 = await self.wait_for_pull_request("opened")

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        check_run_p2 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test.txt": "testB",
            }
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
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
                                {
                                    "and": [
                                        "queue-partition-name=projA",
                                        "status-success=continuous-integration/fake-ci-A",
                                    ]
                                },
                                {
                                    "and": [
                                        "queue-partition-name=projB",
                                        "status-success=continuous-integration/fake-ci-B",
                                    ]
                                },
                            ],
                        },
                    ],
                    "allow_inplace_checks": False,
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(
            files={
                "projA/test.txt": "testA",
                "projB/test2.txt": "testB2",
            }
        )

        p2 = await self.create_pr(
            files={
                "projB/test.txt": "testB",
            }
        )

        # Make sure p2 is queued before p1
        await self.add_label(p2["number"], "queue")
        await self.run_engine()
        check_run_p2 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
        )
        assert (
            check_run_p2["check_run"]["output"]["title"]
            == "The pull request is the 1st in the `projB` partition queue to be merged"
        )
        draft_pr_p2 = await self.wait_for_pull_request("opened")

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run_p1 = await self.wait_for_check_run(
            action="created", name="Rule: Automatic merge (queue)"
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "The pull request is queued in the following partitions to be merged: 1st in projA, 2nd in projB"
        )

        draft_pr_p1_projA = await self.wait_for_pull_request("opened")
        await self.create_status(
            draft_pr_p1_projA["pull_request"], "continuous-integration/fake-ci-A"
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p1_projA["number"])

        # p1 still needs to wait for p2 in projB to be merged, then
        # create draft pr for p1 in projB then merge it

        await self.create_status(
            draft_pr_p2["pull_request"], "continuous-integration/fake-ci-B"
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p2["number"])
        p2_closed = await self.wait_for_pull_request("closed", p2["number"])
        assert p2_closed["pull_request"]["merged"]

        draft_pr_p1_projB = await self.wait_for_pull_request("opened")
        await self.create_status(
            draft_pr_p1_projB["pull_request"], "continuous-integration/fake-ci-B"
        )
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p1_projB["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr(files={"projA/test.txt": "testA"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Automatic merge (queue)",
            conclusion="failure",
        )
        check = check_run["check_run"]
        assert check["conclusion"] == "failure"
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
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"projA/test.txt": "testA"})

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 1
        assert convoy._trains[0].partition_name is None
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
            }
        )

        p2 = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.merge_pull(p2["number"])
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        # draft_pr_p1 should be closed and reopened and be part of partition
        # `projA`
        await self.wait_for_pull_request("closed", draft_pr_p1["number"])
        await self.wait_for_pull_request("opened")

        # The config was changed, need to clear the cache of the `self.repository_ctxt`
        # to get the correct partition_rules
        self.clear_repository_ctxt_caches()

        convoy = await self.get_convoy()
        assert len(convoy._trains) == 1
        assert convoy._trains[0].partition_name == "projA"
        assert len(convoy._trains[0]._cars) == 1
