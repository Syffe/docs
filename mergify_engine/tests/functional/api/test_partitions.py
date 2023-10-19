import datetime

import anys

from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.tests.tardis import time_travel


class TestPartitionsApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_partitions_endpoints_without_any_partition_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), forward_to_engine=True)

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
        )
        assert r.status_code == 200
        assert r.json() == []

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "branch_name": self.main_branch_name,
            "partitions": {partr_config.DEFAULT_PARTITION_NAME: []},
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/partition_test/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 404
        assert r.json()["detail"] == "Partition `partition_test` does not exist"

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        expected_output = [
            {
                "branch_name": self.main_branch_name,
                "partitions": {
                    partr_config.DEFAULT_PARTITION_NAME: [
                        {
                            "number": p1["number"],
                            "position": 0,
                            "priority": anys.ANY_INT,
                            "effective_priority": anys.ANY_INT,
                            "queue_rule": {
                                "name": "foo",
                                "config": anys.ANY_MAPPING,
                            },
                            "queued_at": anys.ANY_DATETIME_STR,
                            "mergeability_check": {
                                "check_type": "draft_pr",
                                "pull_request_number": draft_pr_p1["number"],
                                "started_at": anys.ANY_DATETIME_STR,
                                "ended_at": None,
                                "state": "pending",
                            },
                            "estimated_time_of_merge": None,
                        }
                    ],
                },
            }
        ]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
        )
        assert r.status_code == 200
        assert r.json() == expected_output

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 200
        assert r.json() == expected_output[0]

    async def test_partitions_endpoints_with_partition_rules(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projectA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projectB",
                    "conditions": [
                        "files~=^projB/",
                    ],
                },
            ],
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), forward_to_engine=True)

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
        )
        assert r.status_code == 200
        assert r.json() == []

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 200
        assert r.json() == {
            "branch_name": self.main_branch_name,
            "partitions": {"projectA": [], "projectB": []},
        }

        p1 = await self.create_pr(files={"projA/test1.txt": "testA"})
        p2 = await self.create_pr(
            files={"projA/test2.txt": "testA", "projB/test2.txt": "testB"}
        )

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr_p1 = await self.wait_for_pull_request("opened")

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
        )
        r2 = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
        )
        rprojectA = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectA/branch/{self.escaped_main_branch_name}",
        )
        rprojectB = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectB/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 200
        assert r2.status_code == 200
        assert rprojectA.status_code == 200
        assert rprojectB.status_code == 200
        expected_output = [
            {
                "branch_name": self.main_branch_name,
                "partitions": {
                    "projectA": [
                        {
                            "number": p1["number"],
                            "position": 0,
                            "priority": anys.ANY_INT,
                            "effective_priority": anys.ANY_INT,
                            "queue_rule": {
                                "name": "foo",
                                "config": anys.ANY_MAPPING,
                            },
                            "queued_at": anys.ANY_DATETIME_STR,
                            "mergeability_check": {
                                "check_type": "draft_pr",
                                "pull_request_number": draft_pr_p1["number"],
                                "started_at": anys.ANY_DATETIME_STR,
                                "ended_at": None,
                                "state": "pending",
                            },
                            "estimated_time_of_merge": None,
                        }
                    ],
                    "projectB": [],
                },
            },
        ]
        assert r.json() == expected_output
        assert r2.json() == expected_output[0]

        assert rprojectA.json() == {
            "pull_requests": expected_output[0]["partitions"]["projectA"],  # type: ignore[index]
        }
        assert rprojectB.json() == {
            "pull_requests": expected_output[0]["partitions"]["projectB"],  # type: ignore[index]
        }

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        draft_pr_p2_projB = await self.wait_for_pull_request("opened")
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
        )
        r2 = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
        )
        rprojectA = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectA/branch/{self.escaped_main_branch_name}",
        )
        rprojectB = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectB/branch/{self.escaped_main_branch_name}",
        )
        assert r.status_code == 200
        assert r2.status_code == 200
        assert rprojectA.status_code == 200
        assert rprojectB.status_code == 200
        expected_output = [
            {
                "branch_name": self.main_branch_name,
                "partitions": {
                    "projectA": [
                        {
                            "number": p1["number"],
                            "position": 0,
                            "priority": anys.ANY_INT,
                            "effective_priority": anys.ANY_INT,
                            "queue_rule": {
                                "name": "foo",
                                "config": anys.ANY_MAPPING,
                            },
                            "queued_at": anys.ANY_DATETIME_STR,
                            "mergeability_check": {
                                "check_type": "draft_pr",
                                "pull_request_number": draft_pr_p1["number"],
                                "started_at": anys.ANY_DATETIME_STR,
                                "ended_at": None,
                                "state": "pending",
                            },
                            "estimated_time_of_merge": None,
                        },
                        {
                            "number": p2["number"],
                            "position": 1,
                            "priority": anys.ANY_INT,
                            "effective_priority": anys.ANY_INT,
                            "queue_rule": {
                                "name": "foo",
                                "config": anys.ANY_MAPPING,
                            },
                            "queued_at": anys.ANY_DATETIME_STR,
                            "mergeability_check": None,
                            "estimated_time_of_merge": None,
                        },
                    ],
                    "projectB": [
                        {
                            "number": p2["number"],
                            "position": 0,
                            "priority": anys.ANY_INT,
                            "effective_priority": anys.ANY_INT,
                            "queue_rule": {
                                "name": "foo",
                                "config": anys.ANY_MAPPING,
                            },
                            "queued_at": anys.ANY_DATETIME_STR,
                            "mergeability_check": {
                                "check_type": "draft_pr",
                                "pull_request_number": draft_pr_p2_projB["number"],
                                "started_at": anys.ANY_DATETIME_STR,
                                "ended_at": None,
                                "state": "pending",
                            },
                            "estimated_time_of_merge": None,
                        }
                    ],
                },
            },
        ]

        assert r.json() == expected_output
        assert r2.json() == expected_output[0]
        assert rprojectA.json() == {
            "pull_requests": expected_output[0]["partitions"]["projectA"],  # type: ignore[index]
        }
        assert rprojectB.json() == {
            "pull_requests": expected_output[0]["partitions"]["projectB"],  # type: ignore[index]
        }

    async def test_partitions_endpoints_invalid_branch_name(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projectA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projectB",
                    "conditions": [
                        "files~=^projB/",
                    ],
                },
            ],
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), forward_to_engine=True)

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/unknown",
        )
        assert r.status_code == 200
        assert r.json() == {
            "branch_name": "unknown",
            "partitions": {"projectA": [], "projectB": []},
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectA/branch/unknown",
        )
        assert r.status_code == 200
        assert r.json() == {"pull_requests": []}

    async def test_estimated_time_of_merge_normal_partitions(self) -> None:
        rules = {
            "partition_rules": [
                {
                    "name": "projectA",
                    "conditions": [
                        "files~=^projA/",
                    ],
                },
                {
                    "name": "projectB",
                    "conditions": [
                        "files~=^projB/",
                    ],
                },
            ],
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }

        start_date = datetime.datetime(2022, 1, 5, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1_a = await self.create_pr(files={"projA/test1.txt": "test1"})
            p2_a = await self.create_pr(files={"projA/test2.txt": "test2"})
            p3_a = await self.create_pr(files={"projA/test3.txt": "test3"})
            p1_b = await self.create_pr(files={"projB/test1.txt": "test1"})
            p2_b = await self.create_pr(files={"projB/test2.txt": "test2"})
            p3_b = await self.create_pr(files={"projB/test3.txt": "test3"})

            await self.add_label(p1_a["number"], "queue")
            await self.add_label(p1_b["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_1 = await self.wait_for_pull_request("opened")
            tmp_mq_pr_2 = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            # Both p1 on projA and projB are closed 1 hour after being queued
            await self.create_status(tmp_mq_pr_1["pull_request"])
            await self.create_status(tmp_mq_pr_2["pull_request"])
            await self.run_engine()

            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p2_a["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2_a = await self.wait_for_pull_request("opened")

            await self.add_label(p2_b["number"], "queue")
            await self.run_engine()

            tmp_mq_pr_2_b = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            # p2 on projA is closed 2 hours after being queued
            await self.create_status(tmp_mq_pr_2_a["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

        with time_travel(start_date + datetime.timedelta(hours=5), tick=True):
            # p2 on projB is closed 4 hours after being queued
            await self.create_status(tmp_mq_pr_2_b["pull_request"])
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})
            await self.wait_for("pull_request", {"action": "closed"})

            await self.add_label(p3_a["number"], "queue")
            await self.run_engine()
            tmp_mq_pr_3_a = await self.wait_for_pull_request("opened")

            await self.add_label(p3_b["number"], "queue")
            await self.run_engine()
            tmp_mq_pr_3_b = await self.wait_for_pull_request("opened")

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
            )
            r2 = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions/branch/{self.escaped_main_branch_name}",
            )
            rprojectA = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectA/branch/{self.escaped_main_branch_name}",
            )
            rprojectB = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partition/projectB/branch/{self.escaped_main_branch_name}",
            )
            assert r.status_code == 200
            assert r2.status_code == 200
            assert rprojectA.status_code == 200
            assert rprojectB.status_code == 200
            expected_output = [
                {
                    "branch_name": self.main_branch_name,
                    "partitions": {
                        "projectA": [
                            {
                                "number": p3_a["number"],
                                "position": 0,
                                "priority": anys.ANY_INT,
                                "effective_priority": anys.ANY_INT,
                                "queue_rule": {
                                    "name": "foo",
                                    "config": anys.ANY_MAPPING,
                                },
                                "queued_at": anys.ANY_DATETIME_STR,
                                "mergeability_check": {
                                    "check_type": "draft_pr",
                                    "pull_request_number": tmp_mq_pr_3_a["number"],
                                    "started_at": anys.ANY_DATETIME_STR,
                                    "ended_at": None,
                                    "state": "pending",
                                },
                                "estimated_time_of_merge": anys.ANY_DATETIME_STR,
                            },
                        ],
                        "projectB": [
                            {
                                "number": p3_b["number"],
                                "position": 0,
                                "priority": anys.ANY_INT,
                                "effective_priority": anys.ANY_INT,
                                "queue_rule": {
                                    "name": "foo",
                                    "config": anys.ANY_MAPPING,
                                },
                                "queued_at": anys.ANY_DATETIME_STR,
                                "mergeability_check": {
                                    "check_type": "draft_pr",
                                    "pull_request_number": tmp_mq_pr_3_b["number"],
                                    "started_at": anys.ANY_DATETIME_STR,
                                    "ended_at": None,
                                    "state": "pending",
                                },
                                "estimated_time_of_merge": anys.ANY_DATETIME_STR,
                            }
                        ],
                    },
                },
            ]

            assert r.json() == expected_output
            assert r2.json() == expected_output[0]
            assert rprojectA.json() == {
                "pull_requests": expected_output[0]["partitions"]["projectA"]  # type: ignore[index]
            }
            assert rprojectB.json() == {
                "pull_requests": expected_output[0]["partitions"]["projectB"]  # type: ignore[index]
            }

            # projB eta should be at least 1 hour more than projA
            assert datetime.datetime.fromisoformat(
                rprojectB.json()["pull_requests"][0]["estimated_time_of_merge"]
            ) > datetime.datetime.fromisoformat(
                rprojectA.json()["pull_requests"][0]["estimated_time_of_merge"]
            ) + datetime.timedelta(
                hours=1
            )
