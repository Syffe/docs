import datetime

import anys

from mergify_engine import date
from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils
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
                },
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
        assert r.json()["detail"] == "The partition `partition_test` does not exist."

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
                        },
                    ],
                },
            },
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
                },
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
            files={"projA/test2.txt": "testA", "projB/test2.txt": "testB"},
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
                        },
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
                        },
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
                },
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
                },
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
                            },
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

            # projB eta should be at least 1 hour more than projA
            assert datetime.datetime.fromisoformat(
                rprojectB.json()["pull_requests"][0]["estimated_time_of_merge"],
            ) > datetime.datetime.fromisoformat(
                rprojectA.json()["pull_requests"][0]["estimated_time_of_merge"],
            ) + datetime.timedelta(hours=1)

    async def test_estimated_time_of_merge_without_partitions_and_pr_in_multiple_queues(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "batch_size": 2,
                    "allow_inplace_checks": False,
                },
                {
                    "name": "lowprio",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "batch_size": 3,
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queuedefault",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queuedefault",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "queuelowprio",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queuelowprio",
                    ],
                    "actions": {"queue": {"name": "lowprio"}},
                },
            ],
        }

        start_date = datetime.datetime(2023, 11, 10, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr()
            p2 = await self.create_pr()

            p3 = await self.create_pr()
            await self.merge_pull_as_admin(p3["number"])

            await self.add_label(p1["number"], "queuedefault")
            await self.add_label(p2["number"], "queuedefault")
            await self.run_engine()

            tmp_pr_queue_default = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            # Create an ETA of ~1hour for queue default
            await self.create_status(tmp_pr_queue_default["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_pr_queue_default["number"])
            await self.wait_for_all(
                [
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p1["number"],
                            merged=True,
                        ),
                    },
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p2["number"],
                            merged=True,
                        ),
                    },
                ],
            )

        with time_travel(start_date + datetime.timedelta(hours=2), tick=True):
            p1_low = await self.create_pr()
            p2_low = await self.create_pr()
            p3_low = await self.create_pr()
            await self.add_label(p1_low["number"], "queuelowprio")
            await self.add_label(p2_low["number"], "queuelowprio")
            await self.add_label(p3_low["number"], "queuelowprio")
            await self.run_engine()

            tmp_pr_queue_lowprio = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            # Create an ETA of ~1hour for queue lowprio
            await self.create_status(tmp_pr_queue_lowprio["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_pr_queue_lowprio["number"])
            await self.wait_for_all(
                [
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p1_low["number"],
                            merged=True,
                        ),
                    },
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p2_low["number"],
                            merged=True,
                        ),
                    },
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p3_low["number"],
                            merged=True,
                        ),
                    },
                ],
            )

            p4 = await self.create_pr()
            p5 = await self.create_pr()
            p6 = await self.create_pr()
            p7 = await self.create_pr()
            p8 = await self.create_pr()

            await self.add_label(p4["number"], "queuedefault")
            await self.add_label(p5["number"], "queuedefault")
            await self.add_label(p6["number"], "queuelowprio")
            await self.add_label(p7["number"], "queuelowprio")
            await self.add_label(p8["number"], "queuelowprio")
            await self.run_engine()

            tmp_pr_queue_default = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=5), tick=True):
            # ETA should be 1 hour late
            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
            )
            assert r.status_code == 200
            assert partr_config.DEFAULT_PARTITION_NAME in r.json()[0]["partitions"]
            assert (
                len(r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME]) == 5
            )

            assert (
                start_date + datetime.timedelta(hours=4)
                < datetime.datetime.fromisoformat(
                    r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][0][
                        "estimated_time_of_merge"
                    ],
                )
                < start_date + datetime.timedelta(hours=4, minutes=10)
            )

            assert (
                start_date + datetime.timedelta(hours=4)
                < datetime.datetime.fromisoformat(
                    r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][1][
                        "estimated_time_of_merge"
                    ],
                )
                < start_date + datetime.timedelta(hours=4, minutes=10)
            )

            eta_p6 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][2][
                "estimated_time_of_merge"
            ]
            eta_p7 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][3][
                "estimated_time_of_merge"
            ]
            eta_p8 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][4][
                "estimated_time_of_merge"
            ]
            assert eta_p6 is not None
            assert eta_p7 is not None
            assert eta_p8 is not None
            assert eta_p6 == eta_p7 == eta_p8

            # ETA is around ~1h
            assert (
                date.utcnow() + datetime.timedelta(minutes=55)
                < datetime.datetime.fromisoformat(eta_p6)
                <= date.utcnow() + datetime.timedelta(hours=1, minutes=5)
            )

    async def test_estimated_time_of_merge_with_1_speculative_check_and_multiple_batch(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "batch_size": 3,
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queuedefault",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queuedefault",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        start_date = datetime.datetime(2023, 11, 10, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr()
            p2 = await self.create_pr()
            p3 = await self.create_pr()

            p4 = await self.create_pr()
            await self.merge_pull_as_admin(p4["number"])

            await self.add_label(p1["number"], "queuedefault")
            await self.add_label(p2["number"], "queuedefault")
            await self.add_label(p3["number"], "queuedefault")
            await self.run_engine()

            tmp_pr_queue_default = await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=1), tick=True):
            # Create an ETA of ~1hour for queue default
            await self.create_status(tmp_pr_queue_default["pull_request"])
            await self.run_engine()

            await self.wait_for_pull_request("closed", tmp_pr_queue_default["number"])
            await self.wait_for_all(
                [
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p1["number"],
                            merged=True,
                        ),
                    },
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p2["number"],
                            merged=True,
                        ),
                    },
                    {
                        "event_type": "pull_request",
                        "payload": tests_utils.get_pull_request_event_payload(
                            pr_number=p3["number"],
                            merged=True,
                        ),
                    },
                ],
            )

            p5 = await self.create_pr()
            p6 = await self.create_pr()
            p7 = await self.create_pr()

            await self.add_label(p5["number"], "queuedefault")
            await self.add_label(p6["number"], "queuedefault")
            await self.run_engine({"delayed-refresh"})

        with time_travel(
            start_date + datetime.timedelta(hours=1, minutes=10),
            tick=True,
        ):
            # Travel a few minutes after the run_engine so the batch max wait time is elapsed and
            # the draft PR is created with only 2 PR out of the 3 possible in the batch
            await self.run_engine({"delayed-refresh"})
            tmp_pr_queue_default = await self.wait_for_pull_request("opened")

            await self.add_label(p7["number"], "queuedefault")
            await self.run_engine()

            r = await self.admin_app.get(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/partitions",
            )
            assert r.status_code == 200
            assert partr_config.DEFAULT_PARTITION_NAME in r.json()[0]["partitions"]
            assert (
                len(r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME]) == 3
            )

            eta_p5 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][0][
                "estimated_time_of_merge"
            ]
            eta_p6 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][1][
                "estimated_time_of_merge"
            ]
            eta_p7 = r.json()[0]["partitions"][partr_config.DEFAULT_PARTITION_NAME][2][
                "estimated_time_of_merge"
            ]

            assert eta_p5 == eta_p6
            assert eta_p7 != eta_p5
            # ETA of p5 and p6 is in ~1 hour, so eta of p7 should be in ~2hours
            assert (
                datetime.datetime.fromisoformat(eta_p7)
                > datetime.datetime.fromisoformat(eta_p5)
                + datetime.timedelta(minutes=55)
                > date.utcnow() + datetime.timedelta(minutes=55)
            )
