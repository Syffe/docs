import anys

from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base


class TestPartitionsApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_partitions_endpoints_without_any_partition_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "foo",
                    "conditions": [
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
                    "conditions": [
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
                    "conditions": [
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
