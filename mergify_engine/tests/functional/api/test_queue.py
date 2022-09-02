# -*- encoding: utf-8 -*-
#
# Copyright © 2022 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import datetime
import statistics

import msgpack
import yaml

from mergify_engine import config
from mergify_engine import constants
from mergify_engine import github_types
from mergify_engine.tests.functional import base


class TestQueueApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_invalid_rules_in_config(self) -> None:
        invalid_rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "batch_max_wait_time": "15 s",
                    "speculative_checks": 1,
                    "false_condition": 3,
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "false_condition": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
                {
                    "name": "low-priority",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "checks_timeout": "10 m",
                    "false_condition": "mergify-test4",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"false_action": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"false_action": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"false_action": {"name": "low-priority"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(invalid_rules))

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 422
        assert r.json() == {"detail": "The configuration file is invalid."}

    async def test_get_queues_config_list(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "batch_max_wait_time": "15 s",
                    "speculative_checks": 1,
                    "batch_size": 3,
                    "queue_branch_prefix": "urgent-",
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "speculative_checks": 2,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                },
                {
                    "name": "low-priority",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "checks_timeout": "10 m",
                    "draft_bot_account": "mergify-test4",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"queue": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"queue": {"name": "low-priority"}},
                },
            ],
        }

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 200
        assert r.json() == {
            "configuration": [
                {
                    "name": "default",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": True,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 1,
                        "batch_max_wait_time": 30.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "speculative_checks": 1,
                        "priority": 0,
                        "queue_branch_prefix": "mergify/merge-queue/",
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues/configuration",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert r.status_code == 200
        assert r.json() == {
            "configuration": [
                {
                    "name": "urgent",
                    "config": {
                        "allow_inplace_checks": True,
                        "allow_queue_branch_edit": False,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 3,
                        "batch_max_wait_time": 15.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "queue_branch_prefix": "urgent-",
                        "priority": 2,
                        "speculative_checks": 1,
                    },
                },
                {
                    "name": "default",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": True,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 2,
                        "batch_max_wait_time": 0.0,
                        "checks_timeout": None,
                        "draft_bot_account": None,
                        "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                        "priority": 1,
                        "speculative_checks": 2,
                    },
                },
                {
                    "name": "low-priority",
                    "config": {
                        "allow_queue_branch_edit": False,
                        "allow_inplace_checks": False,
                        "disallow_checks_interruption_from_queues": [],
                        "batch_size": 1,
                        "batch_max_wait_time": 30.0,
                        "checks_timeout": 600.0,
                        "draft_bot_account": "mergify-test4",
                        "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                        "priority": 0,
                        "speculative_checks": 1,
                    },
                },
            ]
        }

    async def test_estimated_time_of_merge(self) -> None:
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

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        p4 = await self.create_pr()

        p5 = await self.create_pr()
        await self.merge_pull(p5["number"])

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        await self.create_status(
            await self.get_pull(github_types.GitHubPullRequestNumber(p5["number"] + 1))
        )
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.create_status(
            await self.get_pull(github_types.GitHubPullRequestNumber(p5["number"] + 2))
        )
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.create_status(
            await self.get_pull(github_types.GitHubPullRequestNumber(p5["number"] + 3))
        )
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})

        time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 3

        items = await self.redis_links.stats.xrange(time_to_merge_key, "-", "+")
        stats = [
            msgpack.unpackb(v[b"data"], timestamp=3)["time_seconds"] for _, v in items
        ]
        avg_ttm = statistics.fmean(stats)

        await self.add_label(p4["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )

        assert len(r.json()["queues"]) == 1
        assert len(r.json()["queues"][0]["pull_requests"]) == 1
        assert "estimated_time_of_merge" in r.json()["queues"][0]["pull_requests"][0]

        queued_at = datetime.datetime.fromisoformat(
            r.json()["queues"][0]["pull_requests"][0]["queued_at"]
        )
        assert (
            r.json()["queues"][0]["pull_requests"][0]["estimated_time_of_merge"]
            == (queued_at + datetime.timedelta(seconds=avg_ttm)).isoformat()
        )
