import copy
import datetime
import typing
from unittest import mock
from urllib import parse

import anys
from first import first
from freezegun import freeze_time
import msgpack
import pytest

from mergify_engine import check_api
from mergify_engine import config
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import engine
from mergify_engine import eventlogs
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine import utils
from mergify_engine import yaml
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions
from mergify_engine.rules.config import priority_rules as pr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import base


TEMPLATE_GITHUB_ACTION = """
name: Continuous Integration
on:
  pull_request:
    branches:
      - main

jobs:
  unit-tests:
    timeout-minutes: 5
    runs-on: ubuntu-20.04
    steps:
      - uses: actions/checkout@v2
      - run: %s
"""


class TestQueueAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    @mock.patch.object(config, "ALLOW_QUEUE_PRIORITY_ATTRIBUTE", False)
    async def test_priority_brownout(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"priority": "high"}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "failure" == checks[0]["conclusion"]
        assert (
            "The current Mergify configuration is invalid"
            == checks[0]["output"]["title"]
        )
        assert (
            "`high` is invalid for dictionary value @ pull_request_rules → item 0 → actions → queue → priority"
            in checks[0]["output"]["summary"]
        )
        assert (
            "The configuration uses the deprecated `priority` attribute"
            in checks[0]["output"]["summary"]
        )

    async def test_queue_priority_deprecation_notice(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"priority": "high"}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "success" == checks[0]["conclusion"]
        assert (
            "**The configuration uses the deprecated `priority` attribute of the queue action and must be replaced by `priority_rules`.**"
            in checks[0]["output"]["summary"]
        )

    async def test_queue_priority_deprecation_notice_not_present_in_summary(
        self,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        checks = await context.Context(self.repository_ctxt, p1).pull_engine_check_runs
        assert len(checks) == 1
        assert "success" == checks[0]["conclusion"]
        assert (
            "**The configuration uses the deprecated `priority` attribute of the queue action and must be replaced by `priority_rules`.**"
            not in checks[0]["output"]["summary"]
        )

    async def test_queue_routing_conditions_matching_with_pull_request_rules(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "routing_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
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
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.add_label(p1["number"], "hotfix")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        assert (
            "**Required conditions of queue** `hotfix` **for merge:**"
            in check["output"]["summary"]
        )

    async def test_queue_routing_conditions_failure_with_pull_request_rules_and_no_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "routing_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "routing_conditions": [
                        "label=toto",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "failure"
        assert (
            check["output"]["title"] == "There are no queue routing conditions matching"
        )
        assert (
            check["output"]["summary"]
            == "There are routing conditions defined in the configuration, but none matches; the pull request has not been embarked"
        )

    async def test_queue_routing_conditions_failure_with_pull_request_rules_and_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "routing_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        assert (
            "**Required conditions of queue** `default` **"
            in check["output"]["summary"]
        )

    async def test_queue_routing_conditions_success(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": [
                        "files~=^test/",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"test/toto.txt": "toto"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

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

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )

    async def test_queue_routing_conditions_failure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": [
                        "files~=^dummy/",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"test/toto.txt": "toto"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [])

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "failure"
        assert (
            check["output"]["title"] == "There are no queue routing conditions matching"
        )
        assert (
            check["output"]["summary"]
            == "There are routing conditions defined in the configuration, but none matches; the pull request has not been embarked"
        )

    async def test_queue_routing_conditions_failure_update_after_queued(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": ["label=routing"],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"test/toto.txt": "toto"})
        await self.add_label(p1["number"], "queue")
        await self.add_label(p1["number"], "routing")
        await self.run_engine()

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
        await self.remove_label(p1["number"], "routing")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "failure"
        assert (
            check["output"]["title"] == "There are no queue routing conditions matching"
        )
        assert (
            check["output"]["summary"]
            == "There are routing conditions defined in the configuration, but none matches; the pull request has not been embarked"
        )

    async def test_queue_routing_conditions_empty(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": [],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(files={"test/toto.txt": "toto"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

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

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )

    async def test_queue_routing_conditions_matching_with_pull_request_rules_and_queue_forced(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "routing_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
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
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.add_label(p1["number"], "hotfix")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        assert (
            "**Required conditions of queue** `default` **for merge:**"
            in check["output"]["summary"]
        )

    async def test_queue_priority_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "priority_rules": [
                        {
                            "name": "hotfix PR detected",
                            "conditions": [
                                "label=hotfix",
                            ],
                            "priority": "high",
                        },
                        {
                            "name": "default priority",
                            "conditions": [
                                "-label=hotfix",
                                "-label=low",
                            ],
                            "priority": "medium",
                        },
                        {
                            "name": "low priority",
                            "conditions": [
                                "label=low",
                            ],
                            "priority": "low",
                        },
                    ],
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_size": 2,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        p3 = await self.create_pr()
        await self.add_label(p3["number"], "queue")
        await self.add_label(p3["number"], "hotfix")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        await self.remove_label(p3["number"], "hotfix")
        await self.add_label(p3["number"], "low")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 3rd in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

    async def test_queue_rule_deleted(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge me",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        q = await self.get_train()
        assert len(await q.get_pulls()) == 1

        check = first(
            await context.Context(self.repository_ctxt, p).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge me (queue)",
        )
        assert check is not None
        assert check["conclusion"] is None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        updated_rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge only if label is present",
                    "conditions": [f"base={self.main_branch_name}", "label=automerge"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        p2 = await self.create_pr(files={".mergify.yml": yaml.dump(updated_rules)})
        await self.merge_pull(p2["number"])
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        p = await self.get_pull(p["number"])
        check = first(
            await context.Context(self.repository_ctxt, p).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge me (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        q = await self.get_train()
        assert len(await q.get_pulls()) == 0

    async def test_queue_inplace_train_resetted(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        # To force p1 to be rebased
        p2 = await self.create_pr()
        await self.merge_pull_as_admin(p2["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        p2 = await self.get_pull(p2["number"])

        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        q = await self.get_train()
        # base sha should have been updated
        assert p2["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        new_sha = await self.push_file("whatever", "content")
        await self.run_engine()

        await q.load()
        # base sha should have been updated and PR still queued
        await self.assert_merge_queue_contents(
            q,
            new_sha,
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    new_sha,
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

    async def test_queue_inplace_interrupted(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {"name": "default", "require_branch_protection": False}
                    },
                },
            ],
        }

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.setup_repo(yaml.dump(rules))
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        # To force p1 to be rebased
        p2 = await self.create_pr()
        await self.merge_pull_as_admin(p2["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        p2 = await self.get_pull(p2["number"])

        q = await self.get_train()
        # base sha should have been updated
        assert p2["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        # To force p1 to be rebased a second times
        p3 = await self.create_pr()
        await self.merge_pull_as_admin(p3["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        p3 = await self.get_pull(p3["number"])

        q = await self.get_train()
        # base sha should have been updated again
        assert p3["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p3["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p3["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

    async def test_queue_with_bot_account(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": False,
                    "draft_bot_account": "mergify-test4",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        assert tmp_pull_1["number"] not in [p1["number"], p2["number"]]
        assert tmp_pull_1["pull_request"]["user"]["login"] == "mergify-test4"
        assert tmp_pull_2["number"] not in [p1["number"], p2["number"]]
        assert tmp_pull_2["pull_request"]["user"]["login"] == "mergify-test4"

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        async def assert_queued() -> None:
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

        await self.run_engine()
        await assert_queued()
        assert tmp_pull_1["pull_request"]["commits"] == 2
        assert tmp_pull_1["pull_request"]["changed_files"] == 1
        assert tmp_pull_2["pull_request"]["commits"] == 5
        assert tmp_pull_2["pull_request"]["changed_files"] == 2

        await self.create_status(tmp_pull_2["pull_request"])
        await self.run_engine()
        await assert_queued()

        await self.wait_for_pull_request("closed", tmp_pull_2["number"])

        await self.create_status(tmp_pull_1["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_1["number"])
        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [p1["number"], p2["number"]]

        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_with_queue_branch_prefix(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "queue_branch_prefix": "mq-",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        assert tmp_pull_1["number"] not in [p1["number"], p2["number"]]
        assert tmp_pull_1["pull_request"]["head"]["ref"].startswith("mq-")
        assert tmp_pull_2["pull_request"]["number"] not in [p1["number"], p2["number"]]
        assert tmp_pull_2["pull_request"]["head"]["ref"].startswith("mq-")

    async def test_queue_fast_forward(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "priority": "high",
                            "method": "fast-forward",
                        }
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin", two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        pulls = await self.get_pulls()
        assert len(pulls) == 2

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                )
            ],
            [p2["number"]],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p1)
        await self.run_engine()

        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        check = first(
            await context.Context(
                self.repository_ctxt, p1_merged["pull_request"]
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )
        assert (
            check["output"]["summary"]
            == f"The pull request has been merged automatically at *{p1_merged['pull_request']['head']['sha']}*"
        )

        branch = typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}"
            ),
        )
        assert p1_merged["pull_request"]["head"]["sha"] == branch["commit"]["sha"]

        # Continue with the second PR
        await self.assert_merge_queue_contents(
            q,
            p1["head"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p1_merged["pull_request"]["head"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p2["number"],
                )
            ],
        )

        head_sha = p2["head"]["sha"]
        p2 = await self.get_pull(p2["number"])
        assert p2["head"]["sha"] != head_sha  # ensure it has been rebased

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p2)
        await self.run_engine()

        p2_merged = await self.wait_for_pull_request("closed", p2["number"])
        check = first(
            await context.Context(
                self.repository_ctxt, p2_merged["pull_request"]
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )
        assert (
            check["output"]["summary"]
            == f"The pull request has been merged automatically at *{p2_merged['pull_request']['head']['sha']}*"
        )

        branch = typing.cast(
            github_types.GitHubBranch,
            await self.client_integration.item(
                f"{self.url_origin}/branches/{self.main_branch_name}"
            ),
        )
        assert p2_merged["pull_request"]["head"]["sha"] == branch["commit"]["sha"]
        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_with_ci_and_files(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        {
                            "or": [
                                "status-success=continuous-integration/fake-ci",
                                "files~=^.*\\.rst$",
                            ]
                        }
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
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

        await self.add_label(p["number"], "queue")
        await self.run_engine()

        tmp_pull = await self.wait_for_pull_request("opened")

        await self.create_status(tmp_pull["pull_request"], state="failure")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        ctxt = context.Context(self.repository_ctxt, p)
        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [])
        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge train",
        )
        assert check is not None
        assert check["conclusion"] == "failure"

        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"

    async def test_merge_queue_conflict(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=merge",
                    ],
                    "actions": {"merge": {}},
                },
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")

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
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
        )

        await self.add_label(p1["number"], "merge")
        await self.run_engine()
        await q.load()
        await self.assert_merge_queue_contents(q, None, [])

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events?per_page=5",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.merge",
                    "metadata": {"branch": self.main_branch_name},
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge",
                },
                {
                    "event": "action.queue.leave",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "merged": False,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "reason": f"Pull request #{p1['number']} has been dequeued. Pull "
                        "request automatically merged by a `merge` "
                        "action",
                        "seconds_waiting_for_freeze": 0,
                        "seconds_waiting_for_schedule": 0,
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge",
                },
                {
                    "event": "action.queue.checks_end",
                    "metadata": {
                        "abort_code": "PR_DEQUEUED",
                        "abort_reason": f"Pull request #{p1['number']} has been "
                        "dequeued. Pull request automatically "
                        "merged by a `merge` action",
                        "abort_status": "DEFINITIVE",
                        "aborted": True,
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_ended_at": None,
                            "checks_started_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                        },
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "event": "action.queue.checks_start",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_ended_at": None,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                        },
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "event": "action.queue.enter",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Queue",
                },
            ],
            "per_page": 5,
            "size": 5,
            "total": 5,
        }

    async def test_basic_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        async def assert_queued() -> None:
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

        await self.run_engine()
        await assert_queued()
        assert tmp_pull_1["pull_request"]["commits"] == 2
        assert tmp_pull_1["pull_request"]["changed_files"] == 1
        assert tmp_pull_2["pull_request"]["commits"] == 5
        assert tmp_pull_2["pull_request"]["changed_files"] == 2

        await self.create_status(tmp_pull_2["pull_request"])
        await self.run_engine()
        await assert_queued()

        await self.create_comment_as_admin(p1["number"], "@mergifyio refresh")
        await self.run_engine()
        await assert_queued()

        await self.create_status(tmp_pull_1["pull_request"], state="pending")
        await self.run_engine()
        await assert_queued()

        await self.create_status(tmp_pull_1["pull_request"])
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def _test_queue_with_rebase_on_githubapp_pr(
        self, rules: dict[str, typing.Any]
    ) -> None:
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p = (await self.wait_for_pull_request("closed"))["pull_request"]
        assert p["merge_commit_sha"] is not None
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = (await self.wait_for_pull_request("synchronize"))["pull_request"]
        assert p1["head"]["sha"] != head_sha  # ensure it has been rebased
        commits = await self.get_commits(p1["number"])
        assert len(commits) == 1
        assert commits[0]["commit"]["committer"]["name"] == "mergify-test4"

    # The 2 tests below this comment are the same, just the rules are differents.
    # This makes sures that the behavior is the same with the queue configuration in `queue_rules`
    # and in the `actions/queue`
    async def test_queue_with_rebase_on_githubapp_pr(self) -> None:
        await self._test_queue_with_rebase_on_githubapp_pr(
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "conditions": [
                            "status-success=continuous-integration/fake-ci",
                        ],
                    }
                ],
                "pull_request_rules": [
                    {
                        "name": "Merge priority high",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "name": "default",
                                "priority": "high",
                                "update_method": "rebase",
                                "update_bot_account": '{% if not author or author.endswith("[bot]") -%}\nmergify-test4\n{% else -%}\n{{ author }}\n{% endif -%}',  # noqa [FS003]
                            }
                        },
                    },
                ],
            }
        )

    async def test_queue_with_rebase_on_githubapp_pr_conf_in_queue_rules(self) -> None:
        await self._test_queue_with_rebase_on_githubapp_pr(
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "conditions": [
                            "status-success=continuous-integration/fake-ci",
                        ],
                        "update_method": "rebase",
                        "update_bot_account": '{% if not author or author.endswith("[bot]") -%}\nmergify-test4\n{% else -%}\n{{ author }}\n{% endif -%}',  # noqa [FS003]
                    }
                ],
                "pull_request_rules": [
                    {
                        "name": "Merge priority high",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "name": "default",
                                "priority": "high",
                            }
                        },
                    },
                ],
            }
        )

    async def test_queue_with_rebase_update_method(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "priority": "high",
                            "update_method": "rebase",
                            "update_bot_account": "{% if 1 -%}mergify-test4{% endif -%}",  # noqa [FS003]
                        }
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        pulls = await self.get_pulls()
        assert len(pulls) == 2

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
            [p2["number"]],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it has been rebased
        commits = await self.get_commits(p1["number"])
        assert len(commits) == 1
        assert commits[0]["commit"]["committer"]["name"] == "mergify-test4"

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p1)
        await self.run_engine()

        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        await self.wait_for("pull_request", {"action": "synchronize"})

        assert p1_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p1_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p2["number"],
                ),
            ],
        )

        head_sha = p2["head"]["sha"]
        p2 = await self.get_pull(p2["number"])
        assert p2["head"]["sha"] != head_sha  # ensure it has been rebased
        commits = await self.get_commits(p2["number"])
        assert len(commits) == 2
        assert commits[0]["commit"]["committer"]["name"] == "mergify-test4"
        assert commits[1]["commit"]["committer"]["name"] == "mergify-test4"

        await self.create_status(p2)
        await self.run_engine()

        await self.wait_for_pull_request("closed", p2["number"])
        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_no_inplace(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        tmp_pull = await self.wait_for_pull_request("opened")
        assert tmp_pull["number"] not in [p1["number"]]
        assert tmp_pull["pull_request"]["changed_files"] == 1

        # No parent PR, but created instead updated
        q = await self.get_train()
        assert p["merge_commit_sha"]
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
        )

        await self.create_status(tmp_pull["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull["number"])
        p1_merged = await self.wait_for_pull_request("closed", p1["number"])

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        await self.assert_merge_queue_contents(q, None, [])
        # ensure the MERGE QUEUE SUMMARY succeed
        check = first(
            await context.Context(
                self.repository_ctxt, p1_merged["pull_request"]
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert check["conclusion"] == check_api.Conclusion.SUCCESS.value

    async def test_queue_update_inplace_merge_report(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=another-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "priority": "high",
                            "update_method": "rebase",
                        }
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        # Create 2 PR and put them in a queue
        p1 = await self.create_pr(as_="admin")
        await self.create_status(p1)
        await self.run_engine()
        p2 = await self.create_pr(as_="admin")
        await self.create_status(p2)
        await self.run_engine()

        # Merge p1 to force p2 to be rebased
        # The action triggers all CI again, including the one that put p2 in queue
        await self.create_status(p1, context="another-ci")
        await self.run_engine()
        p1 = await self.get_pull(p1["number"])

        # Ensure p2 is still in queue
        q = await self.get_train()
        assert p1["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p1["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p1["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p2["number"],
                ),
            ],
        )

        # Ensure that it have been rebased
        head_sha = p2["head"]["sha"]
        p2 = await self.get_pull(p2["number"])
        assert p2["head"]["sha"] != head_sha

        # Complete condition for merge
        await self.create_status(p2, context="another-ci")
        await self.run_engine()
        assert p1["merge_commit_sha"] is not None
        # Ensure p2 is still in queue
        await self.assert_merge_queue_contents(
            q,
            p1["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p1["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p2["number"],
                ),
            ],
        )

        # Ensure all conditions have been reported, included the ones that put
        # PR in queue
        p2_checks = await context.Context(
            self.repository_ctxt, p2
        ).pull_engine_check_runs
        check = first(
            p2_checks,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        summary = check["output"]["summary"]
        assert (
            """- [X] `status-success=another-ci`

**Required conditions to stay in the queue:**

- [ ] `status-success=continuous-integration/fake-ci`

**The following pull requests are queued:**

| | Pull request | Queue/Priority | Speculative checks | Queued
"""
            in summary
        )

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events?per_page=5",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.queue.merged",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge priority high",
                },
                {
                    "event": "action.queue.leave",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "merged": True,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "reason": f"Pull request #{p1['number']} has been merged "
                        f"automatically at *{p1['merge_commit_sha']}*",
                        "seconds_waiting_for_schedule": 0,
                        "seconds_waiting_for_freeze": 0,
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge priority high",
                },
                {
                    "event": "action.queue.checks_end",
                    "metadata": {
                        "abort_code": None,
                        "abort_reason": "",
                        "abort_status": "DEFINITIVE",
                        "aborted": False,
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "success",
                            "checks_ended_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_started_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_timed_out": False,
                            "in_place": True,
                            "number": p1["number"],
                        },
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "event": "action.queue.checks_start",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_ended_at": None,
                            "checks_timed_out": False,
                            "in_place": True,
                            "number": p1["number"],
                        },
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "event": "action.queue.enter",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                    },
                    "pull_request": p1["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge priority high",
                },
            ],
            "per_page": 5,
            "size": 5,
            "total": 5,
        }

    async def test_unqueue_rule_unmatches(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 2,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin")

        await self.add_label(p1["number"], "queue")
        await self.run_engine()
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        await self.remove_label(p2["number"], "queue")
        await self.run_engine()

        await q.load()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        ctxt = context.Context(self.repository_ctxt, p2)
        check = await ctxt.get_engine_check_run("Rule: Merge priority high (queue)")
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        assert check is not None
        assert check["conclusion"] == "neutral"
        assert check["output"]["title"].endswith(
            "cannot be merged and has been disembarked"
        )

    async def test_unqueue_rule_unmatch_with_batch_requeue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_closed = await self.wait_for_pull_request("closed", pr_number=p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        await self.run_full_engine()

        q = await self.get_train()
        assert p_closed["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"], p3["number"]],
                    [],
                    p_closed["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
        )

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        tmp_pull_2 = await self.wait_for_pull_request("opened")
        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_closed["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"], p3["number"]],
                    [],
                    p_closed["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events?per_page=2",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.queue.leave",
                    "metadata": {
                        "merged": False,
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "reason": f"Pull request #{p1['number']} has been dequeued. The pull request rule doesn't match anymore",
                        "seconds_waiting_for_schedule": 0,
                        "seconds_waiting_for_freeze": 0,
                    },
                    "repository": p1["base"]["repo"]["full_name"],
                    "pull_request": p1["number"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge priority high",
                },
                {
                    "event": "action.queue.checks_end",
                    "metadata": {
                        "abort_reason": anys.AnySearch(
                            str(
                                queue_utils.PrDequeued(
                                    pr_number=p1["number"],
                                    details=". The pull request rule doesn't match anymore",
                                )
                            ),
                        ),
                        "abort_code": queue_utils.PrDequeued.unqueue_code,
                        "abort_status": "DEFINITIVE",
                        "aborted": True,
                        "branch": self.main_branch_name,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_ended_at": None,
                            "checks_started_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                        },
                    },
                    "repository": p1["base"]["repo"]["full_name"],
                    "pull_request": p1["number"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
            ],
            "per_page": 2,
            "size": 2,
            "total": 4,
        }

    async def test_unqueue_command_with_batch_requeue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        await self.run_engine()

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"], p3["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio unqueue")
        await self.run_engine()
        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=p1["number"]
        )
        await self.run_engine()
        tmp_pull_2 = await self.wait_for_pull_request("opened")
        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"], p3["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "The pull request has been removed from the queue"
        )
        assert check["output"]["summary"].startswith(
            f"Pull request #{p1['number']} has been dequeued by an `unqueue` command.\n"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["conclusion"] is None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["conclusion"] is None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

    async def test_train_reset_after_unexpected_base_branch_change_while_merging_batch(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_size": 2,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        sources = [
            context.T_PayloadEventSource(
                {
                    "event_type": "refresh",
                    "data": github_types.GitHubEventRefresh(
                        {
                            "received_at": github_types.ISODateTimeType(
                                "2022-07-26T14:14:14.000000+00:00"
                            ),
                            "organization": p1["base"]["repo"]["owner"],
                            "installation": {
                                "id": github_types.GitHubInstallationIdType(123456),
                                "account": p1["base"]["repo"]["owner"],
                                "target_type": "User",
                                "permissions": {},
                                "suspended_at": None,
                            },
                            "sender": p1["user"],
                            "repository": p1["base"]["repo"],
                            "action": "user",
                            "ref": p1["base"]["ref"],
                            "pull_request_number": p1["number"],
                            "source": "internal",
                        }
                    ),
                    "timestamp": github_types.ISODateTimeType(
                        "2022-07-26T14:14:14.000000+00:00"
                    ),
                    "initial_score": 0,
                }
            ),
        ]

        batch_draft_pr = await self.wait_for_pull_request("opened")

        ctxt = await self.repository_ctxt.get_pull_request_context(
            batch_draft_pr["number"], batch_draft_pr["pull_request"]
        )
        q = await self.get_train()
        base_sha = await q.get_base_sha()
        await self.assert_merge_queue_contents(
            q,
            base_sha,
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"]],
                    [],
                    base_sha,
                    merge_train.TrainCarChecksType.DRAFT,
                    batch_draft_pr["number"],
                ),
            ],
        )
        await self.create_status(batch_draft_pr["pull_request"])
        await engine.run(ctxt, sources)

        ctxt = await self.repository_ctxt.get_pull_request_context(p1["number"], p1)
        await engine.run(ctxt, sources)

        await self.wait_for_pull_request("closed", pr_number=batch_draft_pr["number"])

        p1_merged = await self.wait_for_pull_request("closed", pr_number=p1["number"])
        assert p1_merged["pull_request"]["merged"]

        p2 = await self.get_pull(p2["number"])
        assert not p2["merged"]

        # we push unexpected changes to the base branch
        await self.push_file()

        ctxt = await self.repository_ctxt.get_pull_request_context(p2["number"], p2)
        await engine.run(ctxt, sources)

        p2 = await self.get_pull(p2["number"])
        assert not p2["merged"]

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

    async def test_batch_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 2,
                    "allow_inplace_checks": True,
                    "batch_size": 2,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()
        p4 = await self.create_pr()
        p5 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.add_label(p4["number"], "queue")
        await self.add_label(p5["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"], p4["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
            [p5["number"]],
        )
        assert tmp_pull_1["pull_request"]["changed_files"] == 2
        assert tmp_pull_2["pull_request"]["changed_files"] == 4

        await self.create_status(tmp_pull_1["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_1["number"])
        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [p1["number"], p2["number"]]

        tmp_pull_3 = await self.wait_for_pull_request("opened")

        p2 = await self.get_pull(p2["number"])
        assert p2["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"], p4["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p5["number"]],
                    [p3["number"], p4["number"]],
                    p2["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_3["number"],
                ),
            ],
        )
        assert tmp_pull_2["pull_request"]["changed_files"] == 4
        assert tmp_pull_3["pull_request"]["changed_files"] == 3

        await self.create_status(tmp_pull_2["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_2["number"])
        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [p3["number"], p4["number"]]

        await self.create_status(tmp_pull_3["pull_request"])
        await self.run_engine()
        await self.wait_for_pull_request("closed", tmp_pull_3["number"])
        await self.wait_for_pull_request("closed", p5["number"])

        await self.assert_merge_queue_contents(q, None, [])

    async def test_batch_split_with_no_speculative_checks(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_max_wait_time": "0 s",
                    "allow_inplace_checks": False,
                    "speculative_checks": 1,
                    "batch_size": 3,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        p4 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.add_label(p4["number"], "queue")
        await self.run_engine()

        tmp_pull = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"], p3["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
            [p4["number"]],
        )

        await self.create_status(tmp_pull["pull_request"], state="failure")
        await self.run_engine()

        tmp_pull_sliced_1 = await self.wait_for_pull_request("opened")

        # The train car has been splitted, the second car is in pending
        # state as speculative_checks=1
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_sliced_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    None,
                    None,
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
            [p4["number"]],
        )

        # Merge p1, p2 should be started to be checked, second car must go to
        # created state
        await self.create_status(tmp_pull_sliced_1["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_sliced_1["number"])
        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_merged["pull_request"]["merged"]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        tmp_pull_2 = await self.wait_for_pull_request("opened")

        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
            [p4["number"]],
        )

        # It's fault of p2!
        await self.create_status(tmp_pull_2["pull_request"], state="failure")
        await self.run_engine()

        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [
            tmp_pull["number"],
            tmp_pull_2["number"],
        ]

        tmp_pull_3 = await self.wait_for_pull_request("opened")

        assert p1_merged["pull_request"]["merge_commit_sha"] is not None
        # Thing move on and restart from p3 but based on p1 merge commit
        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"], p4["number"]],
                    [],
                    p1_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_3["number"],
                ),
            ],
            [],
        )

    async def test_batch_split_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 2,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()
        p4 = await self.create_pr()
        p5 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.add_label(p4["number"], "queue")
        await self.add_label(p5["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"], p3["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p4["number"], p5["number"]],
                    [p1["number"], p2["number"], p3["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        await self.create_status(tmp_pull_1["pull_request"], state="failure")
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_2["number"])
        tmp_pull_3 = await self.wait_for_pull_request("opened")
        tmp_pull_4 = await self.wait_for_pull_request("opened")

        # The train car has been splitted
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_3["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_4["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
            [p4["number"], p5["number"]],
        )

        # Merge p1 and p2, p3 should be dropped and p4 et p5 checked
        await self.create_status(tmp_pull_3["pull_request"])
        await self.create_status(tmp_pull_4["pull_request"])
        await self.run_engine()

        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [
            p1["number"],
            p2["number"],
            tmp_pull_1["number"],
            tmp_pull_3["number"],
            tmp_pull_4["number"],
        ]

        tmp_pull_6 = await self.wait_for_pull_request("opened")

        p2 = await self.get_pull(p2["number"])
        assert p2["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p4["number"], p5["number"]],
                    [],
                    p2["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_6["number"],
                ),
            ],
        )

        await self.create_status(tmp_pull_6["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull_6["number"])
        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [p4["number"], p5["number"]]

        await self.assert_merge_queue_contents(q, None, [])

    async def test_first_batch_split_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_max_wait_time": "0 s",
                    "speculative_checks": 2,
                    "batch_size": 3,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to create draft PR
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
        )

        await self.create_status(tmp_pull["pull_request"], state="failure")
        await self.run_engine()

        tmp_pull_2 = await self.wait_for_pull_request("opened")

        # The train car has been splitted
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
            [],
        )

        # Merge p1, p2 should be marked as failure
        await self.create_status(tmp_pull_2["pull_request"])
        await self.run_engine()

        p_closed = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]
        assert sorted([p["number"] for p in p_closed]) == [
            p1["number"],
            tmp_pull["number"],
            tmp_pull_2["number"],
        ]

        await self.assert_merge_queue_contents(q, None, [])

    async def test_unqueue_on_synchronise_and_rule_still_match_then_requeue(
        self,
    ) -> None:
        rules_config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=other-ci",
                    ],
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules_config))

        p1 = await self.create_pr()
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )
        car = q._cars[0]

        # we push changes to the draft PR's branch
        await self.push_file(target_branch=p1["head"]["ref"])
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        # Ensure this is not the same car, a new car has to be created
        await q.load()
        assert car != q._cars[0]

    async def test_unqueue_on_synchronise_and_rule_unmatch(self) -> None:
        rules_config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=other-ci",
                    ],
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules_config))

        p1 = await self.create_pr()
        await self.create_status(p1)
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        # we push changes to the draft PR's branch
        await self.push_file(target_branch=p1["head"]["ref"])
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"

    async def assert_api_checks_end_reason(
        self, pr_number: github_types.GitHubPullRequestNumber, expected_reason: str
    ) -> None:
        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{pr_number}/events",
        )
        assert r.status_code == 200
        response = r.json()
        for event in response["events"]:
            if event["event"] == "action.queue.checks_end":
                assert expected_reason in event["metadata"]["abort_reason"]

    async def test_unqueue_all_pr_when_unexpected_changes_on_draft_pr(self) -> None:
        rules_config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_max_wait_time": "0 s",
                    "speculative_checks": 2,
                    "batch_size": 3,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules_config))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        assert draft_pr["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    draft_pr["number"],
                ),
            ],
        )

        # we push changes to the draft PR's branch
        draft_pr_head_ref = draft_pr["pull_request"]["head"]["ref"]
        await self.git("fetch", "origin", draft_pr_head_ref)
        await self.git("checkout", "-b", "random", f"origin/{draft_pr_head_ref}")
        open(self.git.repository + "/random_file.txt", "wb").close()
        await self.git("add", "random_file.txt")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", f"random:{draft_pr_head_ref}")
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        await self.assert_api_checks_end_reason(
            p1["number"], "has sustained unexpected changes from external sources"
        )
        await self.assert_api_checks_end_reason(
            p2["number"], "has sustained unexpected changes from external sources"
        )

        # when detecting external changes onto the draft PR, the engine should disembark it and
        # unqueue all its contained PRs
        comments = await self.get_issue_comments(draft_pr["number"])
        assert (
            "cannot be merged, due to unexpected changes in this draft PR, and have been disembarked"
            in comments[-1]["body"]
        )

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request has been removed from the queue"
        )
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request has been removed from the queue"
        )

    async def test_draft_pr_train_reset_after_unexpected_base_branch_changes(
        self,
    ) -> None:
        rules_config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_max_wait_time": "0 s",
                    "speculative_checks": 2,
                    "batch_size": 3,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules_config))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        assert draft_pr["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    draft_pr["number"],
                ),
            ],
        )

        # We push changes to the base branch
        await self.git("fetch", "origin", self.main_branch_name)
        await self.git("checkout", "-b", "random", f"origin/{self.main_branch_name}")
        open(self.git.repository + "/random_file.txt", "wb").close()
        await self.git("add", "random_file.txt")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", f"random:{self.main_branch_name}")
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        await self.assert_api_checks_end_reason(
            p1["number"],
            "Unexpected queue change: an external action moved the target branch head to",
        )

        # when detecting base branch changes, the engine should reset the train
        comment = await self.wait_for_issue_comment(str(draft_pr["number"]), "created")
        assert "The whole train will be reset." in comment["comment"]["body"]
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

    async def test_pr_requeue_after_pr_unexpected_updated_changes(
        self,
    ) -> None:
        rules_config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules_config))

        p1 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        # We push changes to the pull request
        await self.git("fetch", "origin", self.main_branch_name)
        await self.git("checkout", "-b", "my_pr_branch", f"origin/{p1['head']['ref']}")
        open(self.git.repository + "/random_file.txt", "wb").close()
        await self.git("add", "random_file.txt")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", f"my_pr_branch:{p1['head']['ref']}")
        await self.wait_for("push", {"ref": f"refs/heads/{p1['head']['ref']}"})
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        p1 = await self.get_pull(p1["number"])
        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p1['number']}/events",
        )
        assert r.status_code == 200
        response = r.json()
        timestamp_checks_end = None
        timestamp_queue_leave = None

        # Unexpected change detected
        for event in response["events"]:
            if event["event"] == "action.queue.checks_end":
                assert (
                    f"Unexpected queue change: the updated pull request #{p1['number']} has been manually updated"
                    in event["metadata"]["abort_reason"]
                )
                timestamp_checks_end = event["timestamp"]
                break
        else:
            raise AssertionError("Event action.queue.checks_end not found")

        # Pr is sliced from queue to be updated
        assert timestamp_checks_end is not None
        for event in response["events"]:
            if event["event"] == "action.queue.leave":
                assert event["timestamp"] > timestamp_checks_end
                assert event["pull_request"] == p1["number"]
                timestamp_queue_leave = event["timestamp"]
                break
        else:
            raise AssertionError("Event action.queue.leave not found")

        # Pr is updated and requeued
        assert timestamp_queue_leave is not None
        for event in response["events"]:
            if event["event"] == "action.queue.enter":
                assert event["timestamp"] > timestamp_queue_leave
                assert event["pull_request"] == p1["number"]
                break
        else:
            raise AssertionError("Event action.queue.enter not found")

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

    async def test_queue_just_rebase(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": ["label=queue"],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        p_other = await self.create_pr()
        await self.merge_pull(p_other["number"])
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [])

        pulls = await self.get_pulls()
        assert len(pulls) == 0

    async def test_queue_already_ready(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": ["label=queue"],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()

        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [])

        pulls = await self.get_pulls()
        assert len(pulls) == 0

    async def test_queue_with_ci_in_pull_request_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.create_status(p1)
        await self.add_label(p1["number"], "queue")
        await self.create_status(p2)
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p1 = await self.get_pull(pulls[1]["number"])
        tmp_mq_p2 = await self.get_pull(pulls[0]["number"])
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        assert tmp_mq_p1["commits"] == 2
        assert tmp_mq_p1["changed_files"] == 1
        assert tmp_mq_p2["commits"] == 5
        assert tmp_mq_p2["changed_files"] == 2
        await self.create_status(tmp_mq_p2)

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        await self.create_status(tmp_mq_p1, state="pending")
        await self.run_engine()

        # Ensure it have not been cancelled on pending event
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )
        await self.create_status(tmp_mq_p1, state="success")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_merge_queue_refresh(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        pulls_in_queue = await q.get_pulls()
        assert pulls_in_queue == [p1["number"], p2["number"]]

        mq_pr_number = q._cars[1].queue_pull_request_number
        assert mq_pr_number is not None
        await self.create_comment_as_admin(mq_pr_number, "@mergifyio update")
        await self.run_engine()
        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=mq_pr_number
        )
        comments = await self.get_issue_comments(mq_pr_number)
        assert (
            "Command not allowed on merge queue pull request." in comments[-1]["body"]
        )

        await self.create_comment_as_admin(mq_pr_number, "@mergifyio refresh")
        await self.run_engine()
        await self.wait_for(
            "issue_comment", {"action": "created"}, test_id=mq_pr_number
        )
        comments = await self.get_issue_comments(mq_pr_number)
        assert (
            f"""> refresh

#### ✅ Pull request refreshed



{utils.get_mergify_payload({"command": "refresh", "conclusion": "success"})}"""
            == comments[-1]["body"]
        )

    async def test_queue_branch_fast_forward_basic(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                    "batch_size": 3,
                    "speculative_checks": 5,
                    "batch_max_wait_time": "0 s",
                    "queue_branch_merge_method": "fast-forward",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        p4 = await self.create_pr()
        p5 = await self.create_pr()

        # Queue PRs
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.add_label(p4["number"], "queue")
        await self.run_engine()

        tmp_mq_p1 = await self.wait_for_pull_request("opened")
        tmp_mq_p2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p2["number"], p3["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p4["number"]],
                    [p1["number"], p2["number"], p3["number"]],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Merge p1
        await self.create_status(tmp_mq_p1["pull_request"])
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        for p in (p1, p2, p3):
            await self.wait_for("pull_request", {"action": "closed"})
            assert (await self.get_pull(p["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p1_refreshed = await self.get_pull(tmp_mq_p1["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name, bypass_cache=True
        )
        assert branch["commit"]["sha"] == tmp_mq_p1_refreshed["head"]["sha"]

        # merge the second one
        await self.create_status(tmp_mq_p2["pull_request"])
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        assert (await self.get_pull(p4["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p2_refreshed = await self.get_pull(tmp_mq_p2["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name, bypass_cache=True
        )
        assert branch["commit"]["sha"] == tmp_mq_p2_refreshed["head"]["sha"]

        # Queue is now empty, the process will restart
        await self.assert_merge_queue_contents(q, None, [])

        # Queue a new one and checks base commit is the good one
        await self.add_label(p5["number"], "queue")
        await self.run_engine()

        tmp_mq_p3 = await self.wait_for_pull_request("opened")

        await self.assert_merge_queue_contents(
            q,
            tmp_mq_p2_refreshed["head"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p5["number"]],
                    [],
                    tmp_mq_p2_refreshed["head"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

        # merge the third one
        await self.create_status(tmp_mq_p3["pull_request"])
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        assert (await self.get_pull(p5["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p3_refreshed = await self.get_pull(tmp_mq_p3["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name, bypass_cache=True
        )
        assert branch["commit"]["sha"] == tmp_mq_p3_refreshed["head"]["sha"]

        # Queue is now empty
        await self.assert_merge_queue_contents(q, None, [])

    async def test_ongoing_train_basic(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        # Queue PRs
        await self.add_label(p1["number"], "queue")
        await self.run_engine()
        tmp_mq_p1 = await self.wait_for_pull_request("opened")

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_mq_p2 = await self.wait_for_pull_request("opened")

        # Check Queue
        pulls = await self.get_pulls()
        # 2 queue PR with its tmp PR + 1 one not queued PR
        assert len(pulls) == 5
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Merge p1
        await self.create_status(tmp_mq_p1["pull_request"])
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()
        pulls = await self.get_pulls()
        assert len(pulls) == 3
        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]

        # ensure base is p, it's tested with p1, but current_base_sha have changed since
        # we create the tmp pull request
        await self.assert_merge_queue_contents(
            q,
            p1["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Queue p3
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_mq_p3 = await self.wait_for_pull_request("opened")

        # Check train state
        pulls = await self.get_pulls()
        assert len(pulls) == 4

        assert tmp_mq_p3["number"] not in [
            p1["number"],
            p2["number"],
            p3["number"],
            tmp_mq_p2["number"],
        ]

        q = await self.get_train()
        assert p1["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p1["merge_commit_sha"],
            [
                # Ensure p2 car is still the same
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
                # Ensure base is p1 and only p2 is tested with p3
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p2["number"]],
                    p1["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

    async def test_ongoing_train_second_pr_ready_first(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        # Queue two pulls
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p1 = pulls[1]
        tmp_mq_p2 = pulls[0]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # p2 is ready first, ensure it's not merged
        await self.create_status(tmp_mq_p2)
        await self.run_engine()
        pulls = await self.get_pulls()
        assert len(pulls) == 3

        # Nothing change
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )
        p2 = await self.get_pull(p2["number"])
        assert not p2["merged"]

        # p1 is ready, check both are merged in a row
        await self.create_status(tmp_mq_p1)
        await self.run_engine()

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0
        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]
        p2 = await self.get_pull(p2["number"])
        assert p2["merged"]

        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_with_allow_queue_branch_edit_set_to_false(self) -> None:
        await self._do_test_queue_with_allow_queue_branch_edit(False)

    async def test_queue_with_allow_queue_branch_edit_set_to_true(self) -> None:
        await self._do_test_queue_with_allow_queue_branch_edit(True)

    async def _do_test_queue_with_allow_queue_branch_edit(
        self, allow_queue_branch_edit: bool
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                    "allow_queue_branch_edit": allow_queue_branch_edit,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")

        # we push changes to the draft PR's branch
        draft_pr_head_ref = draft_pr["pull_request"]["head"]["ref"]
        await self.git("fetch", "origin", f"{draft_pr_head_ref}")
        await self.git("checkout", "-b", "random", f"origin/{draft_pr_head_ref}")
        open(self.git.repository + "/random_file.txt", "wb").close()
        await self.git("add", "random_file.txt")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", f"random:{draft_pr_head_ref}")
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        # Get the new state
        draft_pr_refreshed = await self.get_pull(draft_pr["number"])
        assert (
            draft_pr_refreshed["state"] == "open"
            if allow_queue_branch_edit
            else "closed"
        )

    async def test_queue_ci_failure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p1 = pulls[1]
        tmp_mq_p2 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # tmp merge queue pull p2 fail
        await self.create_status(tmp_mq_p2, state="failure")
        await self.run_engine()

        await self.create_status(tmp_mq_p1, state="failure")
        await self.run_engine()

        # TODO(sileht): Add some assertion on check-runs content

        # tmp merge queue pull p2 have been closed and p2 updated/rebased
        pulls = await self.get_pulls()
        assert len(pulls) == 3
        tmp_mq_p2_bis = pulls[0]
        assert tmp_mq_p2 != tmp_mq_p2_bis
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2_bis["number"],
                ),
            ],
        )

        # Merge p2
        await self.create_status(tmp_mq_p2_bis)
        await self.run_engine()
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        # Only p1 is still there and the queue is empty
        pulls = await self.get_pulls()
        assert len(pulls) == 1
        assert pulls[0]["number"] == p1["number"]
        await self.assert_merge_queue_contents(q, None, [])

    async def test_batch_cant_create_tmp_pull_request(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "batch_size": 2,
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr(files={"conflicts": "well"})
        p2 = await self.create_pr(files={"conflicts": "boom"})
        p3 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_mq_pr = await self.wait_for_pull_request("opened")

        assert tmp_mq_pr["number"] not in [p1["number"], p2["number"], p3["number"]]

        # Check only p1 and p3 are in the train
        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"], p3["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_pr["number"],
                ),
            ],
        )

        # Ensure p2 status is updated with the failure
        p2 = await self.get_pull(p2["number"])
        ctxt_p2 = context.Context(self.repository_ctxt, p2)
        check = first(
            await ctxt_p2.pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert (
            check["output"]["title"] == "This pull request cannot be embarked for merge"
        )
        assert check["output"]["summary"] == (
            "The merge queue pull request can't be created\n"
            f"Details:\n> The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}\n"
        )

        # Merge the train
        await self.create_status(tmp_mq_pr["pull_request"])
        await self.run_engine()

        pulls = [
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
            await self.wait_for_pull_request("closed"),
        ]

        assert sorted([p["number"] for p in pulls]) == [
            p1["number"],
            p3["number"],
            tmp_mq_pr["number"],
        ]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        # Only p2 is remaining and not in train
        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_cant_create_tmp_pull_request(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr(as_="admin", files={"conflicts": "well"})
        p2 = await self.create_pr(as_="admin", files={"conflicts": "boom"})
        p3 = await self.create_pr(as_="admin")

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_mq_pr = await self.wait_for_pull_request("opened")

        assert tmp_mq_pr["number"] not in [p1["number"], p2["number"], p3["number"]]

        # Check only p1 and p3 are in the train
        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"]],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_pr["number"],
                ),
            ],
        )

        # Ensure p2 status is updated with the failure
        p2 = await self.get_pull(p2["number"])
        ctxt_p2 = context.Context(self.repository_ctxt, p2)
        check = first(
            await ctxt_p2.pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert (
            check["output"]["title"] == "This pull request cannot be embarked for merge"
        )
        assert check["output"]["summary"] == (
            "The merge queue pull request can't be created\n"
            f"Details:\n> The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}\n"
        )

        # Merge the train
        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for_pull_request("closed", p1["number"])
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        await self.create_status(tmp_mq_pr["pull_request"])
        await self.run_engine()

        # Only p2 is remaining and not in train
        await self.wait_for_pull_request("closed", tmp_mq_pr["number"])
        await self.wait_for_pull_request("closed", p3["number"])

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_cancel_and_refresh(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Tchou tchou",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin")
        p3 = await self.create_pr(as_="admin")

        # Queue PRs
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        tmp_mq_p2 = await self.wait_for_pull_request("opened")
        tmp_mq_p3 = await self.wait_for_pull_request("opened")

        assert tmp_mq_p3["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

        await self.create_status(p1)
        await self.run_engine()

        p1_merged = await self.wait_for_pull_request("closed", pr_number=p1["number"])
        assert p1_merged["pull_request"]["merge_commit_sha"] is not None

        # Ensure p1 is removed and current["head"]["sha"] have been updated on p2 and p3
        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p1_merged["pull_request"]["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p1_merged["pull_request"]["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

        # tmp merge queue pr p2, CI fails
        await self.create_status(tmp_mq_p2["pull_request"], state="failure")
        await self.run_engine()

        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")

        tmp_mq_p3_bis = await self.wait_for_pull_request("opened")

        # p3 get a new draft PR
        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [],
                    p1_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3_bis["number"],
                ),
            ],
        )

        # refresh to add it back in queue
        check = typing.cast(
            github_types.GitHubCheckRun,
            await self.client_integration.items(
                f"{self.url_origin}/commits/{p2['head']['sha']}/check-runs",
                resource_name="check runs",
                page_limit=5,
                api_version="antiope",
                list_items="check_runs",
                params={"name": constants.MERGE_QUEUE_SUMMARY_NAME},
            ).__anext__(),
        )
        check_suite_id = check["check_suite"]["id"]

        # click on refresh btn
        await self.installation_ctxt.client.post(
            f"{self.repository_ctxt.base_url}/check-suites/{check_suite_id}/rerequest",
            api_version="antiope",
        )
        await self.wait_for("check_suite", {"action": "rerequested"})
        await self.run_engine()

        tmp_mq_p2_bis = await self.wait_for_pull_request("opened")

        # Check pull is back to the queue and tmp pull recreated
        assert tmp_mq_p2_bis["number"] not in [p1["number"], p2["number"], p3["number"]]
        await self.assert_merge_queue_contents(
            q,
            p1_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [],
                    p1_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3_bis["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p3["number"]],
                    p1_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2_bis["number"],
                ),
            ],
        )

    async def test_queue_manual_merge(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        p_merged = await self.wait_for_pull_request("closed", pr_number=p["number"])
        await self.run_engine()

        # Queue PRs
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")

        await self.run_engine()

        tmp_mq_p1 = await self.wait_for_pull_request("opened")
        tmp_mq_p2 = await self.wait_for_pull_request("opened")

        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"]]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Merge a not queued PR manually
        p_merged_in_meantime = await self.create_pr()
        await self.merge_pull(p_merged_in_meantime["number"])

        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        p_merged_in_meantime = await self.get_pull(p_merged_in_meantime["number"])

        await self.run_engine()

        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")
        tmp_mq_p1_bis = await self.wait_for_pull_request("opened")
        tmp_mq_p2_bis = await self.wait_for_pull_request("opened")

        assert tmp_mq_p1_bis["number"] not in [
            p1["number"],
            p2["number"],
            tmp_mq_p1["number"],
            tmp_mq_p2["number"],
        ]
        assert tmp_mq_p2_bis["number"] not in [
            p1["number"],
            p2["number"],
            tmp_mq_p1["number"],
            tmp_mq_p2["number"],
        ]
        assert p_merged_in_meantime["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged_in_meantime["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged_in_meantime["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1_bis["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged_in_meantime["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2_bis["number"],
                ),
            ],
        )

        # Ensurep1 and p2 got recreate with more commits
        assert tmp_mq_p1_bis["pull_request"]["commits"] == 2
        assert tmp_mq_p1_bis["pull_request"]["changed_files"] == 1
        assert tmp_mq_p2_bis["pull_request"]["commits"] == 4
        assert tmp_mq_p2_bis["pull_request"]["changed_files"] == 2

        # Merge the train
        await self.create_status(tmp_mq_p1_bis["pull_request"])
        await self.create_status(tmp_mq_p2_bis["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")

        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_pr_priority_no_interrupt(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
                {
                    "name": "Merge priority low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=low",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "low"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p_merged = await self.create_pr()
        await self.merge_pull(p_merged["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p_merged = await self.get_pull(p_merged["number"])

        # Put first PR in queue
        await self.add_label(p1["number"], "low")
        await self.add_label(p2["number"], "low")
        await self.run_engine()

        q = await self.get_train()

        # my 3 PRs + 2 merge queue PR
        pulls = await self.get_pulls()
        assert len(pulls) == 5

        tmp_mq_p1 = pulls[1]
        tmp_mq_p2 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert p_merged["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Change the configuration and introduce disallow_checks_interruption_from_queues
        updated_rules = typing.cast(dict[typing.Any, typing.Any], copy.deepcopy(rules))
        updated_rules["queue_rules"][0]["disallow_checks_interruption_from_queues"] = [
            "default"
        ]
        p_new_config = await self.create_pr(
            files={".mergify.yml": yaml.dump(updated_rules)}
        )
        await self.merge_pull(p_new_config["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p_new_config = await self.get_pull(p_new_config["number"])

        q = await self.get_train()

        # my 3 PRs + 2 merge queue PR
        pulls = await self.get_pulls()
        assert len(pulls) == 5

        tmp_mq_p1 = pulls[1]
        tmp_mq_p2 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert p_new_config["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_new_config["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Put second PR at the begining of the queue via pr priority Checks
        # must not be interrupted due to
        # disallow_checks_interruption_from_queues config
        await self.add_label(p3["number"], "high")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 6

        tmp_mq_p3 = pulls[0]
        assert tmp_mq_p3["number"] not in [p1["number"], p2["number"], p3["number"]]

        await self.assert_merge_queue_contents(
            q,
            p_new_config["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

    async def test_queue_priority(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                    "speculative_checks": 5,
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high urgent",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"queue": {"name": "urgent"}},
                },
                {
                    "name": "Merge priority high default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p_merged = await self.create_pr()
        await self.merge_pull(p_merged["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p_merged = await self.get_pull(p_merged["number"])

        # Put first PR in queue
        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        q = await self.get_train()

        # my 3 PRs + 2 merge queue PR
        pulls = await self.get_pulls()
        assert len(pulls) == 5

        tmp_mq_p1 = pulls[1]
        tmp_mq_p2 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert p_merged["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_merged["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        # Put third PR at the begining of the queue via queue priority
        await self.add_label(p3["number"], "queue-urgent")
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p3 = pulls[0]
        assert tmp_mq_p3["number"] not in [p1["number"], p2["number"], p3["number"]]

        # p3 is now the only car in train, as its queue is not the same as p1 and p2
        await self.assert_merge_queue_contents(
            q,
            p_merged["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
            [p1["number"], p2["number"]],
        )

        # Queue API with token
        r = await self.admin_app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
        )
        assert r.status_code == 200
        assert r.json() == {
            "queues": [
                {
                    "branch": {"name": self.main_branch_name},
                    "pull_requests": [
                        {
                            "number": p3["number"],
                            "position": 0,
                            "queued_at": anys.ANY_AWARE_DATETIME_STR,
                            "priority": 2000,
                            "queue_rule": {
                                "config": {
                                    "allow_inplace_checks": True,
                                    "allow_queue_branch_edit": False,
                                    "disallow_checks_interruption_from_queues": [],
                                    "batch_max_wait_time": 30.0,
                                    "batch_size": 1,
                                    "checks_timeout": None,
                                    "draft_bot_account": None,
                                    "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                    "queue_branch_merge_method": None,
                                    "priority": 1,
                                    "speculative_checks": 5,
                                    "batch_max_failure_resolution_attempts": None,
                                    "commit_message_template": None,
                                    "merge_method": "merge",
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                },
                                "name": "urgent",
                            },
                            "speculative_check_pull_request": anys.ANY_MAPPING,
                            "mergeability_check": {
                                "check_type": "draft_pr",
                                "pull_request_number": tmp_mq_p3["number"],
                                "started_at": anys.ANY_AWARE_DATETIME_STR,
                                "ended_at": None,
                                "state": "pending",
                            },
                            "estimated_time_of_merge": None,
                        },
                        {
                            "number": p1["number"],
                            "position": 1,
                            "priority": 2000,
                            "queue_rule": {
                                "config": {
                                    "allow_inplace_checks": True,
                                    "allow_queue_branch_edit": False,
                                    "disallow_checks_interruption_from_queues": [],
                                    "batch_max_wait_time": 30.0,
                                    "batch_size": 1,
                                    "checks_timeout": None,
                                    "draft_bot_account": None,
                                    "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                    "queue_branch_merge_method": None,
                                    "priority": 0,
                                    "speculative_checks": 5,
                                    "batch_max_failure_resolution_attempts": None,
                                    "commit_message_template": None,
                                    "merge_method": "merge",
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                },
                                "name": "default",
                            },
                            "queued_at": anys.ANY_AWARE_DATETIME_STR,
                            "speculative_check_pull_request": None,
                            "mergeability_check": None,
                            "estimated_time_of_merge": None,
                        },
                        {
                            "number": p2["number"],
                            "position": 2,
                            "priority": 2000,
                            "queue_rule": {
                                "config": {
                                    "allow_inplace_checks": True,
                                    "allow_queue_branch_edit": False,
                                    "disallow_checks_interruption_from_queues": [],
                                    "batch_size": 1,
                                    "batch_max_wait_time": 30.0,
                                    "checks_timeout": None,
                                    "draft_bot_account": None,
                                    "queue_branch_prefix": constants.MERGE_QUEUE_BRANCH_PREFIX,
                                    "queue_branch_merge_method": None,
                                    "priority": 0,
                                    "speculative_checks": 5,
                                    "batch_max_failure_resolution_attempts": None,
                                    "commit_message_template": None,
                                    "merge_method": "merge",
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                },
                                "name": "default",
                            },
                            "queued_at": anys.ANY_AWARE_DATETIME_STR,
                            "speculative_check_pull_request": None,
                            "mergeability_check": None,
                            "estimated_time_of_merge": None,
                        },
                    ],
                }
            ],
        }

        # Merge p3
        await self.create_status(tmp_mq_p3, context="continuous-integration/fast-ci")
        await self.run_engine()
        p3 = await self.get_pull(p3["number"])
        assert p3["merged"]

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        # ensure p1 and p2 are back in queue
        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p1_bis = pulls[1]
        tmp_mq_p2_bis = pulls[0]
        assert tmp_mq_p1_bis["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2_bis["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert p3["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p3["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p3["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1_bis["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p3["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2_bis["number"],
                ),
            ],
        )

    async def test_queue_no_tmp_pull_request(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge train",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr(as_="admin")
        await self.create_status(p1)
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        pulls_in_queue = await q.get_pulls()
        assert pulls_in_queue == []

        # pull merged without need of a train car
        p1 = await self.get_pull(p1["number"])
        assert p1["merged"]

    # FIXME(sileht): Provide a tools to generate oauth_token without
    # the need of the dashboard
    # @pytest.mark.skipif(
    #    config.GITHUB_URL != "https://github.com",
    #    reason="We use a PAT token instead of an OAUTH_TOKEN",
    # )
    # MRGFY-472 should fix that
    @pytest.mark.skip(
        reason="This test is not reliable, GitHub doeesn't always allow to create the tmp pr"
    )
    async def test_pull_have_base_branch_merged_commit_with_changed_workflow(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": True,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/ci.yml": TEMPLATE_GITHUB_ACTION % "echo Default CI"
            },
        )

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr(
            files={
                ".github/workflows/ci.yml": TEMPLATE_GITHUB_ACTION % "echo Changed CI"
            }
        )

        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        # Merge base branch into p2
        await self.client_integration.put(
            f"{self.url_origin}/pulls/{p2['number']}/update-branch",
            api_version="lydian",
            json={"expected_head_sha": p2["head"]["sha"]},
        )

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_mq_p1 = await self.get_pull(pulls[1]["number"])
        tmp_mq_p2 = await self.get_pull(pulls[0]["number"])
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"]]

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2["number"],
                ),
            ],
        )

        assert tmp_mq_p1["commits"] == 7
        assert tmp_mq_p1["changed_files"] == 1
        assert tmp_mq_p2["commits"] == 7
        assert tmp_mq_p2["changed_files"] == 5
        await self.create_status(tmp_mq_p2)

        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(tmp_mq_p1)
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_more_ci_in_pull_request_rules_succeed(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "status-success=very-long-ci",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.create_status(p1, context="very-long-ci")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        async def assert_queued() -> None:
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

        await assert_queued()
        await self.create_status(p1)
        await self.run_engine()

        await assert_queued()
        await self.create_status(p1, context="very-long-ci")
        await self.run_engine()

        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_more_ci_in_pull_request_rules_failure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "status-success=very-long-ci",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.create_status(p1, context="very-long-ci")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()

        # not merged and unqueued
        pulls = await self.get_pulls()
        assert len(pulls) == 1

        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_ci_timeout_inplace(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "checks_timeout": "10 m",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        with freeze_time("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config))

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.wait_for_pull_request("closed", p["number"])
            await self.run_full_engine()

            await self.create_status(p1)
            await self.run_full_engine()

            check_run = await self.wait_for_check_run(
                name="Rule: queue (queue)", action="created", status="in_progress"
            )
            assert (
                check_run["check_run"]["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )
            await self.wait_for_pull_request("synchronize", p1["number"])
            await self.run_full_engine()

            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh", "-inf", "+inf", withscores=True
            )
            assert len(pulls_to_refresh) == 1

        with freeze_time("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()

            # Check-runs arrive in random order, so we need to retrieve all of them and check
            # if the event we retrieved are the ones we expected.
            # There are 3 check_run events that should arrive:
            # - Summary
            # - Rule: queue (queue)
            # - Queue: Embarked in merge train
            check_runs = [
                await self.wait_for_check_run(status="completed", action="completed"),
                await self.wait_for_check_run(status="completed", action="completed"),
                await self.wait_for_check_run(status="completed", action="completed"),
            ]

            found_queue_rule = False
            found_embarked_train = False
            for check_run in check_runs:
                match check_run["check_run"]["name"]:
                    case "Rule: queue (queue)":
                        found_queue_rule = True
                        assert (
                            check_run["check_run"]["output"]["title"]
                            == "The pull request has been removed from the queue"
                        )
                        assert check_run["check_run"]["conclusion"] == "cancelled"
                    case "Queue: Embarked in merge train":
                        found_embarked_train = True
                        assert (
                            "checks have timed out"
                            in check_run["check_run"]["output"]["summary"]
                        )
                        assert check_run["check_run"]["conclusion"] == "failure"
                    case _:
                        continue

            if not found_queue_rule:
                raise AssertionError(
                    "Did not find check_run event for 'Rule: queue (queue)'"
                )
            if not found_embarked_train:
                raise AssertionError(
                    "Did not find check_run event for 'Queue: Embarked in merge train'"
                )

    async def test_queue_ci_timeout_inplace_with_only_pull_request_rules(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "checks_timeout": "10 m",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        with freeze_time("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config))

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.wait_for("pull_request", {"action": "closed"})
            await self.create_status(p1)
            await self.run_full_engine()

            check_run = await self.wait_for_check_run(
                name="Rule: queue (queue)", action="created", status="in_progress"
            )
            assert (
                check_run["check_run"]["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )
            await self.wait_for_pull_request("synchronize", p1["number"])
            await self.run_full_engine()

            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh", "-inf", "+inf", withscores=True
            )
            assert len(pulls_to_refresh) == 1

        with freeze_time("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()

            # Check-runs arrive in random order, so we need to retrieve all of them and check
            # if the event we retrieved are the ones we expected.
            # There are 3 check_run events that should arrive:
            # - Summary
            # - Rule: queue (queue)
            # - Queue: Embarked in merge train
            check_runs = [
                await self.wait_for_check_run(status="completed", action="completed"),
                await self.wait_for_check_run(status="completed", action="completed"),
                await self.wait_for_check_run(status="completed", action="completed"),
            ]

            found_queue_rule = False
            found_embarked_train = False
            for check_run in check_runs:
                match check_run["check_run"]["name"]:
                    case "Rule: queue (queue)":
                        found_queue_rule = True
                        assert (
                            check_run["check_run"]["output"]["title"]
                            == "The pull request has been removed from the queue"
                        )
                        assert check_run["check_run"]["conclusion"] == "cancelled"
                    case "Queue: Embarked in merge train":
                        found_embarked_train = True
                        assert (
                            "checks have timed out"
                            in check_run["check_run"]["output"]["summary"]
                        )
                        assert check_run["check_run"]["conclusion"] == "failure"
                    case _:
                        continue

            if not found_queue_rule:
                raise AssertionError(
                    "Did not find check_run event for 'Rule: queue (queue)'"
                )
            if not found_embarked_train:
                raise AssertionError(
                    "Did not find check_run event for 'Queue: Embarked in merge train'"
                )

    async def test_queue_ci_timeout_draft_pr(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "checks_timeout": "10 m",
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        with freeze_time("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config))

            p1 = await self.create_pr()

            await self.create_status(p1)
            await self.run_full_engine()

            await self.wait_for("pull_request", {"action": "opened"})
            await self.run_full_engine()

            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: queue (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )
            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh", "-inf", "+inf", withscores=True
            )
            assert len(pulls_to_refresh) == 1

        with freeze_time("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: queue (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request has been removed from the queue"
            )
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Queue: Embarked in merge train",
            )
            assert check is not None
            assert "checks have timed out" in check["output"]["summary"]

    async def test_queue_ci_timeout_outside_schedule(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        {
                            "or": [
                                "check-success=continuous-integration/fake-ci",
                                "check-success=continuous-integration/other-ci",
                            ]
                        },
                        "schedule: MON-FRI 08:00-17:00",
                    ],
                    "checks_timeout": "10 m",
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        with freeze_time("2021-05-30T20:00:00", tick=True):
            await self.setup_repo(yaml.dump(config))

            p1 = await self.create_pr()

            await self.create_status(p1)
            await self.run_full_engine()

            await self.wait_for("pull_request", {"action": "opened"})
            await self.run_full_engine()

            ctxt_p1 = context.Context(self.repository_ctxt, p1)
            check = await ctxt_p1.get_engine_check_run("Rule: queue (queue)")
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

            check = await ctxt_p1.get_engine_check_run(
                constants.MERGE_QUEUE_SUMMARY_NAME
            )
            assert check is not None
            assert (
                "The checks have to pass before 20:10 UTC" in check["output"]["summary"]
            )

            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh", "-inf", "+inf", withscores=True
            )
            assert len(pulls_to_refresh) == 1

        with freeze_time("2021-05-30T20:12:00", tick=True):
            await self.run_full_engine()
            ctxt_p1._caches.pull_check_runs.delete()
            check = await ctxt_p1.get_engine_check_run("Rule: queue (queue)")
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request has been removed from the queue"
            )
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
            )
            assert check is not None
            assert (
                "cannot be merged and has been disembarked" in check["output"]["title"]
            )
            assert "checks have timed out" in check["output"]["summary"]

    async def test_queue_ci_timeout_ignored_outside_schedule(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        {
                            "or": [
                                "check-success=continuous-integration/fake-ci",
                                "check-success=continuous-integration/other-ci",
                            ]
                        },
                        "schedule: MON-FRI 08:00-17:00",
                    ],
                    "checks_timeout": "10 m",
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        with freeze_time("2021-05-30T20:00:00", tick=True):
            await self.setup_repo(yaml.dump(config))

            p1 = await self.create_pr()

            await self.create_status(p1)
            await self.run_full_engine()

            tmp_pull = await self.wait_for_pull_request("opened")
            await self.run_full_engine()

            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: queue (queue)",
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )
            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh", "-inf", "+inf", withscores=True
            )
            assert len(pulls_to_refresh) == 1
            await self.create_status(tmp_pull["pull_request"])

        with freeze_time("2021-05-30T20:12:00", tick=True):
            await self.run_full_engine()
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: queue (queue)",
            )
            # Should not be unqueued as the CI succeed
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

    async def test_queue_with_default_config_branch_protection_only(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull_as_admin(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])
        time_to_merge_key = self.get_statistic_redis_key("time_to_merge")
        assert await self.redis_links.stats.xlen(time_to_merge_key) == 1

    async def _test_queue_without_branch_protection_for_queueing(
        self, rules: dict[str, typing.Any]
    ) -> None:
        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_linear_history": True,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull_as_admin(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        q = await self.get_train()
        assert p["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    # The 2 tests below this comment are the same, just the rules are differents.
    # This makes sures that the behavior is the same with the queue configuration in `queue_rules`
    # and in the `actions/queue`
    async def test_queue_without_branch_protection_for_queueing(self) -> None:
        await self._test_queue_without_branch_protection_for_queueing(
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "conditions": [],
                    }
                ],
                "pull_request_rules": [
                    {
                        "name": "Merge priority high",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "method": "squash",
                                "name": "default",
                                "require_branch_protection": False,
                            }
                        },
                    },
                ],
            }
        )

    async def test_queue_without_branch_protection_for_queueing_conf_in_queue_rules(
        self,
    ) -> None:
        await self._test_queue_without_branch_protection_for_queueing(
            {
                "queue_rules": [
                    {
                        "name": "default",
                        "conditions": [],
                        "merge_method": "squash",
                    }
                ],
                "pull_request_rules": [
                    {
                        "name": "Merge priority high",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "name": "default",
                                "require_branch_protection": False,
                            }
                        },
                    },
                ],
            }
        )

    async def test_queue_checks_and_branch(self) -> None:
        rules = f"""
queue_rules:
  - name: default
    conditions:
      - "check-success=Summary"
      - "check-success=ci/status"
      - "check-success=ci/service-test"
      - "check-success=ci/pipelines"
      - "#approved-reviews-by>=1"
      - "-label=flag:wait"

pull_request_rules:
  - name: merge
    conditions:
      - "-draft"
      - "-closed"
      - "-merged"
      - "-conflict"
      - "base={self.main_branch_name}"
      - "label=flag:merge"
    actions:
      queue:
        name: default
        priority: medium
        update_method: rebase
        require_branch_protection: false
"""
        await self.setup_repo(rules)

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "ci/status",
                    "ci/service-test",
                    "ci/pipelines",
                ],
            },
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        await self.branch_protection_protect(self.main_branch_name, protection)

        p = await self.create_pr(as_="fork")

        # To force others to be rebased
        p_other = await self.create_pr()
        await self.merge_pull_as_admin(p_other["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p_other = await self.get_pull(p_other["number"])

        await self.create_review(p["number"])
        await self.add_label(p["number"], "flag:merge")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        q = await self.get_train()
        assert p_other["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p_other["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p["number"]],
                    [],
                    p_other["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p["number"],
                ),
            ],
        )

        head_sha = p["head"]["sha"]
        p = await self.get_pull(p["number"])
        assert p["head"]["sha"] != head_sha  # ensure it have been rebased

        check = first(
            await context.Context(self.repository_ctxt, p).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p, "ci/status", state="pending")
        await self.run_engine()

        await self.create_status(p, "ci/status")
        await self.create_status(p, "ci/service-test")
        await self.run_engine()

        await self.create_status(p, "ci/pipelines")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_unexpected_config_change(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge me",
                    "conditions": ["label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        # Put a PR in queue
        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        # Change the name of the queue in the configuration
        updated_rules = {
            "queue_rules": [
                {
                    "name": "new_default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge me",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {"queue": {"name": "new_default"}},
                },
            ],
        }
        await self.push_file(".mergify.yml", yaml.dump(updated_rules))

        # Put a new PR in queue, with the new configuration
        p2 = await self.create_pr()
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        # The first PR has been removed from the queue, then queued again with
        # the new configuration
        p1 = await self.get_pull(p1["number"])
        ctxt = context.Context(self.repository_ctxt, p1)
        check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge me (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )
        assert (
            "Required conditions of queue** `new_default` **for merge"
            in check["output"]["summary"]
        )

    async def test_target_branch_vanished(self) -> None:
        featureA = self.get_full_branch_name("featureA")
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={featureA}",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), test_branches=[featureA])

        await self.create_pr(base=featureA)
        await self.create_pr(base=featureA)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "opened"})

        queues = [
            q
            async for q in merge_train.Train.iter_trains(
                self.repository_ctxt, await self.get_queue_rules()
            )
        ]
        assert len(queues) == 1
        assert len(await queues[0].get_pulls()) == 2

        await self.client_integration.delete(
            f"{self.url_origin}/git/refs/heads/{parse.quote(featureA)}"
        )
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        queues = [
            q
            async for q in merge_train.Train.iter_trains(
                self.repository_ctxt, await self.get_queue_rules()
            )
        ]
        assert len(queues) == 0

    async def test_queue_with_two_pull_request_rules_that_match_then_unmatch(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
                {
                    "name": "Merge priority low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=low",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "low"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()

        await self.add_label(p1["number"], "high")
        await self.add_label(p1["number"], "low")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
            ],
        )
        await self.remove_label(p1["number"], "high")
        await self.run_engine()

        tmp_pull_2 = await self.wait_for_pull_request("opened")

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p1["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p1["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )


class TestTrainApiCalls(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_create_pull_basic(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "foo",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(config))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        ctxt = context.Context(self.repository_ctxt, p1)
        q = await self.get_train()
        base_sha = await q.get_base_sha()

        queue_config = qr_config.QueueConfig(
            priority=0,
            speculative_checks=5,
            batch_size=1,
            batch_max_wait_time=datetime.timedelta(seconds=0),
            allow_inplace_checks=True,
            disallow_checks_interruption_from_queues=[],
            allow_queue_branch_edit=False,
            checks_timeout=None,
            draft_bot_account=None,
            queue_branch_prefix=constants.MERGE_QUEUE_BRANCH_PREFIX,
            queue_branch_merge_method=None,
            batch_max_failure_resolution_attempts=None,
            commit_message_template=None,
            merge_method="merge",
            merge_bot_account=None,
            update_method=None,
            update_bot_account=None,
        )
        pull_queue_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
        )

        car = merge_train.TrainCar(
            q,
            merge_train.TrainCarState(),
            [
                merge_train.EmbarkedPull(
                    q, p2["number"], pull_queue_config, date.utcnow()
                )
            ],
            [
                merge_train.EmbarkedPull(
                    q, p2["number"], pull_queue_config, date.utcnow()
                )
            ],
            [p1["number"]],
            base_sha,
        )
        q._cars.append(car)

        q.queue_rules = qr_config.QueueRules(
            [
                qr_config.QueueRule(
                    name=qr_config.QueueName("foo"),
                    merge_conditions=conditions.QueueRuleMergeConditions([]),
                    routing_conditions=conditions.QueueRuleMergeConditions([]),
                    config=queue_config,
                    priority_rules=pr_config.PriorityRules([]),
                )
            ]
        )
        await car.start_checking_with_draft(None)
        assert car.queue_pull_request_number is not None
        pulls = await self.get_pulls()
        assert len(pulls) == 3

        tmp_pull = [p for p in pulls if p["number"] == car.queue_pull_request_number][0]
        assert tmp_pull["draft"]
        assert tmp_pull["body"] is not None
        assert (
            f"""
---
pull_requests:
  - number: {p2["number"]}
...
"""
            in tmp_pull["body"]
        )

        pull_url_prefix = f"/{self.installation_ctxt.owner_login}/{self.repository_ctxt.repo['name']}/pull"
        expected_table = f"| 1 | test_create_pull_basic: pull request n2 from integration ([#{p2['number']}]({pull_url_prefix}/{p2['number']})) | foo/0 | [#{tmp_pull['number']}]({pull_url_prefix}/{tmp_pull['number']}) | <fake_pretty_datetime()>|"
        assert expected_table in await car.generate_merge_queue_summary()

        await car.send_checks_end_signal(
            p2["number"], queue_utils.ChecksFailed(), "REEMBARKED"
        )
        await car.end_checking(
            reason=queue_utils.ChecksFailed(), not_reembarked_pull_requests={}
        )

        ctxt = context.Context(self.repository_ctxt, tmp_pull)
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["conclusion"] == "cancelled"
        assert str(queue_utils.ChecksFailed()) in summary["output"]["summary"]

        pulls = await self.get_pulls()
        assert len(pulls) == 2

        failure_by_reason_key = self.get_statistic_redis_key("failure_by_reason")
        assert await self.redis_links.stats.xlen(failure_by_reason_key) == 1

        await self.assert_eventlog_check_end("REEMBARKED")

    async def assert_eventlog_check_end(
        self, abort_status: typing.Literal["DEFINITIVE", "REEMBARKED"]
    ) -> None:
        redis_repo_key = eventlogs._get_repository_key(
            self.subscription.owner_id, self.RECORD_CONFIG["repository_id"]
        )
        bdata = await self.redis_links.eventlogs.xrange(redis_repo_key)
        events = [msgpack.unpackb(raw[b"data"]) for _, raw in bdata]
        check_end_events = [
            e for e in events if e["event"] == "action.queue.checks_end"
        ]

        assert len(check_end_events) == 1

        event = check_end_events[0]
        assert event["metadata"]["abort_status"] == abort_status

    async def test_create_pull_after_failure(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "foo",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(config))

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        q = await self.get_train()
        base_sha = await q.get_base_sha()

        queue_config = qr_config.QueueConfig(
            priority=0,
            speculative_checks=5,
            batch_size=1,
            batch_max_wait_time=datetime.timedelta(seconds=0),
            allow_inplace_checks=True,
            disallow_checks_interruption_from_queues=[],
            allow_queue_branch_edit=False,
            checks_timeout=None,
            draft_bot_account=None,
            queue_branch_prefix=constants.MERGE_QUEUE_BRANCH_PREFIX,
            queue_branch_merge_method=None,
            batch_max_failure_resolution_attempts=None,
            commit_message_template=None,
            merge_method="merge",
            merge_bot_account=None,
            update_method=None,
            update_bot_account=None,
        )
        queue_pull_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
        )

        car = merge_train.TrainCar(
            q,
            merge_train.TrainCarState(),
            [
                merge_train.EmbarkedPull(
                    q, p2["number"], queue_pull_config, date.utcnow()
                )
            ],
            [
                merge_train.EmbarkedPull(
                    q, p2["number"], queue_pull_config, date.utcnow()
                )
            ],
            [p1["number"]],
            base_sha,
        )
        q._cars.append(car)

        q.queue_rules = qr_config.QueueRules(
            [
                qr_config.QueueRule(
                    name=qr_config.QueueName("foo"),
                    merge_conditions=conditions.QueueRuleMergeConditions([]),
                    routing_conditions=conditions.QueueRuleMergeConditions([]),
                    config=queue_config,
                    priority_rules=pr_config.PriorityRules([]),
                )
            ]
        )
        await car.start_checking_with_draft(None)
        assert car.queue_pull_request_number is not None
        pulls = await self.get_pulls()
        assert len(pulls) == 3

        tmp_pull = [p for p in pulls if p["number"] == car.queue_pull_request_number][0]
        assert tmp_pull["draft"]
        assert car.queue_branch_name is not None

        # Ensure pull request is closed and re-created
        await car.start_checking_with_draft(None)
        assert car.queue_pull_request_number is not None
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "opened"})
        pulls = await self.get_pulls()
        assert len(pulls) == 3

        tmp_pull = [p for p in pulls if p["number"] == car.queue_pull_request_number][0]
        assert tmp_pull["draft"]

    async def test_failed_draft_pr_auto_cleanup(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "foo",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "foo"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(config))

        p = await self.create_pr()

        q = await self.get_train()
        base_sha = await q.get_base_sha()

        queue_config = qr_config.QueueConfig(
            priority=0,
            speculative_checks=5,
            batch_size=1,
            batch_max_wait_time=datetime.timedelta(seconds=0),
            allow_inplace_checks=True,
            disallow_checks_interruption_from_queues=[],
            allow_queue_branch_edit=False,
            checks_timeout=None,
            draft_bot_account=None,
            queue_branch_prefix=constants.MERGE_QUEUE_BRANCH_PREFIX,
            queue_branch_merge_method=None,
            batch_max_failure_resolution_attempts=None,
            commit_message_template=None,
            merge_method="merge",
            merge_bot_account=None,
            update_method=None,
            update_bot_account=None,
        )
        queue_pull_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
        )

        embarked_pulls = [
            merge_train.EmbarkedPull(q, p["number"], queue_pull_config, date.utcnow())
        ]
        car = merge_train.TrainCar(
            q,
            merge_train.TrainCarState(),
            embarked_pulls,
            embarked_pulls,
            [],
            base_sha,
        )
        q._cars.append(car)

        q.queue_rules = qr_config.QueueRules(
            [
                qr_config.QueueRule(
                    name=qr_config.QueueName("foo"),
                    merge_conditions=conditions.QueueRuleMergeConditions([]),
                    routing_conditions=conditions.QueueRuleMergeConditions([]),
                    config=queue_config,
                    priority_rules=pr_config.PriorityRules([]),
                )
            ]
        )
        await car.start_checking_with_draft(None)
        assert car.queue_pull_request_number is not None
        pulls = await self.get_pulls()
        assert len(pulls) == 2

        # NOTE(sileht): We don't save the merge train in Redis on purpose, so next
        # engine run should delete merge queue branch of draft PR not tied to a
        # TrainCar
        draft_pr = await self.wait_for_pull_request("opened")
        await self.run_engine()
        await self.wait_for(
            "pull_request", {"action": "closed", "number": draft_pr["number"]}
        )

    async def test_create_pull_conflicts(self) -> None:
        await self.setup_repo(yaml.dump({}), files={"conflicts": "foobar"})

        p = await self.create_pr(files={"conflicts": "well"})
        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr(files={"conflicts": "boom"})

        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})

        q = await self.get_train()
        base_sha = await q.get_base_sha()

        queue_config = qr_config.QueueConfig(
            priority=0,
            speculative_checks=5,
            batch_size=1,
            batch_max_wait_time=datetime.timedelta(seconds=0),
            allow_inplace_checks=True,
            disallow_checks_interruption_from_queues=[],
            allow_queue_branch_edit=False,
            checks_timeout=None,
            draft_bot_account=None,
            queue_branch_prefix=constants.MERGE_QUEUE_BRANCH_PREFIX,
            queue_branch_merge_method=None,
            batch_max_failure_resolution_attempts=None,
            commit_message_template=None,
            merge_method="merge",
            merge_bot_account=None,
            update_method=None,
            update_bot_account=None,
        )
        config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
        )

        car = merge_train.TrainCar(
            q,
            merge_train.TrainCarState(),
            [merge_train.EmbarkedPull(q, p3["number"], config, date.utcnow())],
            [merge_train.EmbarkedPull(q, p3["number"], config, date.utcnow())],
            [p1["number"], p2["number"]],
            base_sha,
        )

        q.queue_rules = qr_config.QueueRules(
            [
                qr_config.QueueRule(
                    name=qr_config.QueueName("foo"),
                    merge_conditions=conditions.QueueRuleMergeConditions([]),
                    routing_conditions=conditions.QueueRuleMergeConditions([]),
                    config=queue_config,
                    priority_rules=pr_config.PriorityRules([]),
                )
            ]
        )

        with pytest.raises(merge_train.TrainCarPullRequestCreationFailure) as exc_info:
            await car.start_checking_with_draft(None)
        assert exc_info.value.car == car
        assert car.queue_pull_request_number is None

        p3 = await self.get_pull(p3["number"])
        ctxt_p3 = context.Context(self.repository_ctxt, p3)
        check = first(
            await ctxt_p3.pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert check is not None
        assert (
            check["output"]["title"] == "This pull request cannot be embarked for merge"
        )
        assert check["output"]["summary"] == (
            "The merge queue pull request can't be created\n"
            f"Details:\n> The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}, #{p2['number']}\n"
        )

    async def test_train_car_state_waiting_for_schedule_after_pr_ahead_dequeued(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "schedule=MON-FRI 08:00-17:00[UTC]",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                    "batch_size": 3,
                    "batch_max_wait_time": "0 s",
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default", "priority": "high"}},
                },
            ],
        }
        # Friday, 18:00 UTC
        start_date = datetime.datetime(2022, 11, 4, 18, tzinfo=datetime.timezone.utc)

        with freeze_time(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules))

            p1 = await self.create_pr()
            p2 = await self.create_pr(two_commits=True)
            p3 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            p_closed = await self.wait_for_pull_request("closed", pr_number=p["number"])
            await self.run_engine()

            await self.add_label(p1["number"], "queue")
            await self.add_label(p2["number"], "queue")
            await self.add_label(p3["number"], "queue")
            await self.run_engine()

            tmp_pull_1 = await self.wait_for_pull_request("opened")
            await self.run_full_engine()

            q = await self.get_train()
            assert p_closed["pull_request"]["merge_commit_sha"] is not None
            await self.assert_merge_queue_contents(
                q,
                p_closed["pull_request"]["merge_commit_sha"],
                [
                    base.MergeQueueCarMatcher(
                        [p1["number"], p2["number"], p3["number"]],
                        [],
                        p_closed["pull_request"]["merge_commit_sha"],
                        merge_train.TrainCarChecksType.DRAFT,
                        tmp_pull_1["number"],
                    ),
                ],
            )

            await self.remove_label(p1["number"], "queue")
            await self.run_engine()
            await self.wait_for("pull_request", {"action": "closed"})

            # If the seconds_waiting_for_schedule computation failed, the new pull
            # request with the sliced car will not be opened
            await self.run_engine()
            await self.wait_for_pull_request("opened")

    async def test_batch_max_failure_resolution_attempts(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 2,
                    "batch_size": 3,
                    "allow_inplace_checks": False,
                    "batch_max_wait_time": "0 s",
                    "batch_max_failure_resolution_attempts": 0,
                }
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

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"], state="failure")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Automatic merge (queue)", conclusion="cancelled"
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "The pull request has been removed from the queue"
        )
        assert check_run["check_run"]["output"]["summary"].startswith(
            "The maximum batch failure resolution attempts has been reached."
        )
