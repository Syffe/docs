import copy
import datetime
import re
import typing
from unittest import mock
from urllib import parse

import anys
from first import first
import pytest
import respx
import sqlalchemy
from sqlalchemy import func

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import database
from mergify_engine import date
from mergify_engine import engine
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import utils
from mergify_engine import yaml
from mergify_engine.actions import merge_base
from mergify_engine.engine import actions_runner
from mergify_engine.engine import commands_runner
from mergify_engine.models import events as evt_models
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils
from mergify_engine.tests.tardis import time_travel


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


# NOTE(Syffe): The tests contained in this test file are using
# the keyword "conditions" instead of "merge_conditions" for queue_rules config
# in order to keep the backward compatibility tested. They should not be changed
# until we totally deprecate the "conditions" keyword.
class TestQueueAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_pr_with_depends_on_unqueued_after_dependent_pr_failed_checks_with_speculative_checks(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                    "allow_inplace_checks": False,
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
        body = f"Awesome body\nDepends-On: #{p1['number']}\n"
        p2 = await self.create_pr(message=body)

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        await self.wait_for_pull_request("opened")

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        await self.create_status(tmp_pull_1["pull_request"], state="failure")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            f"Pull request #{p2['number']} has been dequeued"
            == check["output"]["title"]
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert (
            f"The following conditions don't match anymore:\n- [ ] `depends-on=#{p1['number']}`"
            in check["output"]["summary"]
        )

    async def test_pr_with_depends_on_unqueued_after_dependent_pr_failed_to_merge_no_inplace(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr()
        body = f"Awesome body\nDepends-On: #{p1['number']}\n"
        p2 = await self.create_pr(message=body)

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        tmp_pull_1 = await self.wait_for_pull_request("opened")

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        await self.create_status(tmp_pull_1["pull_request"], state="failure")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            f"Pull request #{p2['number']} has been dequeued"
            == check["output"]["title"]
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert (
            f"The following conditions don't match anymore:\n- [ ] `depends-on=#{p1['number']}`"
            in check["output"]["summary"]
        )

    async def test_pr_with_depends_on_unqueued_after_dependent_pr_failed_to_merge_inplace(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr()
        body = f"Awesome body\nDepends-On: #{p1['number']}\n"
        p2 = await self.create_pr(message=body)

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
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

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        await self.create_status(p1, state="failure")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
        )
        assert check is not None
        assert (
            f"Pull request #{p2['number']} has been dequeued"
            == check["output"]["title"]
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert (
            f"The following conditions don't match anymore:\n- [ ] `depends-on=#{p1['number']}`"
            in check["output"]["summary"]
        )

    async def test_queue_dequeue_reason_condition(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
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
                {
                    "name": "Label dequeued PR",
                    "conditions": ["queue-dequeue-reason=checks-failed"],
                    "actions": {
                        "label": {
                            "add": ["dequeued"],
                            "remove": ["queue"],
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        pr = await self.create_pr()
        await self.add_label(pr["number"], "queue")
        await self.run_engine()

        await self.wait_for_check_run(name="Rule: Queue (queue)", status="in_progress")
        await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            status="in_progress",
        )

        # disembark pr
        await self.create_status(pr, state="failure")
        await self.run_engine()
        await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="failure",
        )
        check = await self.wait_for_check_run(
            name="Rule: Queue (queue)",
            conclusion="cancelled",
        )
        assert check is not None
        assert (
            check["check_run"]["output"]["title"]
            == "The pull request has been removed from the queue `default`"
        )

        # pr labeled after dequeue condition matches
        await self.run_engine()
        pr_labeled = await self.wait_for_pull_request("labeled")
        assert pr_labeled is not None
        assert "dequeued" in [
            label["name"] for label in pr_labeled["pull_request"]["labels"]
        ]

    async def test_pr_with_depends_on_not_queued_if_dependent_pr_is_not_queued(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr()
        body = f"Awesome body\nDepends-On: #{p1['number']}\n"
        p2 = await self.create_pr(message=body)

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is None

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.create_comment_as_admin(p2["number"], "@mergifyio refresh")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

    async def test_pr_queued_with_depends_on_then_merged_after_first_pr_merged(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr()
        body = f"Awesome body\nDepends-On: #{p1['number']}\n"
        p2 = await self.create_pr(message=body)

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        await self.create_status(p2)
        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )
        await self.create_status(p1)
        await self.run_engine()

        p1_merged = await self.wait_for_pull_request("closed")
        assert p1_merged["pull_request"]["merged"]

        await self.wait_for_pull_request("synchronize")
        p2_updated = await self.get_pull(p2["number"])

        await self.create_status(p2_updated)
        await self.run_engine()

        p2_merged = await self.wait_for_pull_request("closed")
        assert p2_merged["pull_request"]["merged"]

    async def test_queue_conditions_matching_with_pull_request_rules(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_conditions_failure_with_pull_request_rules_and_no_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        "label=toto",
                        "label=tata",
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check = await self.wait_for_check_run(name="Summary", conclusion="success")
        assert check["check_run"]["output"]["title"] == "1 potential rule"
        assert """- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `label=tata`
    - [ ] `label=toto`
  - [ ] all of: [:pushpin: queue conditions of queue `hotfix`]
    - [ ] `label=hotfix`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] `label=queue`""" in check["check_run"]["output"]["summary"]

    async def test_queue_conditions_failure_with_pull_request_rules_and_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_conditions_success(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "files~=^test/",
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
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_conditions_failure(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "files~=^dummy/",
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
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(files={"test/toto.txt": "toto"})
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check = await self.wait_for_check_run(name="Summary")
        assert check["check_run"]["output"]["title"] == "1 potential rule"
        expected_summary = f"""- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `files~=^dummy/`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] `base={p1['base']['ref']}`
- [X] `label=queue`"""
        assert expected_summary in check["check_run"]["output"]["summary"]

    async def test_queue_conditions_failure_update_after_queued(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": ["label=routing"],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        assert check["conclusion"] == "cancelled"
        assert (
            check["output"]["title"]
            == "The pull request has been removed from the queue `default`"
        )
        assert (
            "The queue conditions cannot be satisfied due to failing checks."
            in check["output"]["summary"]
        )

    async def test_queue_conditions_empty(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_reporting_when_queue_action_cancelled(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": ["label=hotfix"],
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        "check-success=check",
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        # p1 is rebased and merged after p2
        p1 = await self.create_pr(files={"test/first.txt": "1"})
        await self.add_label(p1["number"], "queue")
        await self.create_check_run(
            p1,
            name="check",
            conclusion="success",
        )

        # p2 gets queued and merged first in hotfix
        p2 = await self.create_pr(files={"test/second.txt": "2"})
        await self.add_label(p2["number"], "queue")
        await self.add_label(p2["number"], "hotfix")

        await self.run_engine()

        # p2 merged and p1 rebased
        p2_merged = await self.wait_for_pull_request("closed", p2["number"], True)
        p1_rebased = await self.get_pull(p1["number"])
        assert (
            p1_rebased["base"]["sha"] == p2_merged["pull_request"]["merge_commit_sha"]
        )

        # check back after rebase
        await self.create_check_run(
            p1_rebased,
            name="check",
            conclusion="failure",
            external_id=check_api.USER_CREATED_CHECKS,
        )

        await self.run_engine()

        check_run = await self.wait_for_check_run(
            conclusion="cancelled",
            name="Rule: Queue (queue)",
        )
        assert check_run is not None
        assert check_run["check_run"]["head_sha"] == p1_rebased["head"]["sha"]
        assert (
            check_run["check_run"]["output"]["title"]
            == "The pull request has been removed from the queue `default`"
        )

    async def test_queue_conditions_checks_still_queued_after_rebase(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": ["label=hotfix"],
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        "check-success=check",
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        # p1 is rebased and merged after p2
        p1 = await self.create_pr(files={"test/first.txt": "1"})
        await self.add_label(p1["number"], "queue")
        await self.create_check_run(
            p1,
            name="check",
            conclusion="success",
        )

        # p2 gets queued and merged first in hotfix
        p2 = await self.create_pr(files={"test/second.txt": "2"})
        await self.add_label(p2["number"], "queue")
        await self.add_label(p2["number"], "hotfix")

        await self.run_engine()

        # p2 merged and p1 rebased
        p2_merged = await self.wait_for_pull_request("closed", p2["number"], True)
        p1_rebased = await self.get_pull(p1["number"])
        assert (
            p1_rebased["base"]["sha"] == p2_merged["pull_request"]["merge_commit_sha"]
        )

        # check back after rebase
        await self.create_check_run(
            p1_rebased,
            name="check",
            conclusion="success",
            external_id=check_api.USER_CREATED_CHECKS,
        )

        await self.run_engine()

        check_run = await self.wait_for_check_run(
            conclusion="success",
            name="Rule: Queue (queue)",
        )
        assert check_run is not None
        assert check_run["check_run"]["head_sha"] == p1_rebased["head"]["sha"]

    async def test_queue_conditions_matching_with_pull_request_rules_and_queue_forced(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_conditions_with_pending_check_runs(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "pending-check-success",
                    "queue_conditions": [
                        "check-success=some-pending-check-in-ci",
                    ],
                },
                {
                    "name": "default",
                    "queue_conditions": ["label=will-not-be-set"],
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        # run engine with pending check
        p = await self.create_pr()
        await self.add_label(p["number"], "queue")
        pending_check = await self.create_check_run(
            p,
            name="some-pending-check-in-ci",
            conclusion=None,
            external_id=check_api.USER_CREATED_CHECKS,
        )
        await self.run_engine()

        check = await self.wait_for_check_run(name="Summary", conclusion="success")
        assert check["check_run"]["output"]["title"] == "1 potential rule"
        assert """- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `label=will-not-be-set`
  - [ ] all of: [:pushpin: queue conditions of queue `pending-check-success`]
    - [ ] `check-success=some-pending-check-in-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] `label=queue`""" in check["check_run"]["output"]["summary"]

        # run engine with check in success -> pull request is queued
        await self.update_check_run(
            p,
            pending_check["check_run"]["id"],
        )
        await self.run_engine()
        check = await self.wait_for_check_run(
            name="Rule: Queue (queue)",
            conclusion="success",
        )
        assert check is not None

    async def test_checks_inject_queue_conditions_in_merge_conditions(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "allow_inplace_checks": False,
                    "queue_conditions": [
                        "status-success=continuous-integration/fake-ci-queue",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci-merge",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p = await self.create_pr()
        await self.create_status(p, "continuous-integration/fake-ci-queue")
        await self.run_engine()
        draft_pr = await self.wait_for_pull_request("opened")

        check_summary = first(
            await context.Context(self.repository_ctxt, p).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue (queue)",
        )
        assert check_summary is not None
        assert (
            f"""
- `status-success=continuous-integration/fake-ci-queue`
  - [X] #{p["number"]}
"""
            in check_summary["output"]["summary"]
        )
        assert (
            "[ ] `status-success=continuous-integration/fake-ci-merge`"
            in check_summary["output"]["summary"]
        )

        await self.create_status(
            draft_pr["pull_request"],
            "continuous-integration/fake-ci-merge",
        )
        await self.run_engine()
        await self.wait_for_pull_request("closed", draft_pr["number"])
        await self.wait_for_pull_request("closed", p["number"], merged=True)

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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge me",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                },
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
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        p = await self.get_pull(p["number"])
        check = first(
            await context.Context(self.repository_ctxt, p).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge me (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert check["output"]["summary"] == "This action has been cancelled."
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
                },
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

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force p1 to be rebased
        p2 = await self.create_pr()
        p2_merged = await self.merge_pull_as_admin(p2["number"])
        await self.run_engine()

        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})

        q = await self.get_train()
        # base sha should have been updated
        assert p2_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        new_sha = await self.push_file("whatever", "content")
        await self.run_engine()

        await q.test_helper_load_from_redis()
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

    async def test_bypass_branch_protections_with_queue_injection_mode(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [],
                    "branch_protection_injection_mode": "queue",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_conversation_resolution": True,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        summary = await self.wait_for_check_run(name="Summary")
        assert """Rule: Merge default (queue)
- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
    - [X] `#review-threads-unresolved=0` [🛡 GitHub branch protection]""" in summary[
            "check_run"
        ]["output"]["summary"]

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            None,
            [],
        )

        await self.create_status(p1, context="continuous-integration/fake-ci")
        await self.run_engine()

        check_run = await self.wait_for_check_run(name="Rule: Merge default (queue)")
        assert """**Required conditions of queue** `default` **for merge:**

- [ ] `#review-threads-unresolved=0` [🛡 GitHub branch protection]
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`""" in check_run["check_run"][
            "output"
        ]["summary"]

        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        check = first(
            await context.Context(
                self.repository_ctxt,
                p1_merged["pull_request"],
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )

    async def test_bypass_branch_protections_with_merge_injection_mode(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [],
                    "branch_protection_injection_mode": "merge",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_conversation_resolution": True,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(name="Rule: Merge default (queue)")
        assert """**Required conditions of queue** `default` **for merge:**

- [ ] `#review-threads-unresolved=0` [🛡 GitHub branch protection]
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`""" in check_run["check_run"][
            "output"
        ]["summary"]

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

        await self.create_status(p1, context="continuous-integration/fake-ci")
        await self.run_engine()

        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        check = first(
            await context.Context(
                self.repository_ctxt,
                p1_merged["pull_request"],
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )

    async def test_bypass_branch_protections_with_none_injection_mode(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [],
                    "branch_protection_injection_mode": "none",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_bot_account": "mergify-test1",
                        },
                    },
                },
            ],
        }

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_conversation_resolution": True,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(name="Rule: Merge default (queue)")
        assert """**Required conditions of queue** `default` **for merge:**

- [ ] `#review-threads-unresolved=0` [🛡 GitHub branch protection]
- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`""" not in check_run["check_run"][
            "output"
        ]["summary"]

        p1_merged = await self.wait_for_pull_request("closed", p1["number"])
        check = first(
            await context.Context(
                self.repository_ctxt,
                p1_merged["pull_request"],
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge default (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "success"
        assert (
            check["output"]["title"] == "The pull request has been merged automatically"
        )

    async def test_error_bypass_branch_protections_with_none_injection_mode_without_bot_account(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [],
                    "branch_protection_injection_mode": "none",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge default",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_conversation_resolution": True,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(name="Rule: Merge default (queue)")
        assert (
            "Cannot use `branch_protection_injection_mode` set to `none` without using a `merge_bot_account` with the `queue` action"
            in check_run["check_run"]["output"]["summary"]
        )

    async def test_queue_inplace_interrupted(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "require_branch_protection": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [f"base={self.main_branch_name}", "label=queue"],
                    "actions": {
                        "queue": {
                            "name": "default",
                        },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        summary = await self.wait_for_check_run(name="Summary")
        assert (
            actions_runner.REQUIRE_BRANCH_PROTECTION_DEPRECATION_SAAS
            in summary["check_run"]["output"]["summary"]
        )

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
        p2_merged = await self.merge_pull_as_admin(p2["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        await self.wait_for_pull_request("synchronize", p1["number"])

        q = await self.get_train()
        # base sha should have been updated
        assert p2_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p2_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        # To force p1 to be rebased a second time
        p3 = await self.create_pr()
        p3_merged = await self.merge_pull_as_admin(p3["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        await self.wait_for_pull_request("synchronize", p1["number"])

        q = await self.get_train()
        # base sha should have been updated again
        assert p3_merged["pull_request"]["merge_commit_sha"] is not None
        await self.assert_merge_queue_contents(
            q,
            p3_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p3_merged["pull_request"]["merge_commit_sha"],
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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
                key=lambda c: c["name"] == "Rule: default merge (queue)",
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.run_engine()

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge fast-forward",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_method": "fast-forward",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(as_="admin")
        p2 = await self.create_pr(as_="admin", two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        pulls = await self.get_pulls()
        assert len(pulls) == 2

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
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
            [p2["number"]],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge fast-forward (queue)",
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
                self.repository_ctxt,
                p1_merged["pull_request"],
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge fast-forward (queue)",
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
                f"{self.url_origin}/branches/{self.main_branch_name}",
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
                ),
            ],
        )

        head_sha = p2["head"]["sha"]
        p2 = await self.get_pull(p2["number"])
        assert p2["head"]["sha"] != head_sha  # ensure it has been rebased

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge fast-forward (queue)",
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
                self.repository_ctxt,
                p2_merged["pull_request"],
            ).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge fast-forward (queue)",
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
                f"{self.url_origin}/branches/{self.main_branch_name}",
            ),
        )
        assert p2_merged["pull_request"]["head"]["sha"] == branch["commit"]["sha"]
        await self.assert_merge_queue_contents(q, None, [])

    # NOTE(charly): we don't check log errors for missing TrainCarState. The
    # test volontarily mock GitHub to return an error, so the engine retries the
    # merge with a limit of retries. In a normal situation, the merge conflict
    # would be detected when the TrainCar is created and checked, and we
    # shouldn't hit this limit of retries.
    @pytest.mark.ignored_logging_errors(
        "Merge queue check doesn't contain any TrainCarState",
        "failed to merge after 15 refresh attempts",
    )
    async def test_queue_fast_forward_with_retries(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge fast-forward",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "merge_method": "fast-forward",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(as_="admin")

        # Forces a rebase of p1
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        # Fulfill conditions to queue
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("synchronize", p1["number"])
        pulls = await self.get_pulls()
        assert len(pulls) == 1

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
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        # p1 has been rebased
        assert p1["head"]["sha"] != head_sha

        await self.run_engine()
        check = await self.wait_for_check_run(name="Rule: merge fast-forward (queue)")
        assert (
            check["check_run"]["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        # Fulfill merge_conditions
        await self.create_status(p1)

        # GitHub fast-forward is failing unexpectedly
        with respx.mock(assert_all_called=False) as respx_mock:
            respx_mock.put(
                f"{settings.GITHUB_REST_API_URL}"
                f"/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}"
                f"/git/refs/heads/{self.main_branch_name}",
            ).respond(405, json={"message": "Base branch was modified"})
            respx_mock.route(host="api.github.com").pass_through()
            respx_mock.route(
                url__startswith=settings.TESTING_FORWARDER_ENDPOINT,
            ).pass_through()

            await self.run_engine()

        await self.wait_for_check_run(name="Queue: Embarked in merge queue")
        pr_disembarked_check = await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="failure",
        )
        assert pr_disembarked_check is not None
        assert (
            pr_disembarked_check["check_run"]["output"]["title"]
            == f"The pull request {p1['number']} cannot be merged and has been disembarked"
        )
        assert pr_disembarked_check["check_run"]["output"]["summary"] == (
            "Mergify failed to merge the pull request\n"
            "GitHub can't merge the pull request after 15 retries.\n"
            "Base branch was modified in the meantime"
        )

        merge_check = await self.wait_for_check_run(
            name="Rule: merge fast-forward (queue)",
            conclusion="cancelled",
        )
        assert merge_check is not None
        assert (
            merge_check["check_run"]["output"]["title"]
            == "Mergify failed to merge the pull request"
        )
        assert merge_check["check_run"]["output"]["summary"] == (
            "GitHub can't merge the pull request after 15 retries.\n"
            "Base branch was modified in the meantime"
        )

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
                            ],
                        },
                    ],
                    "allow_inplace_checks": False,
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
            key=lambda c: c["name"] == "Queue: Embarked in merge queue",
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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        await q.test_helper_load_from_redis()
        await self.assert_merge_queue_contents(q, None, [])

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}&per_page=5",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "type": "action.merge",
                    "metadata": {"branch": self.main_branch_name},
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.leave",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "merged": False,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Merge",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.checks_end",
                    "metadata": {
                        "abort_code": "PR_DEQUEUED",
                        "abort_reason": f"Pull request #{p1['number']} has been "
                        "dequeued. Pull request automatically "
                        "merged by a `merge` action",
                        "abort_status": "DEFINITIVE",
                        "aborted": True,
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "failure",
                            "checks_ended_at": None,
                            "checks_started_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                            "unsuccessful_checks": [],
                        },
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.checks_start",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "start_reason": f"First time checking pull request #{p1['number']}",
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_started_at": None,
                            "checks_ended_at": None,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                            "unsuccessful_checks": [],
                        },
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.enter",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Queue",
                },
            ],
            "per_page": 5,
            "size": 5,
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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
                key=lambda c: c["name"] == "Rule: default merge (queue)",
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
        self,
        rules: dict[str, typing.Any],
    ) -> None:
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        assert p_merged["pull_request"]["merge_commit_sha"] is not None
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
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
                    },
                ],
                "pull_request_rules": [
                    {
                        "name": "default merge",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "name": "default",
                                "update_method": "rebase",
                                "update_bot_account": '{% if not author or author.endswith("[bot]") -%}\nmergify-test4\n{% else -%}\n{{ author }}\n{% endif -%}',
                            },
                        },
                    },
                ],
            },
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
                        "update_bot_account": '{% if not author or author.endswith("[bot]") -%}\nmergify-test4\n{% else -%}\n{{ author }}\n{% endif -%}',
                    },
                ],
                "pull_request_rules": [
                    {
                        "name": "default merge",
                        "conditions": [
                            f"base={self.main_branch_name}",
                            "label=queue",
                        ],
                        "actions": {
                            "queue": {
                                "name": "default",
                            },
                        },
                    },
                ],
            },
        )

    async def test_queue_with_rebase_update_method(self) -> None:
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
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                            "update_bot_account": "{% if 1 -%}mergify-test4{% endif -%}",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})

        pulls = await self.get_pulls()
        assert len(pulls) == 2

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
            key=lambda c: c["name"] == "Rule: default merge (queue)",
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force others to be rebased
        p2 = await self.create_pr()
        p2_merged = await self.merge_pull(p2["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        tmp_pull = await self.wait_for_pull_request("opened")
        assert tmp_pull["number"] not in [p1["number"]]
        assert tmp_pull["pull_request"]["changed_files"] == 1

        # No parent PR, but created instead updated
        q = await self.get_train()
        assert p2_merged["pull_request"]["merge_commit_sha"]
        await self.assert_merge_queue_contents(
            q,
            p2_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
        )

        await self.create_status(tmp_pull["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", tmp_pull["number"])
        p1_merged = await self.wait_for_pull_request("closed", p1["number"])

        await self.wait_for_push(branch_name=self.main_branch_name)

        await self.assert_merge_queue_contents(q, None, [])
        # ensure the MERGE QUEUE SUMMARY succeed
        check = first(
            await context.Context(
                self.repository_ctxt,
                p1_merged["pull_request"],
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
            self.repository_ctxt,
            p2,
        ).pull_engine_check_runs
        check = first(
            p2_checks,
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        summary = check["output"]["summary"]
        assert (
            """- [X] `status-success=another-ci`

**Required conditions to stay in the queue:**

- [ ] all of [📃 From pull request rule **default merge**]:
  - `status-success=continuous-integration/fake-ci`
    - [ ]"""
            in summary
        )

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}&per_page=5",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.merged",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "partition_names": [partr_config.DEFAULT_PARTITION_NAME],
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: default merge",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.leave",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "merged": True,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "reason": f"Pull request #{p1['number']} has been merged "
                        f"automatically at *{p1['merge_commit_sha']}*",
                        "seconds_waiting_for_schedule": 0,
                        "seconds_waiting_for_freeze": 0,
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: default merge",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.checks_end",
                    "metadata": {
                        "abort_code": None,
                        "abort_reason": "",
                        "abort_status": "DEFINITIVE",
                        "aborted": False,
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                            "unsuccessful_checks": [],
                        },
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.checks_start",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "start_reason": f"First time checking pull request #{p1['number']}",
                        "speculative_check_pull_request": {
                            "checks_conclusion": "pending",
                            "checks_started_at": None,
                            "checks_ended_at": None,
                            "checks_timed_out": False,
                            "in_place": True,
                            "number": p1["number"],
                            "unsuccessful_checks": [],
                        },
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.enter",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: default merge",
                },
            ],
            "per_page": 5,
            "size": 5,
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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

        await q.test_helper_load_from_redis()
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
        assert (
            check["output"]["summary"]
            == "The following conditions don't match anymore:\n- [ ] `label=queue`"
        )
        check = await ctxt.get_engine_check_run(constants.MERGE_QUEUE_SUMMARY_NAME)
        assert check is not None
        assert check["conclusion"] == "failure"
        assert (
            check["output"]["title"]
            == f"Pull request #{p2['number']} has been dequeued"
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_closed = await self.merge_pull(p["number"])
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
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert (
            check["output"]["summary"]
            == "The following conditions don't match anymore:\n- [ ] `label=queue`"
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}&per_page=2",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.leave",
                    "metadata": {
                        "merged": False,
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "reason": f"Pull request #{p1['number']} has been dequeued. "
                        "The pull request rule doesn't match anymore. "
                        "The following conditions don't match anymore:\n- [ ] `label=queue`",
                        "seconds_waiting_for_schedule": 0,
                        "seconds_waiting_for_freeze": 0,
                    },
                    "repository": p1["base"]["repo"]["full_name"],
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: default merge",
                },
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.checks_end",
                    "metadata": {
                        "abort_reason": anys.AnySearch(
                            str(
                                queue_utils.PrDequeued(
                                    pr_number=p1["number"],
                                    details=". The pull request rule doesn't match anymore",
                                ),
                            ),
                        ),
                        "abort_code": queue_utils.PrDequeued.dequeue_code,
                        "abort_status": "DEFINITIVE",
                        "aborted": True,
                        "branch": self.main_branch_name,
                        "partition_name": partr_config.DEFAULT_PARTITION_NAME,
                        "position": 0,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "speculative_check_pull_request": {
                            "checks_conclusion": "failure",
                            "checks_ended_at": None,
                            "checks_started_at": anys.ANY_AWARE_DATETIME_STR,
                            "checks_timed_out": False,
                            "in_place": False,
                            "number": tmp_pull_1["number"],
                            "unsuccessful_checks": [],
                        },
                    },
                    "repository": p1["base"]["repo"]["full_name"],
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "merge queue internal",
                },
            ],
            "per_page": 2,
            "size": 2,
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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

        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
        await self.run_engine()

        events = await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="opened",
                    ),
                },
                {
                    "event_type": "issue_comment",
                    "payload": tests_utils.get_issue_comment_event_payload(
                        action="created",
                    ),
                    "test_id": p1["number"],
                },
            ],
        )

        for event in events:
            if event.event_type == "pull_request":
                tmp_pull_2 = typing.cast(
                    github_types.GitHubPullRequest,
                    typing.cast(github_types.GitHubEventPullRequest, event.event)[
                        "pull_request"
                    ],
                )
                break
        else:
            raise RuntimeError(
                "Received events but did not find the correct event for tmp_pull_2",
            )

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
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert check["conclusion"] == "cancelled"
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )
        assert check["output"]["summary"].startswith(
            f"Pull request #{p1['number']} has been dequeued by a `dequeue` command.\n",
        )

        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert check["conclusion"] is None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        check = first(
            await context.Context(self.repository_ctxt, p3).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: default merge (queue)",
        )
        assert check is not None
        assert check["conclusion"] is None
        assert (
            check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

    async def test_train_reset_after_unexpected_base_branch_pushed_while_merging_batch(
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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                                "2022-07-26T14:14:14.000000+00:00",
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
                            "flag": None,
                            "attempts": None,
                        },
                    ),
                    "timestamp": github_types.ISODateTimeType(
                        "2022-07-26T14:14:14.000000+00:00",
                    ),
                    "initial_score": 0,
                },
            ),
        ]

        batch_draft_pr = await self.wait_for_pull_request("opened")

        ctxt = await self.repository_ctxt.get_pull_request_context(
            batch_draft_pr["number"],
            batch_draft_pr["pull_request"],
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()
        p4 = await self.create_pr()
        p5 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        p4 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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
        assert tmp_pull_sliced_1["pull_request"]["body"] is not None
        assert (
            f"""```yaml
---
previous_failed_batches:
  - checked_pull_requests:
      - {p1['number']}
      - {p2['number']}
      - {p3['number']}
    draft_pr_number: {tmp_pull['number']}
"""
            in tmp_pull_sliced_1["pull_request"]["body"]
        )

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

        await self.wait_for_push(branch_name=self.main_branch_name)

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

        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull_2["number"],
                    ),
                },
            ],
        )

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)
        p3 = await self.create_pr()
        p4 = await self.create_pr()
        p5 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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

        reason_abort = f"Pull requests combination (#{p1['number']}, #{p2['number']}, #{p3['number']}) which was ahead in the queue failed to get merged"
        await self.assert_api_checks_end_reason(p4["number"], reason_abort)
        await self.assert_api_checks_end_reason(p5["number"], reason_abort)

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

        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p1["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p2["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull_1["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull_3["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull_4["number"],
                    ),
                },
            ],
        )

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

        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_pull_6["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p4["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p5["number"],
                    ),
                },
            ],
        )

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to create draft PR
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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
        await self.push_file(destination_branch=p1["head"]["ref"])
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
        await q.test_helper_load_from_redis()
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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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
        await self.push_file(destination_branch=p1["head"]["ref"])
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["output"]["title"] == "The pull request rule doesn't match anymore"
        assert (
            check["output"]["summary"]
            == "The pull request has been synchronized by a user."
        )

    async def assert_api_checks_end_reason(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        expected_reason: str,
    ) -> None:
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={pr_number}",
        )
        assert r.status_code == 200
        response = r.json()
        for event in response["events"]:
            if event["type"] == "action.queue.checks_end":
                assert expected_reason in event["metadata"]["abort_reason"]
                return

        pytest.fail("No action.queue.checks_end event found")

    async def assert_api_checks_start_reason(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        expected_reason: str,
    ) -> None:
        response = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/"
            f"logs?pull_request={pr_number}&event_type=action.queue.checks_start&per_page=1",
        )
        assert response.status_code == 200
        payload = response.json()

        if not len(payload["events"]):
            pytest.fail("No action.queue.checks_start event found")
        else:
            event = payload["events"][0]
            assert expected_reason.lower() in event["metadata"]["start_reason"]

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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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
            p1["number"],
            "has sustained unexpected changes from external sources",
        )
        await self.assert_api_checks_end_reason(
            p2["number"],
            "has sustained unexpected changes from external sources",
        )

        # when detecting external changes onto the draft PR, the engine should disembark it and
        # unqueue all its contained PRs
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )
        check = first(
            await context.Context(self.repository_ctxt, p2).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Merge priority high (queue)",
        )
        assert check is not None
        assert check["output"]["title"].startswith(
            "The pull request has been removed from the queue",
        )

    async def test_frozen_train_reset_after_unexpected_base_branch_pushed(
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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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

        r = await self.admin_app.put(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
            json={"reason": "test freeze reason"},
        )
        assert r.status_code == 200

        # We set the status and run the engine to put the train car into a final state.
        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()

        # We push changes to the base branch
        await self.git("fetch", "origin", self.main_branch_name)
        await self.git("checkout", "-b", "random", f"origin/{self.main_branch_name}")
        open(self.git.repository + "/random_file.txt", "wb").close()
        await self.git("add", "random_file.txt")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", f"random:{self.main_branch_name}")
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        r = await self.admin_app.delete(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
        )
        assert r.status_code == 204

        await self.run_engine()

        await self.assert_api_checks_start_reason(
            p1["number"],
            "Unexpected queue change: an external action moved the base branch head to",
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

    async def test_draft_pr_train_reset_after_unexpected_base_branch_pushed(
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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        await self.assert_api_checks_end_reason(
            p1["number"],
            "Unexpected queue change: an external action moved the base branch head to",
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
                },
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
        await self.setup_repo(yaml.dump(rules_config), preload_configuration=True)

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
        await self.wait_for_push(branch_name=p1["head"]["ref"])
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        p1 = await self.get_pull(p1["number"])
        await self.run_engine()

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}",
        )
        assert r.status_code == 200
        response = r.json()
        timestamp_checks_end = None
        timestamp_queue_leave = None

        # Unexpected change detected
        for event in response["events"]:
            if event["type"] == "action.queue.checks_end":
                assert (
                    f"Unexpected queue change: the updated pull request #{p1['number']} has been manually updated"
                    in event["metadata"]["abort_reason"]
                )
                timestamp_checks_end = event["received_at"]
                break
        else:
            raise AssertionError("Event action.queue.checks_end not found")

        # Pr is sliced from queue to be updated
        assert timestamp_checks_end is not None
        for event in response["events"]:
            if event["type"] == "action.queue.leave":
                assert event["received_at"] > timestamp_checks_end
                assert event["pull_request"] == p1["number"]
                timestamp_queue_leave = event["received_at"]
                break
        else:
            raise AssertionError("Event action.queue.leave not found")

        # Pr is updated and requeued
        assert timestamp_queue_leave is not None
        for event in response["events"]:
            if event["type"] == "action.queue.enter":
                assert event["received_at"] > timestamp_queue_leave
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p = await self.create_pr()

        await self.add_label(p["number"], "queue")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        q = await self.get_train()
        await self.assert_merge_queue_contents(q, None, [])

        pulls = await self.get_pulls()
        assert len(pulls) == 0

    async def test_queue_inplace_with_merge_update_method_without_bot_account(
        self,
    ) -> None:
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
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "merge",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(as_="admin")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p1["number"], "queue")
        await self.run_engine()
        event = await self.wait_for_push(branch_name=p1["head"]["ref"])
        push_event = typing.cast(github_types.GitHubEventPush, event)

        assert (
            f"Merge branch '{self.main_branch_name}' into {p1['head']['ref']}"
            in push_event["head_commit"]["message"]
        )
        assert "mergify" in push_event["head_commit"]["author"]["username"]
        assert "[bot]" in push_event["head_commit"]["author"]["username"]

    async def test_queue_inplace_with_merge_update_method_and_bot_account(self) -> None:
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
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "merge",
                            "update_bot_account": "mergify-test1",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(as_="admin")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p1["number"], "queue")
        await self.run_engine()
        event = await self.wait_for_push(branch_name=p1["head"]["ref"])
        push_event = typing.cast(github_types.GitHubEventPush, event)
        assert (
            f"Merge branch '{self.main_branch_name}' into {p1['head']['ref']}"
            in push_event["head_commit"]["message"]
        )
        assert "mergify" in push_event["head_commit"]["author"]["username"]
        assert "[bot]" in push_event["head_commit"]["author"]["username"]

    async def test_queue_inplace_with_merge_update_method_and_different_bot_account(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "merge",
                            "update_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        protection = {
            "required_status_checks": {
                "strict": True,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_conversation_resolution": True,
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        p1 = await self.create_pr(as_="fork")

        p2 = await self.create_pr()
        await self.merge_pull_as_admin(p2["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.run_full_engine()

        p1_synced = await self.wait_for_pull_request("synchronize", p1["number"])
        await self.create_status(p1_synced["pull_request"])
        with mock.patch.object(
            merge_train.Train,
            "_send_queue_leave_signal",
            autospec=True,
        ) as mocked_queue_leave:
            await self.run_full_engine()

        # p1 should not have been removed from the train due to anything
        mocked_queue_leave.assert_not_called()

        await self.wait_for_pull_request("closed", p1["number"], merged=True)

    async def test_queue_with_ci_in_pull_request_rules(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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

        assert tmp_mq_p1["commits"] == 2
        assert tmp_mq_p1["changed_files"] == 1
        assert tmp_mq_p2["commits"] == 5
        assert tmp_mq_p2["changed_files"] == 2
        await self.create_status(tmp_mq_p2)

        await self.run_engine()
        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge default (queue)",
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
            key=lambda c: c["name"] == "Rule: merge default (queue)",
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
            "issue_comment",
            {"action": "created"},
            test_id=mq_pr_number,
        )
        comments = await self.get_issue_comments(mq_pr_number)
        assert (
            "Command not allowed on merge queue pull request." in comments[-1]["body"]
        )

        await self.create_comment_as_admin(mq_pr_number, "@mergifyio refresh")
        await self.run_engine()
        await self.wait_for(
            "issue_comment",
            {"action": "created"},
            test_id=mq_pr_number,
        )
        comments = await self.get_issue_comments(mq_pr_number)
        assert (
            f"""> refresh

#### ✅ Pull request refreshed



{utils.serialize_hidden_payload(commands_runner.CommandPayload({"command": "refresh", "conclusion": "success", "action_is_running":  True}))}"""
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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()
        for p in (p1, p2, p3):
            await self.wait_for("pull_request", {"action": "closed"})
            assert (await self.get_pull(p["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p1_refreshed = await self.get_pull(tmp_mq_p1["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name,
            bypass_cache=True,
        )
        assert branch["commit"]["sha"] == tmp_mq_p1_refreshed["head"]["sha"]

        # merge the second one
        await self.create_status(tmp_mq_p2["pull_request"])
        await self.run_engine()
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        assert (await self.get_pull(p4["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p2_refreshed = await self.get_pull(tmp_mq_p2["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name,
            bypass_cache=True,
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
        await self.wait_for_push(branch_name=self.main_branch_name)
        await self.run_engine()
        assert (await self.get_pull(p5["number"]))["merged"]

        # ensure it's fast-forward
        tmp_mq_p3_refreshed = await self.get_pull(tmp_mq_p3["number"])
        branch = await self.repository_ctxt.get_branch(
            self.main_branch_name,
            bypass_cache=True,
        )
        assert branch["commit"]["sha"] == tmp_mq_p3_refreshed["head"]["sha"]

        # Queue is now empty
        await self.assert_merge_queue_contents(q, None, [])

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p1['number']}&per_page=1",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "type": "action.queue.merged",
                    "metadata": {
                        "branch": self.main_branch_name,
                        "queue_name": "default",
                        "queued_at": anys.ANY_AWARE_DATETIME_STR,
                        "partition_names": [partr_config.DEFAULT_PARTITION_NAME],
                    },
                    "pull_request": p1["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: Queue",
                },
            ],
            "per_page": 1,
            "size": 1,
        }

    async def test_ongoing_train_basic(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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

        # Merge p1
        await self.create_status(tmp_mq_p1["pull_request"])
        await self.run_engine()
        await self.wait_for_push(branch_name=self.main_branch_name)
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
                    p_merged["pull_request"]["merge_commit_sha"],
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
                    p_merged["pull_request"]["merge_commit_sha"],
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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

        # p2 is ready first, ensure it's not merged
        await self.create_status(tmp_mq_p2)
        await self.run_engine()
        pulls = await self.get_pulls()
        assert len(pulls) == 3

        # Nothing change
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
        p2 = await self.get_pull(p2["number"])
        assert not p2["merged"]

        # p1 is ready, check both are merged in a row
        await self.create_status(tmp_mq_p1)
        await self.run_engine()

        await self.wait_for_push(branch_name=self.main_branch_name)
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
        self,
        allow_queue_branch_edit: bool,
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
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p2_bis["number"],
                ),
            ],
        )

        # Merge p2
        await self.create_status(tmp_mq_p2_bis)
        await self.run_engine()
        await self.wait_for_push(branch_name=self.main_branch_name)
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr(files={"conflicts": "well"})
        p2 = await self.create_pr(files={"conflicts": "boom"})
        p3 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.run_engine()

        # p2 merge failed in draft pr, p2 is taken out of the queue
        # p1 should be checked with p3 in draft
        tmp_mq_pr = await self.wait_for_pull_request("opened")

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
        assert (
            "The merge queue pull request can't be created\n"
            f"Details:\n> The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}\n"
        ) in check["output"]["summary"]

        # Merge the train
        await self.create_status(tmp_mq_pr["pull_request"])
        await self.run_engine()

        await self.wait_for_all(
            [
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=tmp_mq_pr["number"],
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p1["number"],
                        merged=True,
                    ),
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p3["number"],
                        merged=True,
                    ),
                },
                {
                    "event_type": "push",
                    "payload": tests_utils.get_push_event_payload(
                        branch_name=self.main_branch_name,
                    ),
                },
            ],
        )

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
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
        assert (
            "The merge queue pull request can't be created\n"
            f"Details:\n> The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}\n"
        ) in check["output"]["summary"]

        # Merge the train
        await self.create_status(p1)
        await self.run_engine()
        await self.wait_for_pull_request("closed", p1["number"], merged=True)

        await self.create_status(tmp_mq_pr["pull_request"])
        await self.run_engine()

        # Only p2 is remaining and not in train
        await self.wait_for_pull_request("closed", tmp_mq_pr["number"])
        await self.wait_for_pull_request("closed", p3["number"], merged=True)

        await self.assert_merge_queue_contents(q, None, [])

    async def test_queue_cancel_and_refresh_inplace(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 1,
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Tchou tchou",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr(as_="admin")

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        assert p_merged["pull_request"]["merge_commit_sha"] is not None

        # Queue PR
        await self.create_status(p1)
        await self.run_engine()

        rebased_p1 = await self.wait_for_pull_request("synchronize")

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        await self.create_status(rebased_p1["pull_request"], state="failure")
        await self.run_engine()

        await self.assert_merge_queue_contents(q, None, [])

        # refresh to erase the queue status
        check = typing.cast(
            github_types.GitHubCheckRun,
            await self.client_integration.items(
                f"{self.url_origin}/commits/{rebased_p1['pull_request']['head']['sha']}/check-runs",
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

        # To force others to be rebased
        p2 = await self.create_pr()
        p2_merged = await self.merge_pull(p2["number"])
        assert p2_merged["pull_request"]["merge_commit_sha"] is not None
        await self.run_engine()

        # Now it's success again, it can be reembarked automatically
        await self.create_status(rebased_p1["pull_request"], state="success")
        await self.run_engine()

        await self.assert_merge_queue_contents(
            q,
            p2_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p2_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

    async def test_queue_cancel_and_refresh(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
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
        p_merged_in_meantime = (await self.merge_pull(p_merged_in_meantime["number"]))[
            "pull_request"
        ]
        await self.wait_for_push(branch_name=self.main_branch_name)

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
                    "priority_rules": [
                        {
                            "name": "high priority",
                            "conditions": [
                                "label=high",
                            ],
                            "priority": "high",
                        },
                        {
                            "name": "default priority",
                            "conditions": [
                                "-label=high",
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
                        "status-success=continuous-integration/slow-ci",
                    ],
                    "allow_inplace_checks": False,
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        # put 3 PRs in queue
        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()
        await self.add_label(p1["number"], "low")
        await self.add_label(p2["number"], "low")
        await self.add_label(p3["number"], "low")

        # To force others to be rebased
        p_merged = await self.create_pr()
        await self.merge_pull(p_merged["number"])
        await self.run_engine()
        p_merged = await self.get_pull(p_merged["number"])
        q = await self.get_train()

        # my 3 PRs + 3 merge queue PR (3 spec checks)
        pulls = await self.get_pulls()
        assert len(pulls) == 6

        tmp_mq_p1 = pulls[2]
        tmp_mq_p2 = pulls[1]
        tmp_mq_p3 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p3["number"] not in [p1["number"], p2["number"], p3["number"]]
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
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_merged["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

        # Change the configuration and introduce disallow_checks_interruption_from_queues
        updated_rules = typing.cast(dict[typing.Any, typing.Any], copy.deepcopy(rules))
        updated_rules["queue_rules"][0]["disallow_checks_interruption_from_queues"] = [
            "default",
        ]
        p_new_config = await self.create_pr(
            files={".mergify.yml": yaml.dump(updated_rules)},
        )
        await self.merge_pull(p_new_config["number"])
        await self.run_engine()
        p_new_config = await self.get_pull(p_new_config["number"])

        q = await self.get_train()

        # my 3 PRs + 3 merge queue PR (3 spec checks)
        pulls = await self.get_pulls()
        assert len(pulls) == 6

        tmp_mq_p1 = pulls[2]
        tmp_mq_p2 = pulls[1]
        tmp_mq_p3 = pulls[0]
        assert tmp_mq_p1["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p2["number"] not in [p1["number"], p2["number"], p3["number"]]
        assert tmp_mq_p3["number"] not in [p1["number"], p2["number"], p3["number"]]
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
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [p1["number"], p2["number"]],
                    p_new_config["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
        )

        # Put last PR at the begining of the queue via pr priority Checks
        # must not be interrupted due to
        # disallow_checks_interruption_from_queues config
        await self.remove_label(p3["number"], "low")
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()
        p3 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p3["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_mq_p3["number"],
                ),
            ],
            [p1["number"], p2["number"]],
        )

        # Queue API with token
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queues",
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
                            "effective_priority": 12000,
                            "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                                    "merge_method": None,
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                    "autosquash": True,
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
                            "effective_priority": 2000,
                            "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                                    "merge_method": None,
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                    "autosquash": True,
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
                            "effective_priority": 2000,
                            "partition_name": partr_config.DEFAULT_PARTITION_NAME,
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
                                    "merge_method": None,
                                    "merge_bot_account": None,
                                    "update_method": None,
                                    "update_bot_account": None,
                                    "autosquash": True,
                                },
                                "name": "default",
                            },
                            "queued_at": anys.ANY_AWARE_DATETIME_STR,
                            "speculative_check_pull_request": None,
                            "mergeability_check": None,
                            "estimated_time_of_merge": None,
                        },
                    ],
                },
            ],
        }

        # Merge p3
        await self.create_status(tmp_mq_p3, context="continuous-integration/fast-ci")
        await self.run_engine()
        p3 = await self.get_pull(p3["number"])
        assert p3["merged"]

        await self.wait_for_push(branch_name=self.main_branch_name)
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        reason="This test is not reliable, GitHub doeesn't always allow to create the tmp pr",
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/ci.yml": TEMPLATE_GITHUB_ACTION % "echo Default CI",
            },
        )

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr(
            files={
                ".github/workflows/ci.yml": TEMPLATE_GITHUB_ACTION % "echo Changed CI",
            },
        )

        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

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

        assert tmp_mq_p1["commits"] == 7
        assert tmp_mq_p1["changed_files"] == 1
        assert tmp_mq_p2["commits"] == 7
        assert tmp_mq_p2["changed_files"] == 5
        await self.create_status(tmp_mq_p2)

        await self.run_engine()

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge default (queue)",
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "status-success=very-long-ci",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.create_status(p1, context="very-long-ci")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

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
                key=lambda c: c["name"] == "Rule: default merge (queue)",
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

        await self.wait_for_push(branch_name=self.main_branch_name)

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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "status-success=continuous-integration/fake-ci",
                        "status-success=very-long-ci",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.create_status(p1, context="very-long-ci")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

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
            key=lambda c: c["name"] == "Rule: merge default (queue)",
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
                },
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
        with time_travel("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.run_full_engine()

            await self.create_status(p1)
            await self.run_full_engine()

            check_run = await self.wait_for_check_run(
                name="Rule: queue (queue)",
                action="created",
                status="in_progress",
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
                "delayed-refresh",
                "-inf",
                "+inf",
                withscores=True,
            )
            assert len(pulls_to_refresh) == 1

        with time_travel("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()

            # Check-runs arrive in random order, so we need to retrieve all of them and check
            # if the event we retrieved are the ones we expected.
            # There are 3 check_run events that should arrive:
            # - Summary
            # - Rule: queue (queue)
            # - Queue: Embarked in merge queue
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
                        assert check_run["check_run"]["output"]["title"].startswith(
                            "The pull request has been removed from the queue",
                        )
                        assert check_run["check_run"]["conclusion"] == "cancelled"
                    case "Queue: Embarked in merge queue":
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
                    "Did not find check_run event for 'Rule: queue (queue)'",
                )
            if not found_embarked_train:
                raise AssertionError(
                    "Did not find check_run event for 'Queue: Embarked in merge queue'",
                )

    async def test_queue_ci_timeout_inplace_with_only_pull_request_rules(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "checks_timeout": "10 m",
                },
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
        with time_travel("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

            p1 = await self.create_pr()

            # To force others to be rebased
            p = await self.create_pr()
            await self.merge_pull(p["number"])
            await self.create_status(p1)
            await self.run_full_engine()

            check_run = await self.wait_for_check_run(
                name="Rule: queue (queue)",
                action="created",
                status="in_progress",
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
                "delayed-refresh",
                "-inf",
                "+inf",
                withscores=True,
            )
            assert len(pulls_to_refresh) == 1

        with time_travel("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()

            # Check-runs arrive in random order, so we need to retrieve all of them and check
            # if the event we retrieved are the ones we expected.
            # There are 3 check_run events that should arrive:
            # - Summary
            # - Rule: queue (queue)
            # - Queue: Embarked in merge queue
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
                        assert check_run["check_run"]["output"]["title"].startswith(
                            "The pull request has been removed from the queue",
                        )
                        assert check_run["check_run"]["conclusion"] == "cancelled"
                    case "Queue: Embarked in merge queue":
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
                    "Did not find check_run event for 'Rule: queue (queue)'",
                )
            if not found_embarked_train:
                raise AssertionError(
                    "Did not find check_run event for 'Queue: Embarked in merge queue'",
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
                },
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
        with time_travel("2021-05-30T10:00:00", tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

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
                "delayed-refresh",
                "-inf",
                "+inf",
                withscores=True,
            )
            assert len(pulls_to_refresh) == 1

        with time_travel("2021-05-30T10:12:00", tick=True):
            await self.run_full_engine()
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Rule: queue (queue)",
            )
            assert check is not None
            assert check["output"]["title"].startswith(
                "The pull request has been removed from the queue",
            )
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == "Queue: Embarked in merge queue",
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
                            ],
                        },
                        "schedule: MON-FRI 08:00-17:00",
                    ],
                    "checks_timeout": "10 m",
                    "allow_inplace_checks": False,
                },
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

        start_date = datetime.datetime(2021, 5, 30, 20, tzinfo=date.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

            p1 = await self.create_pr()

            await self.create_status(p1)
            await self.run_full_engine()

            await self.wait_for_pull_request("opened")
            await self.run_full_engine()

            ctxt_p1 = context.Context(self.repository_ctxt, p1)
            check = await ctxt_p1.get_engine_check_run("Rule: queue (queue)")
            assert check is not None
            assert (
                check["output"]["title"]
                == "The pull request is the 1st in the queue to be merged"
            )

            check = await ctxt_p1.get_engine_check_run(
                constants.MERGE_QUEUE_SUMMARY_NAME,
            )
            assert check is not None

            # In case of lag on the recording machine, the expected `20:10` might be a few minutes
            # late.
            match = re.search(
                r"The checks have to pass before 20:1(\d) UTC",
                check["output"]["summary"],
            )
            assert match

            minutes_delay = int(match.group(1))

            pulls_to_refresh: list[
                tuple[bytes, float]
            ] = await self.redis_links.cache.zrangebyscore(
                "delayed-refresh",
                "-inf",
                "+inf",
                withscores=True,
            )
            assert len(pulls_to_refresh) == 1

        with time_travel(
            start_date + datetime.timedelta(minutes=12 + minutes_delay),
            tick=True,
        ):
            await self.run_full_engine()
            ctxt_p1._caches.pull_check_runs.delete()
            check = await ctxt_p1.get_engine_check_run("Rule: queue (queue)")
            assert check is not None
            assert check["output"]["title"].startswith(
                "The pull request has been removed from the queue",
            )
            check = first(
                await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
                key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
            )
            assert check is not None
            assert (
                check["output"]["title"]
                == "The queue conditions cannot be satisfied due to checks timeout"
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
                            ],
                        },
                        "schedule: MON-FRI 08:00-17:00",
                    ],
                    "checks_timeout": "10 m",
                    "allow_inplace_checks": False,
                },
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

        with time_travel("2021-05-30T20:00:00", tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

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
                "delayed-refresh",
                "-inf",
                "+inf",
                withscores=True,
            )
            assert len(pulls_to_refresh) == 1
            await self.create_status(tmp_pull["pull_request"])

        with time_travel("2021-05-30T20:12:00", tick=True):
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

    async def test_unfreeze_outside_schedule(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": ["schedule: MON-FRI 08:00-17:00"],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        with time_travel("2023-06-30T15:00:00", tick=True):
            await self.setup_repo(yaml.dump(config), preload_configuration=True)

            r = await self.admin_app.put(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
                json={"reason": "frezee for test"},
            )
            assert r.status_code == 200

            pr = await self.create_pr()

            await self.add_label(pr["number"], "queue")

            await self.run_engine()

            train = await self.get_train()
            car = train.get_car(
                await self.repository_ctxt.get_pull_request_context(pr["number"]),
            )
            assert car is not None
            assert car.train_car_state.outcome == merge_train.TrainCarOutcome.MERGEABLE

            r = await self.admin_app.delete(
                f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/queue/default/freeze",
            )
            assert r.status_code == 204

        with time_travel("2023-06-30T18:59:00", tick=True):
            await self.run_engine()

            train = await self.get_train()
            car = train.get_car(
                await self.repository_ctxt.get_pull_request_context(pr["number"]),
            )
            assert car is not None
            assert car.train_car_state.outcome == merge_train.TrainCarOutcome.UNKNOWN

        pr = await self.get_pull(pr["number"])
        assert pr["merged"] is False

        with time_travel("2023-07-03T08:10:00", tick=True):
            await self.run_full_engine()

        await self.wait_for_pull_request("closed", pr_number=pr["number"], merged=True)

    async def test_queue_with_default_config_branch_protection_only(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                },
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
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        p_merged = await self.merge_pull_as_admin(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.create_status(p1)
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

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

    async def _test_queue_without_branch_protection_for_queueing(
        self,
        rules: dict[str, typing.Any],
    ) -> None:
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
        p_merged = await self.merge_pull_as_admin(p["number"])
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

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
                        "branch_protection_injection_mode": "merge",
                    },
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
                                "merge_method": "squash",
                                "name": "default",
                            },
                        },
                    },
                ],
            },
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
                        "branch_protection_injection_mode": "merge",
                    },
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
                            },
                        },
                    },
                ],
            },
        )

    async def test_queue_checks_and_branch(self) -> None:
        rules = f"""
queue_rules:
  - name: default
    merge_conditions:
      - "check-success=Summary"
      - "check-success=ci/status"
      - "check-success=ci/service-test"
      - "check-success=ci/pipelines"
      - "#approved-reviews-by>=1"
      - "-label=flag:wait"
    branch_protection_injection_mode: merge

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
        update_method: rebase
"""
        await self.setup_repo(rules, preload_configuration=True)

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

        p1 = await self.create_pr(as_="fork")

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull_as_admin(p["number"])
        await self.run_engine()

        await self.create_review(p1["number"])
        await self.add_label(p1["number"], "flag:merge")
        await self.run_engine()

        await self.wait_for_pull_request("synchronize", p1["number"])
        await self.run_engine()

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
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        check = first(
            await context.Context(self.repository_ctxt, p1).pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: merge (queue)",
        )
        assert check is not None
        assert (
            check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        await self.create_status(p1, "ci/status", state="pending")
        await self.run_engine()

        await self.create_status(p1, "ci/status")
        await self.create_status(p1, "ci/service-test")
        await self.run_engine()

        await self.create_status(p1, "ci/pipelines")
        await self.run_engine()
        await self.wait_for_pull_request("closed", p1["number"], merged=True)

        await self.assert_merge_queue_contents(q, None, [])

    async def test_unexpected_config_change(self) -> None:
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
                    "name": "Merge me",
                    "conditions": ["label=queue"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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
                },
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

    async def test_base_branch_vanished(self) -> None:
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
                    "conditions": [
                        f"base={featureA}",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(
            yaml.dump(rules),
            test_branches=[featureA],
            preload_configuration=True,
        )

        await self.create_pr(base=featureA)
        await self.create_pr(base=featureA)
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "opened"})

        queues = [
            q
            async for convoy in merge_train.Convoy.iter_convoys(
                self.repository_ctxt,
            )
            for q in convoy.iter_trains()
        ]
        assert len(queues) == 1
        assert len(await queues[0].get_pulls()) == 2

        await self.client_integration.delete(
            f"{self.url_origin}/git/refs/heads/{parse.quote(featureA)}",
        )
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        queues = [
            q
            async for convoy in merge_train.Convoy.iter_convoys(self.repository_ctxt)
            for q in convoy.iter_trains()
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
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=high",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=low",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

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

    async def test_queue_inplace_autosquash_true(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                            "autosquash": True,
                            "update_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(pr_fixup["number"], "queue")
        await self.run_engine()
        await self.wait_for_pull_request("synchronize", pr_fixup["number"])

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

    async def test_queue_inplace_autosquash_false(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                            "autosquash": False,
                            "update_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(pr_fixup["number"], "queue")
        await self.run_engine()
        await self.wait_for_pull_request("synchronize", pr_fixup["number"])

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 2

    async def test_queue_inplace_uptodate_autosquash_true(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                            "autosquash": True,
                            "update_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

        await self.add_label(pr_fixup["number"], "queue")
        await self.run_engine()
        await self.wait_for_pull_request("synchronize", pr_fixup["number"])

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 1

    async def test_queue_inplace_uptodate_autosquash_false(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "allow_inplace_checks": True,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Automatic merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {
                            "name": "default",
                            "update_method": "rebase",
                            "autosquash": False,
                            "update_bot_account": "mergify-test4",
                        },
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        pr_fixup = await self.create_pr_with_autosquash_commit("fixup")

        await self.add_label(pr_fixup["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("closed", pr_fixup["number"])

        fixup_commits = await self.get_commits(pr_fixup["number"])
        assert len(fixup_commits) == 2

    async def test_queue_inplace_with_outdated_pull_request_from_github(self) -> None:
        rules = f"""
shared:
  merge_ci: &common_checks
    - "check-success=ci/circleci: check_if_tests_done"
    - "#approved-reviews-by>=1"

queue_rules:
  - name: master_queue
    conditions: *common_checks
    checks_timeout: 25m


pull_request_rules:
  - name: master
    conditions:
      - base={self.main_branch_name}
      - label=to-be-merged
      - and: *common_checks
    actions:
      queue:
        name: master_queue
        method: squash
"""

        await self.setup_repo(rules, preload_configuration=True)

        p1 = await self.create_pr()
        await self.create_review(p1["number"])
        await self.add_label(p1["number"], "to-be-merged")
        await self.create_status(p1, "ci/circleci: check_if_tests_done")

        p1 = await self.get_pull(p1["number"])
        p1_ctxt = context.Context(self.repository_ctxt, p1, [])
        p1_statuses = await p1_ctxt.pull_statuses

        p2 = await self.create_pr()
        await self.merge_pull_as_admin(p2["number"])

        real_handle_merge_error = merge_base.MergeUtilsMixin._handle_merge_error

        with respx.mock(assert_all_called=False) as respx_mock:
            # Mock the merge response to get the one from the bug, otherwise
            # we get a 409 but with a different message
            respx_mock.put(
                f"{settings.GITHUB_REST_API_URL}"
                f"/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}"
                f"/pulls/{p1['number']}/merge",
            ).respond(
                409,
                json={
                    "message": "Head branch is out of date. Review and try the merge again.",
                },
            )

            # Return an outdated p1 pull request
            respx_mock.get(
                f"{settings.GITHUB_REST_API_URL}"
                f"/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}"
                f"/pulls/{p1['number']}",
            ).respond(
                200,
                json=p1,  # type: ignore[arg-type]
            )

            # Mock get on status, that are not for the old p1 head sha (before the inplace merge)
            # or the p2 head sha, to return the status for the old p1 (before inplace merge)
            # in order to reproduce the bug all the time.
            respx_mock.route(
                url__regex=f"{settings.GITHUB_REST_API_URL}"
                f"/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}"
                rf"/commits/(?!{p1['head']['sha']}|{p2['head']['sha']})\w+/status",
            ).respond(
                200,
                json={
                    "state": p1_statuses[0]["state"],
                    "statuses": p1_statuses,
                },
            )

            respx_mock.route(host="api.github.com").pass_through()

            async def mocked_handle_merge_error(*args, **kwargs):  # type: ignore[no-untyped-def]
                # Rollback the previous mock so we get the 409 error only once
                respx_mock.rollback()
                respx_mock.route(host="api.github.com").pass_through()
                return await real_handle_merge_error(*args, **kwargs)

            with mock.patch.object(
                merge_base.MergeUtilsMixin,
                "_handle_merge_error",
                mocked_handle_merge_error,
            ), mock.patch.object(
                context.Context,
                "is_head_sha_outdated",
                return_value=False,
            ):
                await self.run_full_engine()

        # PR should still be in the queue since the merge conditions do not match anymore
        # after the inplace update of the PR (the status success should be reset)
        train = await self.get_train()
        assert len(train._cars) == 1
        assert len(train._cars[0].still_queued_embarked_pulls) == 1
        assert [
            p.user_pull_request_number
            for p in train._cars[0].still_queued_embarked_pulls
        ] == [p1["number"]]

        assert (
            f"""
  - [ ] all of:
    - [ ] `check-success=ci/circleci: check_if_tests_done`
    - `#approved-reviews-by>=1`
      - [X] #{p1['number']}"""
            in train._cars[0].get_original_pr_summary(p1["number"]).merge_conditions
        )

    async def test_queue_command_after_action_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge on queue label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        p1 = await self.create_pr()

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()

        await self.wait_for_all(
            [
                {
                    "event_type": "issue_comment",
                    "payload": tests_utils.get_issue_comment_event_payload(
                        action="created",
                    ),
                    "test_id": p1["number"],
                },
                {
                    "event_type": "pull_request",
                    "payload": tests_utils.get_pull_request_event_payload(
                        action="closed",
                        pr_number=p1["number"],
                        merged=True,
                    ),
                },
            ],
        )

    async def test_unqueue_command_after_queue_action_and_command(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge on queue label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        p2 = await self.create_pr()
        await self.merge_pull(p2["number"])

        await self.add_label(p1["number"], "queue")
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
        await self.run_engine()

        # queue command's comment
        events = await self.wait_for_all(
            [
                {
                    "event_type": "issue_comment",
                    "payload": tests_utils.get_issue_comment_event_payload(
                        action="created",
                    ),
                    "test_id": p1["number"],
                },
                {
                    "event_type": "issue_comment",
                    "payload": tests_utils.get_issue_comment_event_payload(
                        action="created",
                    ),
                    "test_id": p1["number"],
                },
                {
                    "event_type": "check_run",
                    "payload": tests_utils.get_check_run_event_payload(
                        name="Queue: Embarked in merge queue",
                        status="completed",
                        conclusion="cancelled",
                    ),
                },
            ],
        )

        # The unqueue comment should always be the last of the two issue_comment events
        if events[0].event_type == "check_run":
            check_run = typing.cast(github_types.GitHubEventCheckRun, events[0].event)
            comment_unqueue = typing.cast(
                github_types.GitHubEventIssueComment,
                events[-1].event,
            )
        elif events[1].event_type == "check_run":
            check_run = typing.cast(github_types.GitHubEventCheckRun, events[1].event)
            comment_unqueue = typing.cast(
                github_types.GitHubEventIssueComment,
                events[-1].event,
            )
        else:
            check_run = typing.cast(github_types.GitHubEventCheckRun, events[2].event)
            comment_unqueue = typing.cast(
                github_types.GitHubEventIssueComment,
                events[1].event,
            )

        assert (
            "The pull request has been removed from the queue `default`"
            in comment_unqueue["comment"]["body"]
        )

        assert (
            check_run["check_run"]["output"]["title"]
            == f"Pull request #{p1['number']} has been dequeued"
        )

        assert " by a `dequeue` command" in check_run["check_run"]["output"]["summary"]

    async def test_unqueue_then_requeue_not_in_first_place_check_run(self) -> None:
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
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("opened")

        ctxt = context.Context(self.repository_ctxt, p1)
        rule_label_check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue on label (queue)",
        )
        assert rule_label_check is not None
        assert rule_label_check["status"] == "in_progress"
        assert (
            rule_label_check["output"]["title"]
            == "The pull request is the 1st in the queue to be merged"
        )

        queue_summary_check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert queue_summary_check is not None
        assert queue_summary_check["status"] == "in_progress"

        await self.remove_label(p1["number"], "queue")
        await self.run_engine()

        await self.wait_for_check_run(
            name=constants.MERGE_QUEUE_SUMMARY_NAME,
            status="completed",
            conclusion="neutral",
        )

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        ctxt = context.Context(self.repository_ctxt, p1)
        rule_label_check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == "Rule: Queue on label (queue)",
        )
        assert rule_label_check is not None
        assert (
            rule_label_check["output"]["title"]
            == "The pull request is the 2nd in the queue to be merged"
        )

        # Retrieve the second one from context check runs because the check run
        # we receive from events are highly unreliable
        queue_summary_check = first(
            await ctxt.pull_engine_check_runs,
            key=lambda c: c["name"] == constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert queue_summary_check is not None
        assert queue_summary_check["status"] == "in_progress"
        assert queue_summary_check["conclusion"] is None
        assert (
            queue_summary_check["output"]["title"]
            == "The pull request has been refreshed and is going to be re-embarked soon"
        )

    async def test_base_branch_conflicts_check_run_message_draft(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "allow_inplace_checks": False,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Label queue conflicts",
                    "conditions": ["queue-dequeue-reason=conflict-with-base-branch"],
                    "actions": {"label": {"add": ["queue-conflict"]}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1, p2 = await self.create_prs_with_conflicts()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]

        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        pr2_labeled = await self.wait_for_pull_request(
            "labeled",
            pr_number=p2["number"],
        )
        assert pr2_labeled is not None
        assert "queue-conflict" in [
            label["name"] for label in pr2_labeled["pull_request"]["labels"]
        ]

        ctxt = context.Context(self.repository_ctxt, p2)
        queue_summary_check_run = await ctxt.get_engine_check_run(
            constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert queue_summary_check_run is not None
        assert (
            "This pull request cannot be embarked for merge"
            in queue_summary_check_run["output"]["title"]
        )
        assert (
            "The pull request conflicts with the base branch"
            in queue_summary_check_run["output"]["summary"]
        )

        # Check event logs
        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/logs?pull_request={p2['number']}&per_page=5",
        )
        assert len(r.json()["events"]) == 3
        assert r.json()["events"][1]["type"] == "action.queue.leave"
        assert r.json()["events"][1]["metadata"]["reason"] == (
            "The pull request conflicts with the base branch"
        )

    async def test_conflicts_with_pull_ahead(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                    "allow_inplace_checks": False,
                    "batch_size": 2,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Label queue conflicts",
                    "conditions": ["queue-dequeue-reason=conflict-with-pull-ahead"],
                    "actions": {"label": {"add": ["queue-conflict"]}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1, p2 = await self.create_prs_with_conflicts()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        pr2_labeled = await self.wait_for_pull_request(
            "labeled",
            pr_number=p2["number"],
        )
        assert pr2_labeled is not None
        assert "queue-conflict" in [
            label["name"] for label in pr2_labeled["pull_request"]["labels"]
        ]

        ctxt = context.Context(self.repository_ctxt, p2)
        queue_summary_check_run = await ctxt.get_engine_check_run(
            constants.MERGE_QUEUE_SUMMARY_NAME,
        )
        assert queue_summary_check_run is not None
        assert (
            queue_summary_check_run["output"]["title"]
            == "This pull request cannot be embarked for merge"
        )
        assert (
            f"The pull request conflicts with at least one pull request ahead in queue: #{p1['number']}"
            in queue_summary_check_run["output"]["summary"]
        )

    async def test_batch_max_wait_time_after_pr_conflicts(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": True,
                    "batch_size": 2,
                    "batch_max_wait_time": "5 m",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue on label",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()
        p2, p3 = await self.create_prs_with_conflicts()
        p4 = await self.create_pr()

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.add_label(p3["number"], "queue")
        await self.add_label(p4["number"], "queue")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")

        train = await self.get_train()
        assert len(train._cars) == 1
        assert [p1["number"], p2["number"]] == [
            p.user_pull_request_number
            for p in train._cars[0].still_queued_embarked_pulls
        ]
        assert [p3["number"], p4["number"]] == [
            p.user_pull_request_number for p in train._waiting_pulls
        ]

        await self.create_status(draft_pr["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])
        # p1 and p2
        await self.wait_for_pull_request("closed")
        await self.wait_for_pull_request("closed")

        draft_pr_p4 = await self.wait_for_pull_request("opened")

        # p3 should be taken out of the train because of conflicts
        # and p4 should be checked immediately without waiting for batch_max_wait_time

        train = await self.get_train()
        assert len(train._cars) == 1
        assert [p4["number"]] == [
            p.user_pull_request_number
            for p in train._cars[0].still_queued_embarked_pulls
        ]
        assert [] == [p.user_pull_request_number for p in train._waiting_pulls]

        await self.create_status(draft_pr_p4["pull_request"])
        await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr_p4["number"])
        p4_closed = await self.wait_for_pull_request("closed", p4["number"])
        assert p4_closed["pull_request"]["merged"]

    async def test_check_runs_filtering(self) -> None:
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
                    "name": "Queue and post check",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {
                        "queue": {"name": "default"},
                        "post_check": {},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        await self.create_pr()
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.run_engine()

        p1 = await self.wait_for_pull_request("synchronize")
        await self.create_status(p1["pull_request"], state="pending")

        await self.run_engine()

        train = await self.get_train()
        assert len(train._cars) == 1
        assert [c.name for c in train._cars[0].last_checks] == [
            "Rule: Queue and post check (post_check)",
            "continuous-integration/fake-ci",
        ]

    async def test_queue_checks_end_emission(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "check-success=pending_check",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "queue_to_default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)
        pr = await self.create_pr()

        await self.add_label(pr["number"], "queue")
        await self.run_engine()

        # embarking PR in queue
        await self.wait_for_check_run(
            name="Rule: queue_to_default (queue)",
            status="in_progress",
        )
        await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            status="in_progress",
        )

        pending_check = await self.create_check_run(
            pr,
            name="pending_check",
            conclusion=None,
            external_id=check_api.USER_CREATED_CHECKS,
        )

        # a queue freeze is created
        await self._create_queue_freeze(
            queue_name="default",
            freeze_payload={
                "reason": "weekend",
            },
        )
        await self.run_engine()

        # merge conditions validated while queue is frozen -> pr is mergeable
        await self.update_check_run(
            pr,
            check_id=pending_check["check_run"]["id"],
        )
        await self.run_engine()
        await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="success",
        )

        # checks_end signal has been emitted
        async with database.create_session() as db:
            result = await db.execute(
                sqlalchemy.select(func.count()).select_from(
                    evt_models.EventActionQueueChecksEnd,
                ),
            )
        assert result.scalar() == 1

        await self.wait_for_check_run(
            name="Queue: Embarked in merge queue",
            conclusion="neutral",
        )

        # queue freeze lifted
        await self._delete_queue_freeze(queue_name="default", expected_status_code=204)
        await self.run_engine()

        await self.wait_for_check_run(
            name="Rule: queue_to_default (queue)",
            conclusion="success",
        )

        # signal has already been emitted
        async with database.create_session() as db:
            result = await db.execute(
                sqlalchemy.select(func.count()).select_from(
                    evt_models.EventActionQueueChecksEnd,
                ),
            )
        assert result.scalar() == 1

    async def test_train_car_state_time_spent_outside_schedule(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "schedule=09:00-18:00",
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
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        start_date = datetime.datetime(2023, 9, 28, 16, tzinfo=datetime.UTC)
        with time_travel(start_date, tick=True):
            await self.setup_repo(yaml.dump(rules), preload_configuration=True)

            p1 = await self.create_pr()

            await self.add_label(p1["number"], "queue")
            await self.run_full_engine()

            await self.wait_for_pull_request("opened")

        with time_travel(start_date + datetime.timedelta(hours=3), tick=True):
            # The delayed_refresh should refresh the train car and add a
            # value to `train_car_state.seconds_spent_outside_schedule_start_dates`
            await self.run_full_engine()

            train = await self.get_train()
            assert len(train._cars) == 1
            assert (
                len(
                    train._cars[
                        0
                    ].train_car_state.time_spent_outside_schedule_start_dates,
                )
                == 1
            )
            assert (
                len(
                    train._cars[
                        0
                    ].train_car_state.time_spent_outside_schedule_end_dates,
                )
                == 0
            )

        # Next day at the start of schedule
        with time_travel(
            datetime.datetime(2023, 9, 29, 9, tzinfo=datetime.UTC),
            tick=True,
        ):
            # The delayed_refresh should refresh the train car and add a
            # value to `train_car_state.seconds_spent_outside_schedule_end_dates`
            await self.run_full_engine()

            train = await self.get_train()
            assert len(train._cars) == 1
            assert (
                len(
                    train._cars[
                        0
                    ].train_car_state.time_spent_outside_schedule_start_dates,
                )
                == 1
            )
            assert (
                len(
                    train._cars[
                        0
                    ].train_car_state.time_spent_outside_schedule_start_dates,
                )
                == 1
            )

            assert (
                50000
                <= train._cars[0].train_car_state.seconds_spent_outside_schedule
                <= 54000
            )

    async def test_queue_auto_reembark(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "check-success=second-ci",
                    ],
                    "speculative_checks": 1,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [
                        "check-success=continuous-integration/fake-ci",
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        p1 = await self.create_pr()

        # To force others to be rebased
        p = await self.create_pr()
        p_merged = await self.merge_pull(p["number"])
        assert p_merged["pull_request"]["merge_commit_sha"] is not None

        await self.create_status(p1)
        await self.run_engine()

        # p1 got rebased
        await self.wait_for("pull_request", {"action": "synchronize"})

        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

        head_sha = p1["head"]["sha"]
        p1 = await self.get_pull(p1["number"])
        assert p1["head"]["sha"] != head_sha  # ensure it have been rebased

        # CI has failed ensure it have been unqueue
        await self.create_status(p1, state="failure")
        await self.run_engine()
        await self.assert_merge_queue_contents(q, None, [])
        p1 = await self.get_pull(p1["number"])
        assert not p1["merged"]

        # CI has been restart and works, we can reembark it automatically
        await self.create_status(p1)
        await self.run_engine()

        # Back to the queue
        q = await self.get_train()
        await self.assert_merge_queue_contents(
            q,
            p_merged["pull_request"]["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p_merged["pull_request"]["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
        )

    async def test_queue_default_merge_method(self) -> None:
        rules = {
            "queue_rules": [{"name": "default", "conditions": []}],
            "pull_request_rules": [
                {
                    "name": "Queue",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules), preload_configuration=True)

        # Allow merge
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("merge",),
        ):
            await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )

        # Allow rebase
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("rebase",),
        ):
            await self.run_full_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )

        # Allow squash
        pull = await self.create_pr()
        async with self.allow_merge_methods(
            self.url_origin,
            pull["number"],
            ("squash",),
        ):
            await self.run_engine()
        await self.wait_for_pull_request(
            action="closed",
            pr_number=pull["number"],
            merged=True,
        )


class TestQueueActionFeaturesSubscription(base.FunctionalTestBase):
    @pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
    async def test_queue_action_and_command_without_merge_queue_feature_flag(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge on queue label",
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

        check_run_p1 = await self.wait_for_check_run(
            name="Rule: Merge on queue label (queue)",
            conclusion="action_required",
        )
        assert (
            check_run_p1["check_run"]["output"]["title"]
            == "Cannot use the merge queue."
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()

        comment_p1_rep = await self.wait_for_issue_comment(str(p1["number"]), "created")
        assert "Cannot use the command `queue`" in comment_p1_rep["comment"]["body"]
        assert (
            f"The [subscription]({settings.DASHBOARD_UI_FRONT_URL}/github/mergifyio-testing/subscription) needs to be updated to enable this feature"
            in comment_p1_rep["comment"]["body"]
        )

        await self.create_comment_as_admin(p1["number"], "@mergifyio dequeue")
        await self.run_engine()

        comment_p1_rep = await self.wait_for_issue_comment(str(p1["number"]), "created")
        assert "Cannot use the command `dequeue`" in comment_p1_rep["comment"]["body"]
        assert (
            f"The [subscription]({settings.DASHBOARD_UI_FRONT_URL}/github/mergifyio-testing/subscription) needs to be updated to enable this feature"
            in comment_p1_rep["comment"]["body"]
        )
