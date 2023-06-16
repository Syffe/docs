import datetime
import typing
from unittest import mock

from first import first
from freezegun import freeze_time
import httpx
import msgpack
import pytest

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import eventlogs
from mergify_engine import queue
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules import conditions
from mergify_engine.rules.config import priority_rules as pr_config
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import base


class TestTrainApiCalls(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_create_pull_basic(self) -> None:
        config = {
            "queue_rules": [
                {
                    "name": "foo",
                    "merge_conditions": [
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
            autosquash=True,
        )
        pull_queue_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
            autosquash=True,
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

        q.convoy.queue_rules = qr_config.QueueRules(
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

        tmp_pull = await self.wait_for_pull_request("opened")
        assert tmp_pull["pull_request"]["draft"]
        assert tmp_pull["pull_request"]["body"] is not None
        assert (
            f"""
---
pull_requests:
  - number: {p2["number"]}
...
"""
            in tmp_pull["pull_request"]["body"]
        )

        await car.send_checks_end_signal(
            p2["number"], queue_utils.ChecksFailed(), "REEMBARKED"
        )
        await car.end_checking(
            reason=queue_utils.ChecksFailed(), not_reembarked_pull_requests={}
        )

        await self.wait_for_pull_request("edited", tmp_pull["number"])
        tmp_pull = await self.wait_for_pull_request("edited", tmp_pull["number"])
        assert tmp_pull["pull_request"]["body"] is not None
        assert str(queue_utils.ChecksFailed()) in tmp_pull["pull_request"]["body"]

        await self.wait_for_pull_request("closed", tmp_pull["number"])

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
                    "merge_conditions": [
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
            autosquash=True,
        )
        queue_pull_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
            autosquash=True,
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

        q.convoy.queue_rules = qr_config.QueueRules(
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
                    "merge_conditions": [
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
            autosquash=True,
        )
        queue_pull_config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
            autosquash=True,
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

        q.convoy.queue_rules = qr_config.QueueRules(
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
            autosquash=True,
        )
        config = queue.PullQueueConfig(
            name=qr_config.QueueName("foo"),
            update_method="merge",
            priority=0,
            effective_priority=0,
            bot_account=None,
            update_bot_account=None,
            autosquash=True,
        )

        car = merge_train.TrainCar(
            q,
            merge_train.TrainCarState(),
            [merge_train.EmbarkedPull(q, p3["number"], config, date.utcnow())],
            [merge_train.EmbarkedPull(q, p3["number"], config, date.utcnow())],
            [p1["number"], p2["number"]],
            base_sha,
        )

        q.convoy.queue_rules = qr_config.QueueRules(
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
                    "merge_conditions": [
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
                    "name": "merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        # Friday, 18:00 UTC
        start_date = datetime.datetime(2022, 11, 4, 18, tzinfo=datetime.UTC)

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
                    "merge_conditions": [
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

    async def test_handle_merge_error(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
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

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        draft_pr = await self.wait_for_pull_request("opened")
        await self.create_status(draft_pr["pull_request"])

        real_put = github.AsyncGithubClient.put
        counter = 0

        async def mock_put(self, *args, **kwargs) -> httpx.Response:  # type: ignore[no-untyped-def]
            nonlocal counter
            if counter == 0:
                counter += 1
                raise http.HTTPClientSideError(
                    message="Head branch was modified in the meantime",
                    request=mock.Mock(),
                    response=httpx.Response(
                        status_code=405,
                        json={"message": "Head branch was modified in the meantime"},
                    ),
                )

            return await real_put(self, *args, **kwargs)

        # We just want to trigger the part of the code that calls the
        # `pending_result_builder` to make sure it is called properly
        with mock.patch.object(
            github.AsyncGithubClient,
            "put",
            new=mock_put,
        ):
            await self.run_engine()

        await self.wait_for_pull_request("closed", draft_pr["number"])
        p1_closed = await self.wait_for_pull_request("closed", p1["number"])
        assert p1_closed["pull_request"]["merged"]
