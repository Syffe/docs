import base64
import typing
from unittest import mock

import pytest
import voluptuous

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


@pytest.fixture(autouse=True)
def setup_fake_mergify_bot_user(fake_mergify_bot: None) -> None:
    pass


MERGIFY_CONFIG = """
queue_rules:
  - name: inplace
    merge_conditions: []
    speculative_checks: 5

  - name: high-1x1
    merge_conditions: []
    speculative_checks: 1

  - name: high-1x2
    merge_conditions: []
    speculative_checks: 1
    allow_inplace_checks: False
    batch_size: 2
    batch_max_wait_time: 0 s

  - name: 1x5
    merge_conditions: []
    speculative_checks: 1
    allow_inplace_checks: False
    batch_size: 5
    batch_max_wait_time: 0 s

  - name: 2x1
    merge_conditions: []
    speculative_checks: 2
    allow_inplace_checks: False
  - name: 2x5
    merge_conditions: []
    speculative_checks: 2
    allow_inplace_checks: False
    batch_size: 5
    batch_max_wait_time: 0 s

  - name: 5x1
    merge_conditions: []
    speculative_checks: 5
    allow_inplace_checks: False
  - name: 5x3
    merge_conditions: []
    speculative_checks: 5
    allow_inplace_checks: False
    batch_size: 3
    batch_max_wait_time: 0 s

  - name: batch-wait-time
    merge_conditions: []
    speculative_checks: 2
    allow_inplace_checks: False
    batch_size: 2
    batch_max_wait_time: 5 m

  - name: high-2x5-noint
    merge_conditions: []
    speculative_checks: 2
    allow_inplace_checks: False
    batch_size: 5
    batch_max_wait_time: 0 s
    allow_checks_interruption: False
  - name: low-2x5-noint
    merge_conditions: []
    speculative_checks: 2
    allow_inplace_checks: False
    batch_size: 5
    batch_max_wait_time: 0 s
    allow_checks_interruption: False
    allow_inplace_checks: False
  - name: low-1x5-noint
    merge_conditions: []
    speculative_checks: 1
    batch_size: 5
    batch_max_wait_time: 0 s
    allow_checks_interruption: False
    allow_inplace_checks: False

  - name: urgent-1x4
    merge_conditions: []
    speculative_checks: 1
    batch_size: 4
    batch_max_wait_time: 0 s
    allow_inplace_checks: False
  - name: fastlane-1x8-noint
    merge_conditions: []
    speculative_checks: 1
    batch_size: 8
    batch_max_wait_time: 0 s
    allow_checks_interruption: False
    allow_inplace_checks: False
  - name: regular-1x8-noint-from-fastlane-and-regular
    merge_conditions: []
    speculative_checks: 1
    batch_size: 4
    batch_max_wait_time: 0 s
    allow_inplace_checks: False
    disallow_checks_interruption_from_queues:
      - regular-1x8-noint-from-fastlane-and-regular
      - fastlane-1x8-noint

  - name: 2x8-batch-max-failure-2
    merge_conditions: []
    speculative_checks: 2
    batch_size: 8
    allow_inplace_checks: False
    batch_max_wait_time: 0 s
    batch_max_failure_resolution_attempts: 2
  - name: 2x8-batch-max-failure-0
    merge_conditions: []
    speculative_checks: 2
    batch_size: 8
    allow_inplace_checks: False
    batch_max_wait_time: 0 s
    batch_max_failure_resolution_attempts: 0
  - name: 1x8-batch-max-failure-2
    merge_conditions: []
    speculative_checks: 1
    batch_size: 8
    allow_inplace_checks: False
    batch_max_wait_time: 0 s
    batch_max_failure_resolution_attempts: 2
  - name: 1x8-batch-max-failure-0
    merge_conditions: []
    speculative_checks: 1
    batch_size: 8
    allow_inplace_checks: False
    batch_max_wait_time: 0 s
    batch_max_failure_resolution_attempts: 0

partition_rules:
  - name: projectA
    conditions:
      - label=projectA

  - name: projectB
    conditions:
      - label=projectB

  - name: projectC
    conditions:
      - label=projectC
"""

QUEUE_RULES = voluptuous.Schema(qr_config.QueueRulesSchema)(
    rules.YamlSchema(MERGIFY_CONFIG)["queue_rules"],
)
PARTITION_RULES = voluptuous.Schema(partr_config.PartitionRulesSchema)(
    rules.YamlSchema(MERGIFY_CONFIG)["partition_rules"],
)


@pytest.fixture
def convoy(
    repository: context.Repository,
) -> merge_train.Convoy:
    return merge_train.Convoy(
        repository,
        QUEUE_RULES,
        PARTITION_RULES,
        github_types.GitHubRefType("main"),
    )


@pytest.fixture
def fake_client() -> mock.Mock:
    branch = {"commit": {"sha": "sha1"}}

    def item_call(
        url: str,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> dict[str, typing.Any]:
        if url == "/repos/Mergifyio/mergify-engine/contents/.mergify.yml":
            return {
                "type": "file",
                "sha": "whatever",
                "content": base64.b64encode(MERGIFY_CONFIG.encode()).decode(),
                "path": ".mergify.yml",
            }

        if url == "repos/Mergifyio/mergify-engine/branches/main":
            return branch

        for i in range(40, 49):
            if url.startswith(f"/repos/Mergifyio/mergify-engine/pulls/{i}"):
                return {"merged": True, "merge_commit_sha": f"sha{i}"}

        raise Exception(f"url not mocked: {url}")  # noqa: TRY002

    def update_base_sha(sha: github_types.SHAType) -> None:
        branch["commit"]["sha"] = sha

    client = mock.Mock()
    client.item = mock.AsyncMock(side_effect=item_call)
    client.items = mock.MagicMock()
    client.update_base_sha = update_base_sha
    return client


@pytest.fixture
def repository(
    fake_repository: context.Repository,
    fake_client: mock.Mock,
) -> context.Repository:
    fake_repository.installation.client = fake_client
    return fake_repository


@pytest.fixture(autouse=True)
def autoload_redis(redis_cache: redis_utils.RedisCache) -> None:
    # Just always load redis_cache to load all redis scripts
    pass


async def fake_train_car_start_checking_with_draft(
    inner_self: merge_train.TrainCar,
    previous_car: merge_train.TrainCar | None,
) -> None:
    inner_self.train_car_state.checks_type = merge_train.TrainCarChecksType.DRAFT
    inner_self.queue_pull_request_number = github_types.GitHubPullRequestNumber(
        inner_self.still_queued_embarked_pulls[-1].user_pull_request_number + 10,
    )


async def fake_train_car_start_checking_inplace(
    inner_self: merge_train.TrainCar,
) -> None:
    inner_self.train_car_state.checks_type = merge_train.TrainCarChecksType.INPLACE


async def fake_train_car_check_mergeability(
    inner_self: merge_train.TrainCar,
    origin: typing.Literal[
        "original_pull_request",
        "draft_pull_request",
        "batch_split",
    ],
    original_pull_request_rule: prr_config.EvaluatedPullRequestRule | None,
    original_pull_request_number: github_types.GitHubPullRequestNumber | None,
) -> None:
    pass


async def fake_train_car_end_checking(
    inner_self: merge_train.TrainCar,
    reason: queue_utils.BaseDequeueReason,
    not_reembarked_pull_requests: dict[
        github_types.GitHubPullRequestNumber,
        queue_utils.BaseDequeueReason,
    ],
) -> None:
    pass


@pytest.fixture(autouse=True)
def monkeypatched_traincar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "mergify_engine.queue.merge_train.TrainCar.start_checking_inplace",
        fake_train_car_start_checking_inplace,
    )

    monkeypatch.setattr(
        "mergify_engine.queue.merge_train.TrainCar.start_checking_with_draft",
        fake_train_car_start_checking_with_draft,
    )
    monkeypatch.setattr(
        "mergify_engine.queue.merge_train.TrainCar.end_checking",
        fake_train_car_end_checking,
    )
    monkeypatch.setattr(
        "mergify_engine.queue.merge_train.TrainCar.check_mergeability",
        fake_train_car_check_mergeability,
    )


def get_train_cars_content(
    train: merge_train.Train,
) -> list[list[github_types.GitHubPullRequestNumber]]:
    cars = []
    for car in train._cars:
        cars.append(
            car.parent_pull_request_numbers
            + [ep.user_pull_request_number for ep in car.still_queued_embarked_pulls],
        )
    return cars


def get_convoy_train_cars_content(
    convoy: merge_train.Convoy,
) -> list[
    tuple[
        partr_config.PartitionRuleName,
        list[list[github_types.GitHubPullRequestNumber]],
    ]
]:
    content = []
    for train in convoy.iter_trains():
        content.append((train.partition_name, get_train_cars_content(train)))
    return content


def get_train_waiting_pulls_content(
    train: merge_train.Train,
) -> list[github_types.GitHubPullRequestNumber]:
    waiting_pulls, ignored_pulls = train._get_waiting_pulls_ordered_by_priority()
    return [wp.user_pull_request_number for wp in waiting_pulls]


def get_convoy_trains_waiting_pulls_content(
    convoy: merge_train.Convoy,
) -> list[
    tuple[
        partr_config.PartitionRuleName,
        list[github_types.GitHubPullRequestNumber],
    ]
]:
    content = []
    for train in convoy.iter_trains():
        content.append((train.partition_name, get_train_waiting_pulls_content(train)))
    return content
