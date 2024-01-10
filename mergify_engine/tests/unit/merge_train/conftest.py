import base64
from collections import abc
import typing
from unittest import mock

import pytest

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import rules
from mergify_engine import signals
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import pull_request_rules as prr_config


@pytest.fixture(autouse=True, scope="package")
def _mock_signals() -> abc.Generator[None, None, None]:
    # We don't check signal sending in this folder, so we can just mock
    # it to not have to initialize the database.
    with mock.patch.object(signals, "send"):
        yield


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


@pytest.fixture()
def convoy(
    repository: context.Repository,
) -> merge_train.Convoy:
    repository._caches.mergify_config.set(
        rules.UserConfigurationSchema(rules.YamlSchema(MERGIFY_CONFIG)),
    )
    return merge_train.Convoy(
        repository,
        github_types.GitHubRefType("main"),
    )


@pytest.fixture()
def fake_client() -> mock.Mock:
    branch = {"commit": {"sha": "sha1"}}

    def item_call(
        url: str,
        *_args: typing.Any,
        **_kwargs: typing.Any,
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


@pytest.fixture()
def repository(
    fake_repository: context.Repository,
    fake_client: mock.Mock,
) -> context.Repository:
    fake_repository.installation.client = fake_client
    return fake_repository


@pytest.fixture(autouse=True)
def _autoload_redis(redis_cache: redis_utils.RedisCache) -> None:  # noqa: ARG001
    # Just always load redis_cache to load all redis scripts
    pass


async def fake_train_car_start_checking_with_draft(
    inner_self: merge_train.TrainCar,
    previous_car: merge_train.TrainCar | None,  # noqa: ARG001
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
    inner_self: merge_train.TrainCar,  # noqa: ARG001
    origin: typing.Literal[  # noqa: ARG001
        "original_pull_request",
        "draft_pull_request",
        "batch_split",
    ],
    original_pull_request_rule: prr_config.EvaluatedPullRequestRule | None,  # noqa: ARG001
    original_pull_request_number: github_types.GitHubPullRequestNumber | None,  # noqa: ARG001
) -> None:
    pass


async def fake_train_car_end_checking(
    inner_self: merge_train.TrainCar,  # noqa: ARG001
    reason: queue_utils.BaseDequeueReason,  # noqa: ARG001
    not_reembarked_pull_requests: dict[  # noqa: ARG001
        github_types.GitHubPullRequestNumber,
        queue_utils.BaseDequeueReason,
    ],
) -> None:
    pass


async def fake_convoy_update_user_pull_request_summary(
    inner_self: merge_train.Convoy,  # noqa: ARG001
    user_pull_request_number: github_types.GitHubPullRequestNumber,  # noqa: ARG001
    temporary_car: merge_train.TrainCar,  # noqa: ARG001
) -> None:
    pass


@pytest.fixture(autouse=True)
def _monkeypatched_traincar(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(
        "mergify_engine.queue.merge_train.Convoy.update_user_pull_request_summary",
        fake_convoy_update_user_pull_request_summary,
    )


def get_train_cars_content(
    train: merge_train.Train,
) -> list[list[github_types.GitHubPullRequestNumber]]:
    return [
        car.parent_pull_request_numbers
        + [ep.user_pull_request_number for ep in car.still_queued_embarked_pulls]
        for car in train._cars
    ]


def get_convoy_train_cars_content(
    convoy: merge_train.Convoy,
) -> list[
    tuple[
        partr_config.PartitionRuleName,
        list[list[github_types.GitHubPullRequestNumber]],
    ]
]:
    return [
        (train.partition_name, get_train_cars_content(train))
        for train in convoy.iter_trains()
    ]


def get_train_waiting_pulls_content(
    train: merge_train.Train,
) -> list[github_types.GitHubPullRequestNumber]:
    waiting_pulls, _ignored_pulls = train._get_waiting_pulls_ordered_by_priority()
    return [wp.user_pull_request_number for wp in waiting_pulls]


def get_convoy_trains_waiting_pulls_content(
    convoy: merge_train.Convoy,
) -> list[
    tuple[
        partr_config.PartitionRuleName,
        list[github_types.GitHubPullRequestNumber],
    ]
]:
    return [
        (train.partition_name, get_train_waiting_pulls_content(train))
        for train in convoy.iter_trains()
    ]
