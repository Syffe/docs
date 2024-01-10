from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.queue import utils as queue_utils
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.unit import conftest
from mergify_engine.tests.unit.merge_train import conftest as mt_conftest


UNQUEUE_REASON_DEQUEUED = queue_utils.PrDequeued(123, "whatever")


async def test_convoy_add_pull_1_partition_rule(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()
    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    await convoy.add_pull(
        await context_getter(123),
        config,
        [partr_config.PartitionRuleName("projectA")],
        "",
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[123]]),
        ("projectB", []),
        ("projectC", []),
    ]

    await convoy.add_pull(
        await context_getter(456),
        config,
        [partr_config.PartitionRuleName("projectB")],
        "",
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[123]]),
        ("projectB", [[456]]),
        ("projectC", []),
    ]


async def test_convoy_add_pull_multiple_partition_rules(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()
    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    await convoy.add_pull(
        await context_getter(123),
        config,
        [
            partr_config.PartitionRuleName("projectA"),
            partr_config.PartitionRuleName("projectB"),
        ],
        "",
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[123]]),
        ("projectB", [[123]]),
        ("projectC", []),
    ]

    await convoy.add_pull(
        await context_getter(456),
        config,
        [
            partr_config.PartitionRuleName("projectB"),
            partr_config.PartitionRuleName("projectC"),
        ],
        "",
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[123]]),
        ("projectB", [[123], [123, 456]]),
        ("projectC", [[456]]),
    ]


async def tests_convoy_remove_middle_not_merged_1_partition(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()
    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    partition_rules = [partr_config.PartitionRuleName("projectA")]
    await convoy.add_pull(await context_getter(1), config, partition_rules, "")
    await convoy.add_pull(await context_getter(2), config, partition_rules, "")
    await convoy.add_pull(await context_getter(3), config, partition_rules, "")

    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 2], [1, 2, 3]]),
        ("projectB", []),
        ("projectC", []),
    ]

    # Merged by someone else
    await convoy.remove_pull(
        github_types.GitHubPullRequestNumber(2),
        "",
        UNQUEUE_REASON_DEQUEUED,
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 3]]),
        ("projectB", []),
        ("projectC", []),
    ]


async def tests_convoy_remove_middle_not_merged_multiple_partitions(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()
    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    partition_rules_a = [partr_config.PartitionRuleName("projectA")]
    partition_rules_ab = [
        *partition_rules_a,
        partr_config.PartitionRuleName("projectB"),
    ]
    await convoy.add_pull(await context_getter(1), config, partition_rules_ab, "")
    await convoy.add_pull(await context_getter(2), config, partition_rules_ab, "")
    await convoy.add_pull(await context_getter(3), config, partition_rules_a, "")

    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 2], [1, 2, 3]]),
        ("projectB", [[1], [1, 2]]),
        ("projectC", []),
    ]

    # Merged by someone else
    await convoy.remove_pull(
        github_types.GitHubPullRequestNumber(2),
        "",
        UNQUEUE_REASON_DEQUEUED,
    )
    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 3]]),
        ("projectB", [[1]]),
        ("projectC", []),
    ]


async def test_convoy_remove_head_merged_1_partition(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()

    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    partition_rules_a = [partr_config.PartitionRuleName("projectA")]
    await convoy.add_pull(await context_getter(1), config, partition_rules_a, "")
    await convoy.add_pull(await context_getter(2), config, partition_rules_a, "")
    await convoy.add_pull(await context_getter(3), config, partition_rules_a, "")

    await convoy.refresh_trains()
    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 2], [1, 2, 3]]),
        ("projectB", []),
        ("projectC", []),
    ]

    await convoy.remove_pull(
        github_types.GitHubPullRequestNumber(1),
        "",
        queue_utils.PrMerged(1, github_types.SHAType("new_sha1")),
    )

    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1, 2], [1, 2, 3]]),
        ("projectB", []),
        ("projectC", []),
    ]


async def test_convoy_remove_head_merged_multiple_partitions(
    context_getter: conftest.ContextGetterFixture,
    convoy: merge_train.Convoy,
) -> None:
    await convoy.load_from_redis()

    config = conftest.get_pull_queue_config(
        convoy.repository.mergify_config["queue_rules"],
        "5x1",
    )

    partition_rules_a = [partr_config.PartitionRuleName("projectA")]
    partition_rules_ab = [
        *partition_rules_a,
        partr_config.PartitionRuleName("projectB"),
    ]
    await convoy.add_pull(await context_getter(1), config, partition_rules_ab, "")
    await convoy.add_pull(await context_getter(2), config, partition_rules_a, "")
    await convoy.add_pull(await context_getter(3), config, partition_rules_ab, "")

    await convoy.refresh_trains()
    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1], [1, 2], [1, 2, 3]]),
        ("projectB", [[1], [1, 3]]),
        ("projectC", []),
    ]

    await convoy.remove_pull(
        github_types.GitHubPullRequestNumber(1),
        "",
        queue_utils.PrMerged(1, github_types.SHAType("new_sha1")),
    )

    await convoy.refresh_trains()

    assert mt_conftest.get_convoy_train_cars_content(convoy) == [
        ("projectA", [[1, 2], [1, 2, 3]]),
        ("projectB", [[1, 3]]),
        ("projectC", []),
    ]
