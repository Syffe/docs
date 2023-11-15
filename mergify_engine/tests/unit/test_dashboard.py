import typing

from mergify_engine import dashboard
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.unit import conftest


async def test_get_queues_url_from_context(
    context_getter: conftest.ContextGetterFixture,
    fake_convoy: merge_train.Convoy,
) -> None:
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(42))

    # With NO convoy
    assert (
        await dashboard.get_queues_url_from_context(ctxt)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues?branch=main&pull=42"
    )
    assert (
        await dashboard.get_queues_url_from_context(ctxt, open_pr_details=False)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues?branch=main"
    )

    # With Convoy
    await fake_convoy.load_from_redis()
    config = conftest.get_pull_queue_config(conftest.QUEUE_RULES, "default")

    # PR in partition B
    await fake_convoy.add_pull(
        ctxt,
        config,
        [partr_config.PartitionRuleName("projectB")],
        "",
    )
    assert (
        await dashboard.get_queues_url_from_context(ctxt, fake_convoy)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues/partitions/projectB?branch=main&queues=default&pull=42"
    )
    assert (
        await dashboard.get_queues_url_from_context(
            ctxt,
            fake_convoy,
            open_pr_details=False,
        )
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues/partitions/projectB?branch=main&queues=default"
    )

    # PR in partition B and partion A
    await fake_convoy.add_pull(
        ctxt,
        config,
        [partr_config.PartitionRuleName("projectA")],
        "",
    )
    assert ["projectA", "projectB"] == [
        epwt.train.partition_name
        for epwt in await fake_convoy.find_embarked_pull_with_train(ctxt.pull["number"])
    ]
    assert (
        await dashboard.get_queues_url_from_context(ctxt, fake_convoy)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues/partitions/projectA?branch=main&queues=default&pull=42"
    )
    assert (
        await dashboard.get_queues_url_from_context(
            ctxt,
            fake_convoy,
            open_pr_details=False,
        )
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/Mergifyio/repo/mergify-engine/queues/partitions/projectA?branch=main&queues=default"
    )


def test_get_eventlogs_url() -> None:
    fake_login = typing.cast(github_types.GitHubLogin, "Bar")
    fake_repo_name = typing.cast(github_types.GitHubRepositoryName, "Foo")
    assert (
        dashboard.get_eventlogs_url(fake_login, fake_repo_name)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{fake_login}/repo/{fake_repo_name}/event-logs"
    )

    assert (
        dashboard.get_eventlogs_url(
            fake_login,
            fake_repo_name,
            typing.cast(github_types.GitHubPullRequestNumber, 42),
        )
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{fake_login}/repo/{fake_repo_name}/event-logs?pullRequestNumber=42"
    )
