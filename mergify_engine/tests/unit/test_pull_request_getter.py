from unittest import mock

import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import pull_request_getter
from mergify_engine.models.github import pull_request as gh_pr_model
from mergify_engine.tests import db_populator
from mergify_engine.tests import utils


@pytest.mark.populated_db_datasets("AccountAndRepo")
async def test_can_repo_use_pull_requests_in_pg(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await populated_db.commit()

    with mock.patch(
        "mergify_engine.settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS",
        new_callable=mock.PropertyMock(return_value=["OneAccount"]),
    ):
        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["OneRepo"],
            ),
        )
        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("OneAccount"),
        )

        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["colliding_repo_1"],
            ),
        )
        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("colliding-account-1"),
        )

    with mock.patch(
        "mergify_engine.settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS",
        new_callable=mock.PropertyMock(return_value=["*"]),
    ):
        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["OneRepo"],
            ),
        )
        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("OneAccount"),
        )

        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["colliding_repo_1"],
            ),
        )
        assert await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("colliding-account-1"),
        )

    with mock.patch(
        "mergify_engine.settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS",
        new_callable=mock.PropertyMock(return_value=[]),
    ):
        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["OneRepo"],
            ),
        )
        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("OneAccount"),
        )

        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_id=github_types.GitHubRepositoryIdType(
                db_populator.DbPopulator.internal_ref["colliding_repo_1"],
            ),
        )
        assert not await pull_request_getter.can_repo_use_pull_requests_in_pg(
            repo_owner=github_types.GitHubLogin("colliding-account-1"),
        )


async def test_same_pull_request_number_in_multiple_repo(
    db: sqlalchemy.ext.asyncio.AsyncSession,
    _mock_gh_pull_request_commits_insert_in_pg: None,
) -> None:
    pr_number = github_types.GitHubPullRequestNumber(123)

    owner1 = github_types.GitHubAccount(
        id=github_types.GitHubAccountIdType(1),
        login=github_types.GitHubLogin("owner1"),
        type="User",
        avatar_url="https://dummy.com",
    )
    repo1_1 = github_types.GitHubRepository(
        id=github_types.GitHubRepositoryIdType(1),
        owner=owner1,
        private=True,
        name=github_types.GitHubRepositoryName("repo1"),
        full_name="owner1/repo1",
        archived=False,
        url="https://blabla.com",
        html_url="https://blabla.com",
        default_branch=github_types.GitHubRefType("main"),
    )
    pr_repo1 = utils.fake_full_pull_request(
        github_types.GitHubPullRequestId(1),
        pr_number,
        repo1_1,
        title="PR REPO 1_1",
    )

    # #####
    owner2 = github_types.GitHubAccount(
        id=github_types.GitHubAccountIdType(2),
        login=github_types.GitHubLogin("owner2"),
        type="User",
        avatar_url="https://dummy.com",
    )
    repo1_2 = github_types.GitHubRepository(
        id=github_types.GitHubRepositoryIdType(2),
        owner=owner2,
        private=True,
        name=github_types.GitHubRepositoryName("repo1"),
        full_name="owner2/repo1",
        archived=False,
        url="https://blabla.com",
        html_url="https://blabla.com",
        default_branch=github_types.GitHubRefType("main"),
    )
    pr_repo2 = utils.fake_full_pull_request(
        github_types.GitHubPullRequestId(2),
        pr_number,
        repo1_2,
        title="PR REPO 1_2",
    )

    await gh_pr_model.PullRequest.insert_or_update(db, pr_repo1)
    await gh_pr_model.PullRequest.insert_or_update(db, pr_repo2)
    await db.commit()

    # #####
    find_pull_repo_1_1 = await pull_request_getter._find_pull_in_db(
        pr_number,
        repo_owner=github_types.GitHubLogin("owner1"),
        repo_name=github_types.GitHubRepositoryName("repo1"),
    )
    assert find_pull_repo_1_1 is not None
    assert find_pull_repo_1_1["title"] == "PR REPO 1_1"

    find_pull_repo_1_1 = await pull_request_getter._find_pull_in_db(
        pr_number,
        repo_id=github_types.GitHubRepositoryIdType(1),
    )
    assert find_pull_repo_1_1 is not None
    assert find_pull_repo_1_1["title"] == "PR REPO 1_1"

    # #####
    find_pull_repo_1_2 = await pull_request_getter._find_pull_in_db(
        pr_number,
        repo_owner=github_types.GitHubLogin("owner2"),
        repo_name=github_types.GitHubRepositoryName("repo1"),
    )
    assert find_pull_repo_1_2 is not None
    assert find_pull_repo_1_2["title"] == "PR REPO 1_2"

    find_pull_repo_1_2 = await pull_request_getter._find_pull_in_db(
        pr_number,
        repo_id=github_types.GitHubRepositoryIdType(2),
    )
    assert find_pull_repo_1_2 is not None
    assert find_pull_repo_1_2["title"] == "PR REPO 1_2"
