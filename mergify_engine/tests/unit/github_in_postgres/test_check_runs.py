import typing

import sqlalchemy

from mergify_engine import github_types
from mergify_engine.models.github import check_run as gh_checkrun_model


def fake_full_check_run_for_db_insert(
    check_run_id: int,
    repo_id: int,
    app_id: int = 1,
    **kwargs: typing.Any,
) -> github_types.GitHubCheckRunWithRepository:
    check_run = github_types.GitHubCheckRunWithRepository(
        id=check_run_id,
        app=github_types.GitHubApp(
            id=app_id,
            name="1",
            slug="1",
            owner=github_types.GitHubAccount(
                id=github_types.GitHubAccountIdType(1),
                login=github_types.GitHubLogin("owner1"),
                type="User",
                avatar_url="https://dummy.com",
            ),
        ),
        external_id="abc123",
        pull_requests=[],
        head_sha=github_types.SHAType("sha"),
        name="Summary",
        status="in_progress",
        output=github_types.GitHubCheckRunOutput(
            title="outputitle",
            summary="little summary",
            text="lorem ipsum",
            annotations_count=0,
            annotations_url="http://annotations.com",
        ),
        conclusion=None,
        started_at=github_types.ISODateTimeType("2023-12-18 13:52:53.152533+00:00"),
        completed_at=None,
        html_url="",
        details_url="",
        check_suite=github_types.GitHubCheckRunCheckSuite(id=123),
        repository=github_types.GitHubRepository(
            id=github_types.GitHubRepositoryIdType(repo_id),
            owner=github_types.GitHubAccount(
                id=github_types.GitHubAccountIdType(1),
                login=github_types.GitHubLogin("owner1"),
                type="User",
                avatar_url="https://dummy.com",
            ),
            private=True,
            name=github_types.GitHubRepositoryName("repo1"),
            full_name="owner1/repo1",
            archived=False,
            url="https://blabla.com",
            html_url="https://blabla.com",
            default_branch=github_types.GitHubRefType("main"),
        ),
    )

    check_run.update(kwargs)  # type: ignore[typeddict-item]
    return check_run


async def test_get_checks_as_github_dict(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    check_run_1 = fake_full_check_run_for_db_insert(1, repo_id=1, app_id=1)
    check_run_2 = fake_full_check_run_for_db_insert(2, repo_id=1, app_id=2)
    check_run_3 = fake_full_check_run_for_db_insert(
        3,
        repo_id=1,
        app_id=2,
        name="definitelynotasummary",
    )

    await gh_checkrun_model.CheckRun.insert_or_update(db, check_run_1)
    await gh_checkrun_model.CheckRun.insert_or_update(db, check_run_2)
    await gh_checkrun_model.CheckRun.insert_or_update(db, check_run_3)
    await db.commit()

    assert (
        len(
            await gh_checkrun_model.CheckRun.get_checks_as_github_dict(
                repo_id=1,
                sha=github_types.SHAType("sha"),
            ),
        )
        == 3
    )

    check_runs_from_db_1 = await gh_checkrun_model.CheckRun.get_checks_as_github_dict(
        repo_id=1,
        sha=github_types.SHAType("sha"),
        app_id=1,
    )
    assert len(check_runs_from_db_1) == 1
    assert check_runs_from_db_1[0]["app"]["id"] == 1

    check_runs_from_db_2 = await gh_checkrun_model.CheckRun.get_checks_as_github_dict(
        repo_id=1,
        sha=github_types.SHAType("sha"),
        app_id=2,
    )
    assert len(check_runs_from_db_2) == 2
    assert check_runs_from_db_2[0]["app"]["id"] == 2

    check_runs_from_db_2 = await gh_checkrun_model.CheckRun.get_checks_as_github_dict(
        repo_id=1,
        sha=github_types.SHAType("sha"),
        app_id=2,
        check_name="Summary",
    )
    assert len(check_runs_from_db_2) == 1
    assert check_runs_from_db_2[0]["app"]["id"] == 2
    assert check_runs_from_db_2[0]["name"] == "Summary"


async def test_check_runs_cleanup(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    # Three check runs with same head_sha, repo_id, name, app_id but different id and started_at
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            1,
            repo_id=1,
            app_id=1,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:53.152533+00:00"),
        ),
    )
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            2,
            repo_id=1,
            app_id=1,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:54.152533+00:00"),
        ),
    )
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            3,
            repo_id=1,
            app_id=1,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:55.152533+00:00"),
        ),
    )

    # Another check run with the same head_sha, repo_id, name and started_at as the first one
    # but different id and app_id. It shouldn't be deleted.
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            4,
            repo_id=1,
            app_id=2,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:53.152533+00:00"),
        ),
    )
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(5, repo_id=2, app_id=3, head_sha="sha2"),
    )
    await db.commit()

    assert (
        await db.scalar(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                gh_checkrun_model.CheckRun,
            ),
        )
    ) == 5

    await gh_checkrun_model.CheckRun.delete_outdated_check_runs()
    check_runs = (await db.scalars(sqlalchemy.select(gh_checkrun_model.CheckRun))).all()
    assert len(check_runs) == 3

    assert sorted([c.id for c in check_runs]) == [3, 4, 5]


async def test_get_check_runs_ordered(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            1,
            repo_id=1,
            app_id=1,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:53.152533+00:00"),
        ),
    )
    await gh_checkrun_model.CheckRun.insert_or_update(
        db,
        fake_full_check_run_for_db_insert(
            2,
            repo_id=1,
            app_id=1,
            started_at=github_types.ISODateTimeType("2023-12-18 13:52:54.152533+00:00"),
        ),
    )
    await db.commit()

    checks = await gh_checkrun_model.CheckRun.get_checks_as_github_dict(
        1,
        github_types.SHAType("sha"),
    )
    assert len(checks) == 1
    assert checks[0]["started_at"] == "2023-12-18T13:52:54.152533Z"
