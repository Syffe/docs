import random

import pytest
import sqlalchemy
from sqlalchemy import func
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import events_db
from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine.models import events as evt_model


async def insert_event(
    fake_repository: context.Repository,
    event: signals.EventName,
    metadata: signals.EventMetadata,
) -> None:
    await events_db.insert(
        event=event,
        repository=fake_repository.repo,
        pull_request=github_types.GitHubPullRequestNumber(random.randint(1, 100)),
        metadata=metadata,
        trigger="Rule: my rule",
    )


async def assert_base_event(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    # base event inserted
    result = await db.execute(
        sqlalchemy.select(func.count()).select_from(evt_model.Event)
    )
    assert result.scalar() == 1

    # GithubRepository and account inserted
    event = await db.scalar(sqlalchemy.select(evt_model.Event))
    assert event is not None
    assert event.repository.id == fake_repository.repo["id"]
    assert event.repository.owner.id == fake_repository.repo["owner"]["id"]


async def test_event_not_supported(fake_repository: context.Repository) -> None:
    with pytest.raises(events_db.EventNotHandled) as e:
        await events_db.insert(
            event="event.not.supported",  # type: ignore [arg-type]
            repository=fake_repository.repo,
            pull_request=github_types.GitHubPullRequestNumber(random.randint(1, 100)),
            metadata={},
            trigger="Rule: my rule",
        )
    assert "Event 'event.not.supported' not supported in database" == str(e.value)


async def test_event_action_assign_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.assign",
        signals.EventAssignMetadata(
            {"added": ["leo", "charly", "guillaume"], "removed": ["damien", "fabien"]}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionAssign))
    assert event is not None
    assert set(event.added) == {"leo", "charly", "guillaume"}
    assert set(event.removed) == {"damien", "fabien"}


async def test_event_post_check_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.post_check",
        signals.EventPostCheckMetadata(
            {
                "title": "Rule: my check (post_check)",
                "conclusion": "success",
                "summary": "abc" * 100,
            }
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionPostCheck))
    assert event is not None
    assert event.title == "Rule: my check (post_check)"
    assert event.conclusion == "success"


async def test_event_action_copy_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.copy",
        signals.EventCopyMetadata(
            {"to": "test_branch", "pull_request_number": 123, "conflicts": False}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionCopy))
    assert event is not None
    assert event.to == "test_branch"
    assert event.pull_request_number == 123
    assert event.conflicts is False


async def test_event_action_comment_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.comment",
        signals.EventCommentMetadata(message="hello world"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionComment))
    assert event is not None
    assert event.message == "hello world"


async def test_event_action_close_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.close",
        signals.EventCommentMetadata(message="goodbye world"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionClose))
    assert event is not None
    assert event.message == "goodbye world"


async def test_event_action_delete_head_branch_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.delete_head_branch",
        signals.EventDeleteHeadBranchMetadata(branch="change_branch"),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionDeleteHeadBranch))
    assert event is not None
    assert event.branch == "change_branch"


async def test_event_action_dismiss_reviews_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.dismiss_reviews",
        signals.EventDismissReviewsMetadata(users=["leo", "charly", "guillaume"]),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionDismissReviews))
    assert event is not None
    assert set(event.users) == {"leo", "charly", "guillaume"}


async def test_event_action_backport_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.backport",
        signals.EventCopyMetadata(
            {"to": "stable_branch", "pull_request_number": 456, "conflicts": True}
        ),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionBackport))
    assert event is not None
    assert event.to == "stable_branch"
    assert event.pull_request_number == 456
    assert event.conflicts is True


async def test_event_action_edit_consistency(
    db: sqlalchemy.ext.asyncio.AsyncSession, fake_repository: context.Repository
) -> None:
    await insert_event(
        fake_repository,
        "action.edit",
        signals.EventEditMetadata(draft=True),
    )

    await assert_base_event(db, fake_repository)
    event = await db.scalar(sqlalchemy.select(evt_model.EventActionEdit))
    assert event is not None
    assert event.draft is True
