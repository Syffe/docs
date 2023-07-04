import random

import sqlalchemy
from sqlalchemy import func
import sqlalchemy.ext.asyncio

from mergify_engine import context
from mergify_engine import database
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

    async with database.create_session() as session:
        # base event inserted
        result = await session.execute(
            sqlalchemy.select(func.count()).select_from(evt_model.Event)
        )
        assert result.scalar() == 1

        # GithubRepository and account inserted
        event = await session.scalar(sqlalchemy.select(evt_model.Event))
        assert event is not None
        assert event.repository.id == fake_repository.repo["id"]
        assert event.repository.owner.id == fake_repository.repo["owner"]["id"]

        # action assign
        event = await session.scalar(sqlalchemy.select(evt_model.EventActionAssign))
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

    event = await db.scalar(sqlalchemy.select(evt_model.EventActionPostCheck))
    assert event is not None
    assert event.title == "Rule: my check (post_check)"
    assert event.conclusion == "success"
