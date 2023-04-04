import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine.ci import models
from mergify_engine.ci import pull_registries
from mergify_engine.models import github_actions as sql_models


async def test_insert(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    pull = models.PullRequest(id=1, number=2, title="some title", state="open")
    registry = pull_registries.PostgresPullRequestRegistry()

    await registry.insert(pull)

    sql = sqlalchemy.select(sql_models.PullRequest)
    result = await db.scalars(sql)
    pulls = list(result)
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 2
    assert actual_pull.title == "some title"

    # Doing it twice should update the record
    await registry.insert(pull)

    sql = sqlalchemy.select(sql_models.PullRequest)
    result = await db.scalars(sql)
    pulls = list(result)
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 2
    assert actual_pull.title == "some title"
