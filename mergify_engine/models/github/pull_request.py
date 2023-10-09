import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.ci import pull_registries


class PullRequest(models.Base):
    __tablename__ = "pull_request"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    title: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 5 )",
    )
    state: orm.Mapped[github_types.GitHubPullRequestState] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull: pull_registries.PullRequest,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                id=pull.id,
                number=pull.number,
                title=pull.title,
                state=pull.state,
            )
            .on_conflict_do_update(
                index_elements=[cls.id],
                set_={"number": pull.number, "title": pull.title},
            )
        )
        await session.execute(sql)
