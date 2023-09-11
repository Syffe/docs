from __future__ import annotations

import typing

import sqlalchemy.ext.asyncio
import sqlalchemy.orm


class DbPopulator:
    id_auto_increment: typing.ClassVar[dict[str, int]] = {}
    dataset_registry: typing.ClassVar[dict[str, type[DbPopulator]]] = {}
    loaded_dadaset: typing.ClassVar[set[str]] = set()

    def __init_subclass__(cls, *args: typing.Any, **kwargs: typing.Any) -> None:
        if cls.__name__ in cls.dataset_registry:
            raise ValueError(
                f"A dataset named '{cls.__name__}' has already been registered."
            )
        cls.dataset_registry[cls.__name__] = cls
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    def next_id(cls, model: type[sqlalchemy.orm.decl_api.DeclarativeBase]) -> int:
        current_id = cls.id_auto_increment.setdefault(model.__tablename__, 0)
        next_id = current_id + 1
        cls.id_auto_increment[model.__tablename__] = next_id
        return next_id

    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        raise NotImplementedError("_load must be reimplemented on DbPopulator dataset")

    @classmethod
    async def load(
        cls, session: sqlalchemy.ext.asyncio.AsyncSession, datasets: set[str]
    ) -> None:
        if not datasets:
            raise RuntimeError("DbPopulator used without datasets")

        for dataset in datasets:
            if dataset not in cls.loaded_dadaset:
                await cls.dataset_registry[dataset]._load(session)
                cls.loaded_dadaset.add(dataset)


# NOTE(Kontrolix): Import here to register dataset
import mergify_engine.tests.db_populator.account_and_repo  # noqa: E402
import mergify_engine.tests.db_populator.colliding_repo_name  # noqa: E402
import mergify_engine.tests.db_populator.one_account_and_one_repo  # noqa: E402
import mergify_engine.tests.db_populator.workflow_job  # noqa: E402, F401
