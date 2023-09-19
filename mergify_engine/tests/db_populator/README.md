# DbPopulator: Simplifying Database Population for Testing

DbPopulator is a powerful tool designed to streamline the process of registering and loading SQLAlchemy data scenarios into databases for testing purposes.

## Getting Started

To create and load data scenarios, you need to follow these steps:

### Create a Scenario

Begin by creating a new Python file within the `mergify_engine/tests/db_populator` directory. Your file structure should resemble the following:

```python
import sqlalchemy.ext.asyncio

from mergify_engine.tests.db_populator import DbPopulator


class MyNewDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        """
        Implement your SQLAlchemy data creation code here.

        Example:
        session.add(...)
        session.execute(...)
        """
        pass

```

### Create a Meta-dataset

Meta-dataset are datasets that groups several datasets around a theme (e.g. Account and Repo). To create one you just have to do this:

```python
import sqlalchemy.ext.asyncio

from mergify_engine.tests.db_populator import DbPopulator

class MyNewMetaDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"MyNewDataset", "MyOtherNewDataset"})

```

### Use DbPopulator

DbPopulator can be used in two main ways: as a loader and as a fixture.

#### Using DbPopulator as a Loader

Import `DbPopulator` into your test and call the `load` method to populate the database.

```python
import sqlalchemy.ext.asyncio

from mergify_engine.tests.db_populator import DbPopulator

async def my_test(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    # Load all registered scenarios
    DbPopulator.load(db)

async def my_test_2(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    # Load specific scenarios
    DbPopulator.load(db, {"MyNewDataset"})

    # Add other datasets on the fly
    DbPopulator.load(db, {"MyOtherNewDataset"})

    # Load multiple datasets at once
    DbPopulator.load(db, {"AwesomeDataset", "DummyDataset"})
```

#### Using DbPopulator as a Fixture

Instead of using the standard `db` fixture, you can utilize the `populated_db` fixture to automatically populate your SQLAlchemy session with all registered scenarios.
If you want to use `populated_db` with a subset of datasets use `@pytest.mark.populated_db_datasets(...)`.

```python
import sqlalchemy.ext.asyncio

async def my_test_3(populated_db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
      # All registered dataset will be loaded in DB
      pass

@pytest.mark.populated_db_datasets("MyNewDataset", "AwesomeDataset")
async def my_test_4(populated_db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
      # Only MyNewDataset and AwesomeDataset will be loaded in DB
      pass
```
