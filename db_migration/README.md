# Database schema migration

We use Alembic and SQLAlchemy to handle database schema migrations.

## Create a revision

The following command creates a revision file in the `/db_migration/versions/` directory.

```
poe revision -m "What have changed"
```

You can complete the file and then check that the models and the migrations are synchronized.

```
poe test mergify_engine/tests/unit/test_database.py::test_migration
```

### Autogenerate a revision

You can automatically generate a revision with Alembic. You just need an up-to-date database.

```bash
# Activate the virtual environment and start containers
poe shell
# Apply all existing migrations to Postgres database
mergify-database-update
# Autogenerate a revision based on SQLAlchemy models
poe revision --autogenerate -m "What have changed"
```

## Run a local database

The command `poe shell` creates database containers and activates the virtual environment. You can then create all the database objects using one of the following options.

- `mergify-database-create` creates all the database objects using the SQLAlchemy models.
- `mergify-database-update` creates all the database objects using the Alembic migrations.
