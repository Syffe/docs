import asyncio
import pathlib

from mergify_engine import database
from mergify_engine import models
from mergify_engine import settings
from mergify_engine.models import manage
from mergify_engine.tests import utils


HEADERS = """
SELECT pg_catalog.set_config('search_path', 'public', false);
CREATE EXTENSION anon CASCADE;
SELECT anon.init();

CREATE SCHEMA IF NOT EXISTS custom_masks;
SECURITY LABEL FOR anon ON SCHEMA custom_masks IS 'TRUSTED';

DROP FUNCTION IF EXISTS custom_masks.lorem_ipsum_array;
CREATE FUNCTION custom_masks.lorem_ipsum_array(min int, max int, characters integer)
RETURNS text[]
VOLATILE
LANGUAGE plpgsql
AS $$
DECLARE
  result text[];
  count integer;
BEGIN
    count := anon.random_int_between(min, max);
    FOR counter IN 1..count LOOP
        result := result || anon.lorem_ipsum(characters := characters);
    END LOOP;
    RETURN result;
END
$$;

-- To validate the function content syntax
select custom_masks.lorem_ipsum_array(1, 2, 2);


"""


class MissingAnonymizedFunction(Exception):
    pass


async def gen_postgresql_anonymized_rules() -> None:
    rules = HEADERS
    for table in models.Base.metadata.sorted_tables:
        for col in table.c:
            if "anonymizer_config" not in col.dialect_kwargs:
                raise MissingAnonymizedFunction(
                    f"{table.name}.{col.name} does not have anonymizer config yet"
                )

            anonymizer_config = col.dialect_kwargs["anonymizer_config"]
            if anonymizer_config is None:
                continue

            if anonymizer_config.startswith("custom_masks."):
                mask_type = "FUNCTION"
            else:
                mask_type = "VALUE"

            rules += f"SECURITY LABEL FOR anon ON COLUMN {table.name}.{col.name} IS 'MASKED WITH {mask_type} {anonymizer_config}';\n"

    with open("postgresql_anonymizer_rules.sql", "w") as f:
        f.write(rules)

    database.init_sqlalchemy("test")
    await manage.create_all()
    utils.dump_schema(
        settings.DATABASE_URL.path[1:], pathlib.Path("empty_database.sql")
    )


if __name__ == "__main__":
    asyncio.run(gen_postgresql_anonymized_rules())
