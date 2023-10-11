import asyncio

import daiquiri
import pydantic
import pydantic_core
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import database
from mergify_engine import events as evt_utils
from mergify_engine.models import events


LOG = daiquiri.getLogger(__name__)

TA = pydantic.TypeAdapter(evt_utils.Event)


async def events_db_test() -> None:
    async with database.create_session() as session:
        stmt = (
            sqlalchemy.select(events.Event)
            .order_by(events.Event.id)
            .options(
                orm.selectin_polymorphic(events.Event, events.Event.__subclasses__())
            )
            .execution_options(yield_per=100)
        )

        evts = await session.stream_scalars(stmt)
        async for evt in evts:
            try:
                TA.validate_python(evt_utils.format_event_item_response(evt))
            except pydantic_core.ValidationError as e:
                LOG.error("Validation error on event %s: %s", evt.id, e.errors())


async def main_events() -> None:
    LOG.info("Start validating events objects from DB")
    database.init_sqlalchemy("test_events")
    await events_db_test()
    LOG.info("Validation ended")


def main() -> None:
    asyncio.run(main_events())
