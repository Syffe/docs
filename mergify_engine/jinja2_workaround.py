from collections import abc

import jinja2.async_utils
import jinja2.environment
import jinja2.filters


# FIXME(sileht): remove me when Jinja2 unique filter() is fixed and released
#  https://github.com/pallets/jinja/pull/1782
if not hasattr(jinja2.filters, "sync_do_unique"):

    @jinja2.async_utils.async_variant(jinja2.filters.do_unique)  # type: ignore[no-untyped-call,misc]
    async def do_unique(
        environment: jinja2.environment.Environment,
        value: abc.AsyncIterable[jinja2.filters.V] | abc.Iterable[jinja2.filters.V],
        case_sensitive: bool = False,
        attribute: str | int | None = None,
    ) -> abc.Iterator[jinja2.filters.V]:
        return jinja2.filters.do_unique(
            environment,
            await jinja2.async_utils.auto_to_list(value),
            case_sensitive,
            attribute,
        )

    jinja2.filters.FILTERS["unique"] = do_unique
