import asyncio
from collections import abc
import functools
import typing

import click

from mergify_engine import service


P = typing.ParamSpec("P")
R = typing.TypeVar("R")


@click.group()
def admin() -> None:
    pass


def async_admin_command(
    func: abc.Callable[P, abc.Coroutine[typing.Any, typing.Any, R]]
) -> abc.Callable[..., None]:
    @admin.command()
    @functools.wraps(func)
    def inner_func(*args: P.args, **kwargs: P.kwargs) -> R:
        service.setup(func.__name__, stdout_logging_only=True, dump_config=False)
        return asyncio.run(func(*args, **kwargs))

    return inner_func


# NOTE(sileht): ensure click found all commands
from mergify_engine.console_scripts import account_suspend  # noqa
from mergify_engine.console_scripts import merge_queue  # noqa
from mergify_engine.console_scripts import redis_dump  # noqa
from mergify_engine.console_scripts import refresher  # noqa
from mergify_engine.console_scripts import subscription  # noqa
