import asyncio
from collections import abc
import functools
import typing

import click

from mergify_engine import service


P = typing.ParamSpec("P")
R = typing.TypeVar("R")


@click.group()
def admin_cli() -> None:
    pass


def async_command(
    func: abc.Callable[P, abc.Coroutine[typing.Any, typing.Any, R]],
) -> abc.Callable[..., None]:
    @admin_cli.command()
    @functools.wraps(func)
    def inner_func(*args: P.args, **kwargs: P.kwargs) -> R:
        service.setup(func.__name__, stdout_logging_only=True, dump_config=False)
        return asyncio.run(func(*args, **kwargs))

    return inner_func


@click.group()
def devtools() -> None:
    pass


# NOTE(sileht): ensure click found all commands
from mergify_engine.console_scripts import account_suspend  # noqa: E402,F401
from mergify_engine.console_scripts import debug  # noqa: E402,F401
from mergify_engine.console_scripts import merge_queue  # noqa: E402,F401
from mergify_engine.console_scripts import openapi_spec_generator  # noqa: E402,F401
from mergify_engine.console_scripts import redis_dump  # noqa: E402,F401
from mergify_engine.console_scripts import refresher  # noqa: E402,F401
from mergify_engine.console_scripts import stream  # noqa: E402,F401
from mergify_engine.console_scripts import subscription  # noqa: E402,F401
