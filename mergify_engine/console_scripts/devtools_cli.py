import click


@click.group()
def devtools_cli() -> None:
    pass


# NOTE(sileht): ensure click found all commands
from mergify_engine.console_scripts import openapi_spec_generator  # noqa
