import typing

import voluptuous

from mergify_engine import actions as actions_mod


def get_defaults_schema() -> dict[typing.Any, typing.Any]:
    return {
        # FIXME(sileht): actions.get_action_schemas() returns only actions Actions
        # and not command only, since only refresh is command only and it doesn't
        # have options it's not a big deal.
        voluptuous.Required("actions", default={}): actions_mod.get_action_schemas(),
    }
