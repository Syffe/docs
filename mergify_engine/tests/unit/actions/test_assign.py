from unittest import mock

import pytest
import voluptuous

from mergify_engine import github_types
from mergify_engine.actions import assign
from mergify_engine.tests.unit import conftest


def test_assign_get_schema() -> None:
    schema = {"users": ["{{ author }}"]}
    result = assign.AssignAction(schema)
    assert result.config["users"] == schema["users"]

    schema = {"users": ["foo-42"]}
    result = assign.AssignAction(schema)
    assert result.config["users"] == schema["users"]

    schema = {"add_users": ["{{ author }}"]}
    result = assign.AssignAction(schema)
    assert result.config["add_users"] == schema["add_users"]

    schema = {"add_users": ["foo-42"]}
    result = assign.AssignAction(schema)
    assert result.config["add_users"] == schema["add_users"]

    schema = {"remove_users": ["{{ author }}"]}
    result = assign.AssignAction(schema)
    assert result.config["remove_users"] == schema["remove_users"]

    schema = {"remove_users": ["foo-42"]}
    result = assign.AssignAction(schema)
    assert result.config["remove_users"] == schema["remove_users"]


def test_assign_get_schema_with_wrong_template() -> None:
    with pytest.raises(voluptuous.Invalid) as e:
        assign.AssignAction({"users": ["{{ foo }}"]})
    assert str(e.value) == "Template syntax error @ data['users'][0]"

    with pytest.raises(voluptuous.Invalid) as e:
        assign.AssignAction({"add_users": ["{{ foo }}"]})
    assert str(e.value) == "Template syntax error @ data['add_users'][0]"

    with pytest.raises(voluptuous.Invalid) as e:
        assign.AssignAction({"remove_users": ["{{ foo }}"]})
    assert str(e.value) == "Template syntax error @ data['remove_users'][0]"


async def test_unassign_assignees(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    schema = {"remove_users": ["{{ assignee[0] }}"]}
    action = assign.AssignAction(schema)

    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(
        github_types.GitHubPullRequestNumber(1), assignees=[{"login": "me"}]
    )
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())

    assert action.executor.config["users_to_remove"] == {"me"}
