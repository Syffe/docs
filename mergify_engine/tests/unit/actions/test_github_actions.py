from unittest import mock

import pytest
import voluptuous

from mergify_engine.actions import github_actions
from mergify_engine.rules.config import mergify as mergify_conf


async def test_github_actions_get_config() -> None:
    await mergify_conf.get_mergify_config_from_dict(
        mock.MagicMock(),
        {
            "pull_request_rules": [
                {
                    "name": "Trigger GHA",
                    "conditions": ["label=ready"],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "my_first_workflow.yaml",
                                        "inputs": {
                                            "input1": "value1",
                                            "input2": "value2",
                                        },
                                    },
                                    {
                                        "workflow": "my_second_workflow.yaml",
                                        "inputs": {
                                            "input1": "value1",
                                            "input2": "value2",
                                        },
                                    },
                                ]
                            }
                        }
                    },
                },
            ]
        },
        "",
    )


async def test_github_actions_get_schema_error() -> None:
    # Too many dispatched events
    with pytest.raises(voluptuous.Invalid) as e:
        github_actions.GhaAction(
            {
                "workflow": {
                    "dispatch": [
                        {"workflow": f"workflow{i}.yaml"}
                        for i in range(github_actions.MAX_DISPATCHED_EVENTS + 1)
                    ]
                }
            }
        )
    assert (
        str(e.value)
        == f"length of value must be at most {github_actions.MAX_DISPATCHED_EVENTS} "
        "for dictionary value @ data['workflow']['dispatch']"
    )

    # Too many inputs (api limitation)
    with pytest.raises(voluptuous.Invalid) as e:
        github_actions.GhaAction(
            {
                "workflow": {
                    "dispatch": [
                        {
                            "workflow": "workflow.yaml",
                            "inputs": {f"input{i}": f"value{i}" for i in range(11)},
                        }
                    ]
                }
            }
        )
    assert (
        str(e.value)
        == "length of value must be at most 10 for dictionary value @ data['workflow']['dispatch'][0]['inputs']"
    )
