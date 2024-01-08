import io
import typing
from unittest import mock
import zipfile

import respx

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils
from mergify_engine.yaml import yaml


class GhaActionTestBase(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    RUNNER: typing.ClassVar[str] = "ubuntu-latest"

    async def test_gha_success(self) -> None:
        # NOTE(lecrepont01): newly created workflows in the base branch won't be
        # found unless they are executed at least once that is why they also
        # have a "pull_request" condition to be triggered once at pr creation
        pep8_workflow = {
            "name": "pep8",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {
                    "inputs": {
                        "some_boolean": {"required": True, "type": "boolean"},
                    },
                },
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Test dispatched workflow inputs",
                            "run": "echo some_boolean=${{ inputs.some_boolean }}",
                        },
                    ],
                },
            },
        }

        ci_workflow = {
            "name": "continuous_integration",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {
                    "inputs": {
                        "some_string": {"required": True, "type": "string"},
                        "some_number": {"required": True, "type": "number"},
                    },
                },
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Test dispatched workflow inputs",
                            "run": "echo some_string=${{ inputs.some_string }}, some_number=${{ inputs.some_number }}",
                        },
                    ],
                },
            },
        }

        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "pep8_workflow.yaml",
                                        "inputs": {
                                            "some_boolean": True,
                                        },
                                        "ref": self.main_branch_name,
                                    },
                                    {
                                        "workflow": "ci_workflow.yaml",
                                        "inputs": {
                                            "some_string": "Hello",
                                            "some_number": "11",
                                        },
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/pep8_workflow.yaml": yaml.dump(pep8_workflow),
                ".github/workflows/ci_workflow.yaml": yaml.dump(ci_workflow),
            },
        )

        p = await self.create_pr()

        await self.wait_for_all(
            [
                {
                    "event_type": "workflow_run",
                    "payload": tests_utils.get_workflow_run_event_payload(
                        name="pep8",
                        action="completed",
                    ),
                },
                {
                    "event_type": "workflow_run",
                    "payload": tests_utils.get_workflow_run_event_payload(
                        name="continuous_integration",
                        action="completed",
                    ),
                },
            ],
        )

        # Add label and run the action
        await self.add_label(p["number"], "dispatch")

        await self.run_engine()
        await self.wait_for_all(
            [
                {
                    "event_type": "workflow_run",
                    "payload": tests_utils.get_workflow_run_event_payload(
                        name="pep8",
                        action="completed",
                    ),
                },
                {
                    "event_type": "workflow_run",
                    "payload": tests_utils.get_workflow_run_event_payload(
                        name="continuous_integration",
                        action="completed",
                    ),
                },
            ],
        )

    async def test_gha_unknown_workflow(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch unknown workflow",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "unknown.yaml",
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch unknown workflow (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Failed to dispatch workflow `unknown.yaml`"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == "Failed to dispatch workflow `unknown.yaml`. Not Found."
        )

    async def test_gha_workflow_no_dispatch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "no_dispatch_workflow.yaml",
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        no_dispatch = {
            "name": "no_dispatch",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Workflow missing dispatch trigger",
                            "run": "echo yolo",
                        },
                    ],
                },
            },
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/no_dispatch_workflow.yaml": yaml.dump(no_dispatch),
            },
        )
        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch GHA (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Failed to dispatch workflow `no_dispatch_workflow.yaml`"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == "Failed to dispatch workflow `no_dispatch_workflow.yaml`. Workflow does not have 'workflow_dispatch' trigger."
        )

    async def test_gha_required_inputs_not_provided(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "inputs_required.yaml",
                                        "inputs": {},
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        inputs_required = {
            "name": "inputs_required",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {
                    "inputs": {
                        "some_string": {"required": True, "type": "string"},
                    },
                },
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Workflow missing dispatch trigger",
                            "run": "echo yolo",
                        },
                    ],
                },
            },
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/inputs_required.yaml": yaml.dump(inputs_required),
            },
        )
        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch GHA (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Failed to dispatch workflow `inputs_required.yaml`"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == "Failed to dispatch workflow `inputs_required.yaml`. Required input 'some_string' not provided."
        )

    async def test_gha_multiple_workflows_dispatches_containing_error(self) -> None:
        success_workflow = {
            "name": "multiple_workflows_success",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {"inputs": {}},
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Test dispatched workflow inputs",
                            "run": "echo tintin&milou",
                        },
                    ],
                },
            },
        }

        error_workflow = {
            "name": "multiple_workflows_error",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {
                    "inputs": {"some_string": {"required": True, "type": "string"}},
                },
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Test dispatched workflow inputs",
                            "run": "echo fails anyway",
                        },
                    ],
                },
            },
        }

        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "multiple_workflows_success.yaml",
                                        "ref": self.main_branch_name,
                                    },
                                    {
                                        "workflow": "multiple_workflows_error.yaml",
                                        "inputs": {
                                            "some_string": 666,
                                        },
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/multiple_workflows_success.yaml": yaml.dump(
                    success_workflow,
                ),
                ".github/workflows/multiple_workflows_error.yaml": yaml.dump(
                    error_workflow,
                ),
            },
        )

        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch GHA (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"] == "Some workflow dispatch failed"
        )

        if settings.IS_GHES:
            github_action_error = """Invalid request.

For 'additionalProperties', 666 is not a string."""
        else:
            github_action_error = "Invalid value for input 'some_string'."

        assert (
            check_run["check_run"]["output"]["summary"]
            == f"""Workflow dispatch failed:
- Failed to dispatch workflow `multiple_workflows_error.yaml`. {github_action_error}

Workflow successfully dispatched:
- `multiple_workflows_success.yaml`"""
        )

    async def test_gha_dispatch_forbidden(self) -> None:
        forbidden_workflow = {
            "name": "workflow_run_forbidden",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {"inputs": {}},
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Test dispatched workflow inputs",
                            "run": "echo fails anyway",
                        },
                    ],
                },
            },
        }

        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "workflow_dispatch_forbidden.yaml",
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/workflow_dispatch_forbidden.yaml": yaml.dump(
                    forbidden_workflow,
                ),
            },
        )

        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")

        # NOTE(lecrepont01): mock a user who did not grant appropriate permissions
        with respx.mock(
            base_url=settings.GITHUB_REST_API_URL,
            assert_all_called=False,
        ) as respx_mock, mock.patch(
            "mergify_engine.context.Context.github_actions_controllable",
            return_value=False,
        ):
            respx_mock.post(
                url__regex=f"/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}"
                rf"/actions/workflows/workflow_dispatch_forbidden.yaml/dispatches",
            ).respond(403)
            respx_mock.route(host=settings.GITHUB_REST_API_HOST).pass_through()
            await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch GHA (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Unauthorized to dispatch workflow"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == f"""Failed to dispatch workflow `workflow_dispatch_forbidden.yaml`. The new Mergify permissions must be accepted to dispatch actions.
You can accept them at {settings.DASHBOARD_UI_FRONT_URL}"""
        )

    async def test_gha_with_template_input(self) -> None:
        dynamic_workflow = {
            "name": "dynamic_workflow",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
                "workflow_dispatch": {
                    "inputs": {
                        "author": {"required": True, "type": "string"},
                    },
                },
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Show author",
                            "run": "echo author=${{ inputs.author }}",
                        },
                    ],
                },
            },
        }

        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "dynamic_workflow.yaml",
                                        "inputs": {
                                            "author": "{{author}}",
                                        },
                                        "ref": self.main_branch_name,
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/dynamic_workflow.yaml": yaml.dump(dynamic_workflow),
            },
        )

        p = await self.create_pr()
        await self.wait_for("workflow_job", {"action": "completed"})

        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        dispatched_event = typing.cast(
            github_types.GitHubEventWorkflowJob,
            await self.wait_for("workflow_job", {"action": "completed"}),
        )
        assert dispatched_event["workflow_job"] is not None

        log = await self.client_integration.get(
            f"{self.url_origin}/actions/runs/{dispatched_event['workflow_job']['run_id']}/logs",
        )

        with io.BytesIO(log.content) as stream:
            with zipfile.ZipFile(stream, "r") as zip_file:
                for file_name in zip_file.namelist():
                    if "show author" in file_name.lower():
                        file_content = zip_file.read(file_name).decode("utf-8")
                        break

        assert f"author={self.RECORD_CONFIG['app_user_login']}" in file_content

    async def test_gha_specified_ref(self) -> None:
        workflow_with_ref = {
            "name": "workflow_with_ref",
            "on": {
                "pull_request": {"branches": self.main_branch_name},
            },
            "jobs": {
                "test-dispatch": {
                    "runs-on": self.RUNNER,
                    "steps": [
                        {
                            "name": "Hello world",
                            "run": "echo hello world",
                        },
                    ],
                },
            },
        }

        rules = {
            "pull_request_rules": [
                {
                    "name": "Dispatch GHA",
                    "conditions": [
                        "label=dispatch",
                    ],
                    "actions": {
                        "github_actions": {
                            "workflow": {
                                "dispatch": [
                                    {
                                        "workflow": "workflow_with_ref.yaml",
                                        "ref": "branch-does-not-exist",
                                    },
                                ],
                            },
                        },
                    },
                },
            ],
        }

        await self.setup_repo(
            yaml.dump(rules),
            files={
                ".github/workflows/workflow_with_ref.yaml": yaml.dump(
                    workflow_with_ref,
                ),
            },
        )

        p = await self.create_pr()
        await self.add_label(p["number"], "dispatch")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            name="Rule: Dispatch GHA (github_actions)",
            conclusion="failure",
        )
        assert (
            check_run["check_run"]["output"]["title"]
            == "Failed to dispatch workflow `workflow_with_ref.yaml`"
        )
        assert (
            check_run["check_run"]["output"]["summary"]
            == "Failed to dispatch workflow `workflow_with_ref.yaml`. No ref found for: branch-does-not-exist."
        )
