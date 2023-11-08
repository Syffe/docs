import json
import os
import typing
from unittest import mock

import anys
import pytest

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.rules.config import queue_rules as qr_config
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestConfiguration(base.FunctionalTestBase):
    async def test_invalid_configuration_fixed_by_pull_request(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "wrong key": 123,
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        rules["pull_request_rules"] = [
            {
                "name": "foobar",
                "conditions": ["label!=wip"],
                "actions": {"merge": {}},
            }
        ]
        p = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        summary_check = checks[0]
        assert (
            summary_check["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        assert summary_check["output"]["summary"] == (
            "* extra keys not allowed @ pull_request_rules → item 0 → wrong key\n"
            "* required key not provided @ pull_request_rules → item 0 → actions\n"
            "* required key not provided @ pull_request_rules → item 0 → conditions"
        )
        conf_change_check = checks[1]
        assert conf_change_check["conclusion"] == check_api.Conclusion.SUCCESS.value
        assert (
            conf_change_check["output"]["title"]
            == "The new Mergify configuration is valid"
        )

    async def test_invalid_configuration_in_repository(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "wrong key": 123,
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        check = checks[0]
        assert (
            check["output"]["title"] == "The current Mergify configuration is invalid"
        )
        assert check["output"]["summary"] == (
            "* extra keys not allowed @ pull_request_rules → item 0 → wrong key\n"
            "* required key not provided @ pull_request_rules → item 0 → actions\n"
            "* required key not provided @ pull_request_rules → item 0 → conditions"
        )

    async def test_pull_request_with_invalid_team_name_in_new_configuration(
        self,
    ) -> None:
        teams = await self.get_teams()

        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "request_reviews": {"teams": [teams[0]["slug"].upper()]}
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        rules["pull_request_rules"][0]["actions"]["request_reviews"]["teams"] = [  # type: ignore[index]
            "@atchoum",
            "atchoum",
        ]

        p = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        config_changed_check = await ctxt.get_engine_check_run(
            constants.CONFIGURATION_CHANGED_CHECK_NAME
        )
        assert config_changed_check is not None
        assert (
            config_changed_check["output"]["title"]
            == "The new Mergify configuration is invalid"
        )
        assert (
            config_changed_check["output"]["summary"]
            == """In the rule `foobar`, the action `request_reviews` configuration is invalid:
Invalid requested teams
Team `@atchoum` does not exist or has not access to this repository
Team `atchoum` does not exist or has not access to this repository
"""
        )

    async def test_invalid_priority_rules_config(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "priority_rules": [
                        {
                            "name": "hotfix PR detected",
                            "conditions": [
                                "label=hotfix",
                            ],
                            "priority": "whatever",
                        },
                        {
                            "name": "default priority",
                            "conditions": [
                                "-label=hotfix",
                            ],
                            "priority": "false text",
                        },
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                }
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed", status="completed", conclusion="failure"
        )

        assert (
            "The current Mergify configuration is invalid"
            == check_run["check_run"]["output"]["title"]
        )

    async def test_invalid_yaml_configuration_in_repository(self) -> None:
        await self.setup_repo("- this is totally invalid yaml\\n\n  - *\n*")
        p = await self.create_pr()

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        check = checks[0]
        assert (
            check["output"]["title"] == "The current Mergify configuration is invalid"
        )
        # Use startswith because the message has some weird \x00 char
        assert (
            check["output"]["summary"]
            == """Invalid YAML @ line 3, column 2
```
while scanning an alias
  in "<unicode string>", line 3, column 1
did not find expected alphabetic or numeric character
  in "<unicode string>", line 3, column 2
```"""
        )

        check_id = check["id"]
        annotations = [
            annotation
            async for annotation in ctxt.client.items(
                f"{ctxt.base_url}/check-runs/{check_id}/annotations",
                api_version="antiope",
                resource_name="annotations",
                page_limit=10,
            )
        ]
        assert annotations == [
            {
                "path": ".mergify.yml",
                "blob_href": mock.ANY,
                "start_line": 3,
                "start_column": 2,
                "end_line": 3,
                "end_column": 2,
                "annotation_level": "failure",
                "title": "Invalid YAML",
                "message": mock.ANY,
                "raw_details": None,
            }
        ]

    async def test_cached_config_changes_when_push_event_received(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": ["base=main"],
                    "actions": {
                        "comment": {"message": "hello"},
                    },
                },
            ],
        }

        rules_default: dict[str, list[str]] = {"pull_request_rules": []}

        await self.setup_repo(yaml.dump(rules_default))
        assert self.git.repository is not None
        await self.repository_ctxt.get_mergify_config_file()
        await self.run_engine()

        cached_config_file = await self.repository_ctxt.get_cached_config_file()
        assert cached_config_file is not None
        assert cached_config_file["decoded_content"] == yaml.dump(rules_default)

        # Change unrelated file and config stay cached
        with open(self.git.repository + "/random", "w") as f:
            f.write("yo")
        await self.git("add", "random")
        await self.git("commit", "--no-edit", "-m", "random update")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        cached_config_file = await self.repository_ctxt.get_cached_config_file()
        assert cached_config_file is not None
        assert cached_config_file["decoded_content"] == yaml.dump(rules_default)

        # Change config file and config cache gets cleaned
        with open(self.git.repository + "/.mergify.yml", "w") as f:
            f.write(yaml.dump(rules))
        await self.git("add", ".mergify.yml")
        await self.git("commit", "--no-edit", "-m", "conf update")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        cached_config_file = await self.repository_ctxt.get_cached_config_file()
        assert cached_config_file is None

        # Open a PR and it's cached again
        p = await self.create_pr()
        context.Context(self.repository_ctxt, p, [])
        await self.run_engine()

        cached_config_file = await self.repository_ctxt.get_cached_config_file()
        assert cached_config_file is not None
        assert cached_config_file["decoded_content"] == yaml.dump(rules)

    async def test_no_configuration_changed_with_weird_base_sha(self) -> None:
        # Test special case where the configuration is changed around the a
        # pull request creation.
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": ["base=main"],
                    "actions": {
                        "comment": {"message": "hello"},
                    },
                },
            ],
        }
        # config has been update in the meantime
        await self.setup_repo(yaml.dump({"pull_request_rules": []}))
        assert self.git.repository is not None
        with open(self.git.repository + "/.mergify.yml", "wb") as f:
            f.write(yaml.dump(rules).encode())
        await self.git("add", ".mergify.yml")
        await self.git("commit", "--no-edit", "-m", "conf update")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.run_engine()

        await self.git("branch", "save-point")
        # Create a PR on outdated repo to get a wierd base.sha
        await self.git("reset", "--hard", "HEAD^", "--")
        # Create a lot of file to ignore optimization
        p = await self.create_pr(
            git_tree_ready=True, files={f"f{i}": "data" for i in range(0, 160)}
        )
        ctxt = context.Context(self.repository_ctxt, p, [])
        await self.run_engine()

        await self.git("checkout", "save-point", "-b", self.main_branch_name)
        with open(self.git.repository + "/.mergify.yml", "wb") as f:
            f.write(yaml.dump({}).encode())
        await self.git("add", ".mergify.yml")
        await self.git("commit", "--no-edit", "-m", "conf update")
        await self.git("push", "--quiet", "origin", self.main_branch_name)
        await self.run_engine()

        # we didn't change the pull request no configuration must be detected
        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 1
        assert checks[0]["output"]["title"] == "no rules match, no planned actions"

    async def test_invalid_configuration_in_pull_request(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": ["base=main"],
                    "actions": {
                        "comment": {"message": "hello"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr(files={".mergify.yml": "not valid"})

        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        assert len(checks) == 2
        assert checks[0]["output"]["title"] == "no rules match, no planned actions"
        assert (
            checks[1]["output"]["title"] == "The new Mergify configuration is invalid"
        )
        assert checks[1]["output"]["summary"] == "expected a dictionary"

    async def test_disallowed_change_mergify_yml(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "nothing",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))
        rules["pull_request_rules"].append(
            {"name": "foobar", "conditions": ["label!=wip"], "actions": {"merge": {}}}
        )
        p1 = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p1, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["title"] == "1 potential rule"
        assert """### Rule: nothing (merge)
- [ ] `-mergify-configuration-changed` [:pushpin: merge -> allow_merging_configuration_change setting requirement]
""" in summary["output"]["summary"]

    async def test_allowed_change_mergify_yml(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "nothing",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {"allow_merging_configuration_change": True}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))
        rules["pull_request_rules"].append(
            {"name": "foobar", "conditions": ["label!=wip"], "actions": {"merge": {}}}
        )
        p1 = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p1, [])
        summary = await ctxt.get_engine_check_run("Rule: nothing (merge)")
        assert summary is not None
        assert (
            summary["output"]["title"]
            == "The pull request has been merged automatically"
        )

    async def test_change_mergify_yml_in_meantime_on_big_pull_request(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "nothing",
                    "conditions": [f"base!={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))
        rules["pull_request_rules"].append(
            {"name": "foobar", "conditions": ["label!=wip"], "actions": {"merge": {}}}
        )
        p = await self.create_pr(files={f"f{i}": "data" for i in range(0, 160)})
        await self.run_engine()

        p_change_config = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.merge_pull(p_change_config["number"])
        await self.wait_for("push", {"ref": f"refs/heads/{self.main_branch_name}"})
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["title"] == "1 rule matches"

    async def test_invalid_action_option(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": ["base=main"],
                    "actions": {
                        "comment": {"message": "hello", "unknown": "hello"},
                    },
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            summary["output"]["title"] == "The current Mergify configuration is invalid"
        )
        assert (
            summary["output"]["summary"]
            == "extra keys not allowed @ pull_request_rules → item 0 → actions → comment → unknown"
        )

    @pytest.mark.subscription(
        subscription.Features.WORKFLOW_AUTOMATION,
        subscription.Features.MERGE_QUEUE,
    )
    async def test_configuration_changed(self) -> None:
        rules: dict[str, list[dict[str, typing.Any]]] = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "foobar",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {
                        "queue": {},
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p1 = await self.create_pr()
        await self.run_engine()

        await self.add_label(p1["number"], "queue")
        await self.run_engine()

        rules["pull_request_rules"].append(
            {
                "name": "label on queued",
                "conditions": ["queue-position>=0"],
                "actions": {
                    "label": {"toggle": ["queued"]},
                },
            }
        )

        p2 = await self.create_pr(files={".mergify.yml": yaml.dump(rules)})
        await self.merge_pull(p2["number"])
        await self.run_engine()

        p2 = await self.create_pr()
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for_pull_request("labeled", p2["number"])

        await self.send_refresh(p1["number"])
        await self.run_engine()

        await self.wait_for_pull_request("labeled", p1["number"])

    async def test_no_configuration(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()
        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            "no rules configured, just listening for commands"
            == summary["output"]["title"]
        )

    async def test_configuration_moved(self) -> None:
        await self.setup_repo("")

        os.mkdir(self.git.repository + "/.github")
        await self.git("mv", ".mergify.yml", ".github/mergify.yml")
        await self.create_pr(git_tree_ready=True)
        await self.run_engine()

        summary_event = await self.wait_for_check_run(name=constants.SUMMARY_NAME)
        summary = summary_event["check_run"]
        assert (
            "no rules configured, just listening for commands"
            in summary["output"]["title"]
        )

    async def test_configuration_moved_and_disabled(self) -> None:
        await self.setup_repo("")
        await self.git("mv", ".mergify.yml", "disabled.yml")
        p = await self.create_pr(git_tree_ready=True)
        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            "no rules configured, just listening for commands"
            in summary["output"]["title"]
        )
        additionnal_check = await ctxt.get_engine_check_run(
            "Configuration has been deleted"
        )
        assert additionnal_check is not None

    async def test_configuration_deleted(self) -> None:
        await self.setup_repo("")
        await self.git("rm", "-rf", ".mergify.yml")
        p = await self.create_pr(git_tree_ready=True)
        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            "no rules configured, just listening for commands"
            in summary["output"]["title"]
        )
        additionnal_check = await ctxt.get_engine_check_run(
            "Configuration has been deleted"
        )
        assert additionnal_check is not None

    async def test_multiple_configurations(self) -> None:
        await self.setup_repo(files={".mergify.yml": ""})
        p = await self.create_pr(
            files={".github/mergify.yml": "pull_request_rules: []"}
        )
        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            "Multiple Mergify configurations have been found in the repository"
            == summary["output"]["title"]
        )
        assert ".mergify.yml" in summary["output"]["summary"]
        assert ".github/mergify.yml" in summary["output"]["summary"]

    async def test_empty_configuration(self) -> None:
        await self.setup_repo("")
        p = await self.create_pr()
        await self.run_engine()

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert (
            "no rules configured, just listening for commands"
            == summary["output"]["title"]
        )

    async def test_merge_with_not_merged_attribute(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": [f"base={self.main_branch_name}", "-merged"],
                    "actions": {"merge": {}},
                },
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})

        assert await self.is_pull_merged(p["number"])

        p = await self.get_pull(p["number"])
        ctxt = context.Context(self.repository_ctxt, p, [])
        for check in await ctxt.pull_check_runs:
            if check["name"] == "Rule: merge on main (merge)":
                assert (
                    "The pull request has been merged automatically"
                    == check["output"]["title"]
                )
                assert (
                    f"The pull request has been merged automatically at *{ctxt.pull['merge_commit_sha']}*"
                    == check["output"]["summary"]
                )
                break
        else:
            pytest.fail("Merge check not found")

    @pytest.mark.subscription(subscription.Features.CUSTOM_CHECKS)
    async def test_extend_config_file_ok(self) -> None:
        # TODO: mock extends installation request
        # Notes(Jules): this config is stored here: https://github.com/mergifyio-testing/.github/blob/main/.mergify.yml
        mergify_config = {"extends": ".github"}

        await self.setup_repo(yaml.dump(mergify_config))
        p = await self.create_pr()
        await self.add_label(p["number"], "comment")

        real_client_get = github.AsyncGitHubClient.get

        async def mocked_client_get(self, url, *args, **kwargs):  # type: ignore[no-untyped-def]
            if (
                f"/repos/{settings.TESTING_ORGANIZATION_NAME}/.github/installation"
                in url
            ):
                m = mock.Mock()
                m.status_code = 200
                return m
            return await real_client_get(self, url, *args, **kwargs)

        with mock.patch.object(github.AsyncGitHubClient, "get", mocked_client_get):
            await self.run_engine()

            ctxt = context.Context(self.repository_ctxt, p, [])
            config = await ctxt.repository.get_mergify_config()

        assert len(config["queue_rules"]) == 1
        assert len(config["pull_request_rules"].rules) == 2

        # Make sure the installation status is stored in redis
        cache_value = await self.redis_links.cache.get(
            context.Repository.get_mergify_installation_cache_key(
                f"{settings.TESTING_ORGANIZATION_NAME}/.github"
            )
        )
        assert cache_value is not None
        assert json.loads(cache_value) == {"installed": True, "error": None}

        # Make sure that a "installation_repositories" event cleans up the mergify installation status
        # NOTE: This is impossible to test with a real event, so we need to manually send one
        await github_events.clean_and_fill_caches(
            self.redis_links,
            "installation_repositories",
            "123eventid",
            # This is a slim event with just the data required to make sure the
            # authentication cache clearing mechanism work.
            github_types.GitHubEventInstallationRepositories(  # type: ignore[typeddict-item]
                {
                    "installation": {
                        "account": {
                            "id": settings.TESTING_ORGANIZATION_ID,
                            "login": settings.TESTING_ORGANIZATION_NAME,
                        }
                    },
                    "repositories_added": [
                        {
                            "full_name": f"{settings.TESTING_ORGANIZATION_NAME}/.github",
                        }
                    ],
                    "repositories_removed": [],
                }
            ),
        )

        cache_value = await self.redis_links.cache.get(
            context.Repository.get_mergify_installation_cache_key(
                f"{settings.TESTING_ORGANIZATION_NAME}/.github"
            )
        )
        assert cache_value is None

    @pytest.mark.subscription(subscription.Features.CUSTOM_CHECKS)
    async def test_extend_config_file_merge_ok(self) -> None:
        # Notes(Jules): this config is stored here: https://github.com/mergifyio-testing/.github/blob/main/.mergify.yml
        mergify_config = {
            "extends": ".github",
            "pull_request_rules": [
                {
                    "name": "merge on main",
                    "conditions": ["base=new_rule"],
                    "actions": {"merge": {}},
                },
            ],
            "queue_rules": [
                {
                    "name": "new_rule",
                    "merge_conditions": ["schedule: MON-FRI 08:00-17:00"],
                    "allow_inplace_checks": False,
                }
            ],
            "commands_restrictions": {"copy": {"conditions": ["base=new_rule"]}},
        }

        await self.setup_repo(yaml.dump(mergify_config))
        p = await self.create_pr()
        await self.add_label(p["number"], "comment")

        real_client_get = github.AsyncGitHubClient.get

        async def mocked_client_get(self, url, *args, **kwargs):  # type: ignore[no-untyped-def]
            if (
                f"/repos/{settings.TESTING_ORGANIZATION_NAME}/.github/installation"
                in url
            ):
                m = mock.Mock()
                m.status_code = 200
                return m
            return await real_client_get(self, url, *args, **kwargs)

        with mock.patch.object(github.AsyncGitHubClient, "get", mocked_client_get):
            await self.run_engine()
            ctxt = context.Context(self.repository_ctxt, p, [])
            config = await ctxt.repository.get_mergify_config()

        assert len(config["queue_rules"].rules) == 2
        # One from default config, one from extends, one from this repo
        assert len(config["pull_request_rules"].rules) == 3
        assert len(config["commands_restrictions"]) == 9

        rule = config["pull_request_rules"].rules[0].conditions.condition.conditions[0]
        assert isinstance(rule, rules.conditions.RuleCondition)
        assert str(rule) == "base=new_rule"

        rule = config["queue_rules"][
            qr_config.QueueName("new_rule")
        ].merge_conditions.condition.conditions[0]
        assert isinstance(rule, rules.conditions.RuleCondition)
        assert str(rule) == "schedule: MON-FRI 08:00-17:00"

        rule = config["commands_restrictions"]["copy"][
            "conditions"
        ].condition.conditions[0]
        assert isinstance(rule, rules.conditions.RuleCondition)
        assert str(rule) == "base=new_rule"

    async def test_extend_config_repo_does_not_exist(self) -> None:
        rules = {"extends": "this_repo_does_not_exist"}

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = await ctxt.pull_engine_check_runs
        summary_check = checks[0]
        assert (
            summary_check["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        assert (
            "404 Client Error: Not Found for url" in summary_check["output"]["summary"]
        )
        assert (
            "mergifyio-testing/this_repo_does_not_exist`"
            in summary_check["output"]["summary"]
        )
        assert "@ extends" in summary_check["output"]["summary"]
        assert (
            "Extended configuration repository `this_repo_does_not_exist` was not found. This repository doesn't exist or Mergify is not installed on it."
            in summary_check["output"]["summary"]
        )

    async def test_extended_repo_does_not_have_mergify_enabled(self) -> None:
        # repo-without-mergify = repo in mergifyio-testing that doesn't, and shouldn't,
        # have the testing app installed on it.
        rules = {"extends": "repo-without-mergify"}

        await self.setup_repo(yaml.dump(rules))
        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(name="Summary", conclusion="failure")
        assert (
            check_run["check_run"]["output"]["title"]
            == "The current Mergify configuration is invalid"
        )

        assert (
            "Extended configuration repository `repo-without-mergify` doesn't have Mergify installed on it. Mergify needs to be enabled on extended repositories to be able to detect configuration changes properly."
            in check_run["check_run"]["output"]["summary"]
        )

        cache_value = await self.redis_links.cache.get(
            context.Repository.get_mergify_installation_cache_key(
                f"{settings.TESTING_ORGANIZATION_NAME}/repo-without-mergify"
            )
        )
        assert cache_value is not None
        assert json.loads(cache_value) == {"installed": False, "error": anys.ANY_STR}

    async def test_extended_repo_does_not_have_a_configuration_file(self) -> None:
        rules = {"extends": ".github"}

        await self.setup_repo(yaml.dump(rules))

        real_get_mergify_config_file = context.Repository.get_mergify_config_file

        async def mocked_get_mergify_config_file(self):  # type: ignore[no-untyped-def]
            if self.repo["name"] == ".github":
                return None

            return await real_get_mergify_config_file(self)

        await self.create_pr()
        with mock.patch.object(
            context.Repository,
            "get_mergify_config_file",
            mocked_get_mergify_config_file,
        ), mock.patch.object(
            context.Repository, "is_mergify_installed", return_value={"installed": True}
        ):
            await self.run_engine()

        check_run = await self.wait_for_check_run(name="Summary", conclusion="failure")
        assert (
            check_run["check_run"]["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        assert (
            "Extended configuration repository `.github` doesn't have a Mergify configuration file."
            in check_run["check_run"]["output"]["summary"]
        )
