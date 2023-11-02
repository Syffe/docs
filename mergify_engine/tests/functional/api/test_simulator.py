import anys
import pytest

from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestApiSimulator(base.FunctionalTestBase):
    @pytest.mark.subscription(subscription.Features.CUSTOM_CHECKS)
    async def test_simulator_with_token(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "simulator",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        mergify_yaml = f"""pull_request_rules:
  - name: a lot of stuff
    conditions:
      - base={self.main_branch_name}
      - or:
        - schedule=MON-SUN 00:00-23:59
        - label=foobar
    actions:
      post_check:
        summary: "Did you check {{{{ author }}}}?"
        success_conditions:
          - "author=foobar"
          - "label=whatever"
          - "base={self.main_branch_name}"
      comment:
        message: "Welcome {{{{ author }}}}."
      edit:
        draft: True
      assign:
        users:
          - mergify-test1
"""

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 200, r.json()
        assert r.json()["title"] == "The configuration is valid"
        assert r.json()["summary"] == ""

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 200, r.text
        assert r.json()["title"] == "1 rule matches", r.json()
        assert (
            r.json()["summary"].split("<hr />")[0]
            == f"""### Rule: a lot of stuff (post_check, comment, edit, assign)
- [X] `base={self.main_branch_name}`
- [X] any of:
  - [X] `schedule=MON-SUN 00:00-23:59`
  - [ ] `label=foobar`

**post_check action configuration:**
```
always_show: false
neutral_conditions: null
success_conditions: |-
  - [ ] `author=foobar`
  - [ ] `label=whatever`
  - [X] `base={self.main_branch_name}`
summary: Did you check {p['user']['login']}?
title: '''a lot of stuff'' failed'
```

**comment action configuration:**
```
bot_account: null
message: Welcome {p['user']['login']}.
```

**edit action configuration:**
```
bot_account: null
draft: true
```

**assign action configuration:**
```
users_to_add:
- mergify-test1
users_to_remove: []
```

"""
        )

        mergify_yaml = """pull_request_rules:
  - name: remove label conflict
    conditions:
      - -conflict
    actions:
      label:
        remove:
          - conflict:
"""

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 422, r.json()
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body", "mergify_yml"],
                    "msg": "expected str @ pull_request_rules → item 0 → actions → label → remove → item 0",
                    "type": "mergify_config_error",
                }
            ]
        }

        mergify_yaml = """pull_request_rules:
  - name: remove label conflict
    conditions:
      - -conflict:
    actions:
      label:
        remove:
          - conflict:
"""

        r = await self.admin_app.post(
            f"/v1/repos/{p['base']['repo']['owner']['login']}/{p['base']['repo']['name']}/simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 422, r.json()
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body", "mergify_yml"],
                    "msg": "expected str @ pull_request_rules → item 0 → actions → label → remove → item 0",
                    "type": "mergify_config_error",
                },
                {
                    "loc": ["body", "mergify_yml"],
                    "msg": "extra keys not allowed @ pull_request_rules → item 0 → conditions → item 0 → -conflict",
                    "type": "mergify_config_error",
                },
            ]
        }

    async def test_simulator_with_wrong_pull_request_url(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "simulator",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))

        mergify_yaml = f"""pull_request_rules:
  - name: assign
    conditions:
      - base={self.main_branch_name}
    actions:
      assign:
        users:
          - mergify-test1
"""
        resp = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/42424242/simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert resp.status_code == 404

    async def test_simulator_invalid_json(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "simulator",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"merge": {}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/simulator",
            json={"mergify_yml": "- no\n* way"},
        )
        assert r.status_code == 422
        assert r.json() == {
            "detail": [
                {
                    "loc": ["body", "mergify_yml"],
                    "msg": """Invalid YAML @ line 2, column 2
```
while scanning an alias
  in "<unicode string>", line 2, column 1
did not find expected alphabetic or numeric character
  in "<unicode string>", line 2, column 2
```""",
                    "type": "mergify_config_error",
                }
            ],
        }

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/simulator",
            json={"invalid": "json"},
        )
        assert r.status_code == 422
        assert r.json() == {
            "detail": [
                {
                    "input": {"invalid": "json"},
                    "loc": ["body", "mergify_yml"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        }


class TestApiConfigurationSimulator(base.FunctionalTestBase):
    @pytest.mark.subscription(subscription.Features.CUSTOM_CHECKS)
    async def test_config_editor_simulator(self) -> None:
        await self.setup_repo()
        p = await self.create_pr()

        mergify_yaml = f"""
pull_request_rules:
  - name: a lot of stuff
    conditions:
      - base={self.main_branch_name}
      - or:
        - schedule=MON-SUN 00:00-23:59
        - label=foobar
    actions:
      post_check:
        summary: "Did you check {{{{ author }}}}?"
        success_conditions:
          - "author=foobar"
          - "label=whatever"
          - "base={self.main_branch_name}"
      comment:
        message: "Welcome {{{{ author }}}}."
      edit:
        draft: True
      assign:
        users:
          - mergify-test1
"""

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/configuration-simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 200, r.json()
        assert r.json()["message"] == "The configuration is valid"

        r = await self.admin_app.post(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/configuration-simulator",
            json={"mergify_yml": mergify_yaml},
        )
        assert r.status_code == 200, r.json()
        assert r.json()["message"] == "The configuration is valid"
        assert len(r.json()["pull_request_rules"]) == 1
        pull_request_rule = r.json()["pull_request_rules"][0]
        assert pull_request_rule["name"] == "a lot of stuff"
        assert len(pull_request_rule["actions"]) == 4
        actions = pull_request_rule["actions"]
        assert actions["post_check"] == {
            "always_show": False,
            "success_conditions": anys.ANY_DICT,
            "neutral_conditions": None,
            "summary": anys.AnyFullmatch(r"Did you check .+\[bot\]\?"),
            "title": "'a lot of stuff' failed",
        }
        assert actions["comment"] == {
            "message": anys.AnyFullmatch(r"Welcome .+\[bot\]\."),
            "bot_account": None,
        }
        assert actions["edit"] == {"draft": True, "bot_account": None}
        assert actions["assign"] == {
            "users_to_add": ["mergify-test1"],
            "users_to_remove": [],
        }
