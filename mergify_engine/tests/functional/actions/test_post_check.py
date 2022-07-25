# -*- encoding: utf-8 -*-
#
# Copyright © 2020–2022 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import operator
from unittest import mock

import yaml

from mergify_engine import config
from mergify_engine import context
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


class TestPostCheckAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_checks_with_conditions(self):
        rules = {
            "pull_request_rules": [
                {
                    "name": "body need sentry ticket",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {
                        "post_check": {
                            "success_conditions": [
                                "#title>10",
                                "#title<100",
                                "#body<4096",
                                "#files<100",
                                "body~=(?m)^(Fixes|Related|Closes) (MERGIFY-ENGINE|MRGFY)-",
                                "-label=ignore-guideline",
                            ]
                        }
                    },
                }
            ]
        }
        unrelated_branch = self.get_full_branch_name("unrelated")
        await self.setup_repo(yaml.dump(rules), test_branches=[unrelated_branch])
        match_p = await self.create_pr(message="Fixes MRGFY-123")
        unmatch_p = await self.create_pr()
        unrelated_p = await self.create_pr(base=unrelated_branch)
        await self.run_engine()

        # ensure no check is posted on unrelated branch
        unrelated_ctxt = await context.Context.create(
            self.repository_ctxt, unrelated_p, []
        )
        assert len(await unrelated_ctxt.pull_engine_check_runs) == 1

        # ensure a success check is posted on related branch
        match_ctxt = await context.Context.create(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "success" == match_check["conclusion"]

        # ensure a failure check is posted on related branch
        unmatch_ctxt = await context.Context.create(self.repository_ctxt, unmatch_p, [])
        unmatch_sorted_checks = sorted(
            await unmatch_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(unmatch_sorted_checks) == 2
        unmatch_check = unmatch_sorted_checks[0]
        assert "failure" == unmatch_check["conclusion"]
        assert "'body need sentry ticket' failed" == unmatch_check["output"]["title"]

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{unmatch_p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.post_check",
                    "metadata": {
                        "conclusion": "failure",
                        "summary": "- [X] `#title>10`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n"
                        "- [X] `-label=ignore-guideline`\n",
                        "title": "'body need sentry ticket' failed",
                    },
                    "repository": unmatch_p["base"]["repo"]["full_name"],
                    "pull_request": unmatch_p["number"],
                    "timestamp": mock.ANY,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    async def test_checks_default(self):
        rules = {
            "pull_request_rules": [
                {
                    "name": "body need sentry ticket",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#title>10",
                        "#title<50",
                        "#body<4096",
                        "#files<100",
                        "body~=(?m)^(Fixes|Related|Closes) (MERGIFY-ENGINE|MRGFY)-",
                        "-label=ignore-guideline",
                    ],
                    "actions": {"post_check": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        p = await self.get_pull(p["number"])

        ctxt = await context.Context.create(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(sorted_checks) == 2
        check = sorted_checks[0]
        assert "failure" == check["conclusion"]
        assert "'body need sentry ticket' failed" == check["output"]["title"]

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/events",
            headers={
                "Authorization": f"bearer {self.api_key_admin}",
                "Content-type": "application/json",
            },
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "event": "action.post_check",
                    "metadata": {
                        "conclusion": "failure",
                        "summary": "- [X] "
                        f"`base={self.main_branch_name}`\n"
                        f"- [X] `#title>10`\n"
                        f"- [ ] `#title<50`\n"
                        f"- [X] `#body<4096`\n"
                        f"- [X] `#files<100`\n"
                        f"- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        f"(MERGIFY-ENGINE|MRGFY)-`\n"
                        f"- [X] `-label=ignore-guideline`\n",
                        "title": "'body need sentry ticket' failed",
                    },
                    "repository": p["base"]["repo"]["full_name"],
                    "pull_request": p["number"],
                    "timestamp": mock.ANY,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

    async def test_checks_custom(self):
        rules = {
            "pull_request_rules": [
                {
                    "name": "body need sentry ticket",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#title>10",
                        "#title<50",
                        "#body<4096",
                        "#files<100",
                        "body~=(?m)^(Fixes|Related|Closes) (MERGIFY-ENGINE|MRGFY)-",
                        "-label=ignore-guideline",
                    ],
                    "actions": {
                        "post_check": {
                            "title": (
                                "Pull request #{{ number }} does"  # noqa: FS003
                                "{% if not check_succeed %} not{% endif %}"  # noqa: FS003
                                " follow our guideline"
                            ),
                            "summary": """
Full markdown of my awesome pull request guideline:

* Mandatory stuff about title
* Need a ticket number
* Please explain what your trying to achieve

Rule list:

{{ check_conditions }}

""",
                        }
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        p = await self.get_pull(p["number"])

        ctxt = await context.Context.create(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(sorted_checks) == 2
        check = sorted_checks[0]
        assert (
            f"Pull request #{p['number']} does not follow our guideline"
            == check["output"]["title"]
        )
        assert "failure" == check["conclusion"]


class TestPostCheckActionNoSub(base.FunctionalTestBase):
    async def test_checks_feature_disabled(self) -> None:
        self.subscription = subscription.Subscription(
            self.redis_links.cache,
            config.TESTING_INSTALLATION_ID,
            "You're not nice",
            frozenset(
                getattr(subscription.Features, f)
                for f in subscription.Features.__members__
                if f is not subscription.Features.CUSTOM_CHECKS.value
            )
            if self.SUBSCRIPTION_ACTIVE
            else frozenset([subscription.Features.PUBLIC_REPOSITORY]),
        )
        await self.subscription._save_subscription_to_cache()

        rules = {
            "pull_request_rules": [
                {
                    "name": "body need sentry ticket",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#title>10",
                        "#title<50",
                        "#body<4096",
                        "#files<100",
                        "body~=(?m)^(Fixes|Related|Closes) (MERGIFY-ENGINE|MRGFY)-",
                        "-label=ignore-guideline",
                    ],
                    "actions": {"post_check": {}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        p = await self.get_pull(p["number"])

        ctxt = await context.Context.create(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(sorted_checks) == 2
        check = sorted_checks[0]
        assert "action_required" == check["conclusion"]
        assert "Custom checks are disabled" == check["output"]["title"]
