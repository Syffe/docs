import operator
from unittest import mock

from mergify_engine import config
from mergify_engine import context
from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


class TestPostCheckAction(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_checks_with_conditions(self) -> None:
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
                                "approved-reviews-by=@testing",
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

        await self.create_review(
            match_p["number"], oauth_token=config.ORG_ADMIN_PERSONAL_TOKEN
        )

        await self.run_engine()

        # refresh the pr to ensure we don't have the event-logs entry twice
        await self.send_refresh(match_p["number"])
        await self.run_engine()

        # ensure no check is posted on unrelated branch
        unrelated_ctxt = context.Context(self.repository_ctxt, unrelated_p, [])
        assert len(await unrelated_ctxt.pull_engine_check_runs) == 1

        # ensure a success check is posted on related branch
        match_ctxt = context.Context(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "success" == match_check["conclusion"]

        # ensure a failure check is posted on related branch
        unmatch_ctxt = context.Context(self.repository_ctxt, unmatch_p, [])
        unmatch_sorted_checks = sorted(
            await unmatch_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(unmatch_sorted_checks) == 2
        unmatch_check = unmatch_sorted_checks[0]
        assert "failure" == unmatch_check["conclusion"]
        assert "'body need sentry ticket' failed" == unmatch_check["output"]["title"]

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{match_p['number']}/events",
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
                        "conclusion": "success",
                        "summary": "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n",
                        "title": "'body need sentry ticket' succeeded",
                    },
                    "repository": match_p["base"]["repo"]["full_name"],
                    "pull_request": match_p["number"],
                    "timestamp": mock.ANY,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": 1,
        }

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
                        "summary": "- [ ] `approved-reviews-by=@testing`\n"
                        "- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
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

        # Check it moves to failure and the event logs is filled accordingly
        await self.add_label(match_p["number"], "ignore-guideline")
        await self.run_engine()

        match_ctxt = context.Context(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "failure" == match_check["conclusion"]

        r = await self.app.get(
            f"/v1/repos/{config.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{match_p['number']}/events",
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
                        "summary": "- [ ] `-label=ignore-guideline`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n",
                        "title": "'body need sentry ticket' failed",
                    },
                    "pull_request": match_p["number"],
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": mock.ANY,
                    "trigger": "Rule: body need sentry ticket",
                },
                {
                    "event": "action.post_check",
                    "metadata": {
                        "conclusion": "success",
                        "summary": "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n",
                        "title": "'body need sentry ticket' succeeded",
                    },
                    "repository": match_p["base"]["repo"]["full_name"],
                    "pull_request": match_p["number"],
                    "timestamp": mock.ANY,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": 2,
        }

    async def test_checks_default(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "body need sentry ticket",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "#title>10",
                        "#title<100",
                        "#body<4096",
                        "#files<100",
                        "body~=(?m)^(Fixes|Related|Closes) (MERGIFY-ENGINE|MRGFY)-",
                        "-label=ignore-guideline",
                    ],
                    "actions": {"post_check": {}},
                }
            ]
        }

        unrelated_branch = self.get_full_branch_name("unrelated")
        await self.setup_repo(yaml.dump(rules), test_branches=[unrelated_branch])
        match_p = await self.create_pr(message="Fixes MRGFY-123")
        unmatch_p = await self.create_pr()
        unrelated_p = await self.create_pr(
            base=unrelated_branch, message="Fixes MRGFY-123"
        )
        await self.run_engine()

        # ensure check is also posted on unrelated branch as failure
        unrelated_ctxt = context.Context(self.repository_ctxt, unrelated_p, [])
        unrelated_sorted_checks = sorted(
            await unrelated_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(unrelated_sorted_checks) == 2
        unrelated_check = unrelated_sorted_checks[0]
        assert "failure" == unrelated_check["conclusion"]

        # ensure a success check is posted on related branch
        match_ctxt = context.Context(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "success" == match_check["conclusion"]

        # ensure a failure check is posted on related branch
        unmatch_ctxt = context.Context(self.repository_ctxt, unmatch_p, [])
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
                        "summary": "- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        f"- [X] `base={self.main_branch_name}`\n",
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

    async def test_checks_custom(self) -> None:
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

        ctxt = context.Context(self.repository_ctxt, p, [])
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

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs, key=operator.itemgetter("name")
        )
        assert len(sorted_checks) == 1
        check = sorted_checks[0]
        assert "action_required" == check["conclusion"]
        assert (
            "The current Mergify configuration is invalid" == check["output"]["title"]
        )
        assert "Custom checks are disabled" in check["output"]["summary"]
