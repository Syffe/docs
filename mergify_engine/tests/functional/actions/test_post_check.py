import operator

import anys
import pytest

from mergify_engine import context
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine import yaml
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
                            ],
                        },
                    },
                },
            ],
        }
        unrelated_branch = self.get_full_branch_name("unrelated")
        await self.setup_repo(yaml.dump(rules), test_branches=[unrelated_branch])
        match_p = await self.create_pr(message="Fixes MRGFY-123")
        unmatch_p = await self.create_pr()
        unrelated_p = await self.create_pr(base=unrelated_branch)

        await self.create_review(
            match_p["number"],
            oauth_token=settings.TESTING_ORG_ADMIN_PERSONAL_TOKEN,
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
            await match_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "success" == match_check["conclusion"]

        # ensure a failure check is posted on related branch
        unmatch_ctxt = context.Context(self.repository_ctxt, unmatch_p, [])
        unmatch_sorted_checks = sorted(
            await unmatch_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(unmatch_sorted_checks) == 2
        unmatch_check = unmatch_sorted_checks[0]
        assert "failure" == unmatch_check["conclusion"]
        assert "'body need sentry ticket' failed" == unmatch_check["output"]["title"]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{match_p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "event": "action.post_check",
                    "type": "action.post_check",
                    "metadata": {
                        "conclusion": "success",
                        "summary": "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`",
                        "title": "'body need sentry ticket' succeeded",
                    },
                    "repository": match_p["base"]["repo"]["full_name"],
                    "pull_request": match_p["number"],
                    "base_ref": self.main_branch_name,
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": None,
        }

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{unmatch_p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "event": "action.post_check",
                    "type": "action.post_check",
                    "metadata": {
                        "conclusion": "failure",
                        "summary": "- [ ] `approved-reviews-by=@testing`\n"
                        "- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`",
                        "title": "'body need sentry ticket' failed",
                    },
                    "repository": unmatch_p["base"]["repo"]["full_name"],
                    "pull_request": unmatch_p["number"],
                    "base_ref": self.main_branch_name,
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": None,
        }

        # Check it moves to failure and the event logs is filled accordingly
        await self.add_label(match_p["number"], "ignore-guideline")
        await self.run_engine()

        match_ctxt = context.Context(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "failure" == match_check["conclusion"]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{match_p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "event": "action.post_check",
                    "type": "action.post_check",
                    "metadata": {
                        "conclusion": "failure",
                        "summary": "- [ ] `-label=ignore-guideline`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`",
                        "title": "'body need sentry ticket' failed",
                    },
                    "pull_request": match_p["number"],
                    "base_ref": self.main_branch_name,
                    "repository": self.repository_ctxt.repo["full_name"],
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: body need sentry ticket",
                },
                {
                    "id": anys.ANY_INT,
                    "event": "action.post_check",
                    "type": "action.post_check",
                    "metadata": {
                        "conclusion": "success",
                        "summary": "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        "- [X] `approved-reviews-by=@testing`\n"
                        "- [X] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`",
                        "title": "'body need sentry ticket' succeeded",
                    },
                    "repository": match_p["base"]["repo"]["full_name"],
                    "pull_request": match_p["number"],
                    "base_ref": self.main_branch_name,
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 2,
            "total": None,
        }

    async def test_checks_with_neutral_conditions(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "be neutral",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {
                        "post_check": {
                            "neutral_conditions": [
                                "label=be neutral",
                            ],
                        },
                    },
                },
            ],
        }
        unrelated_branch = self.get_full_branch_name("unrelated")
        await self.setup_repo(yaml.dump(rules), test_branches=[unrelated_branch])
        match_pr = await self.create_pr()
        unrelated_pr = await self.create_pr(base=unrelated_branch)

        await self.run_engine()

        # ensure no check is posted on unrelated branch
        unrelated_ctxt = context.Context(self.repository_ctxt, unrelated_pr, [])
        assert len(await unrelated_ctxt.pull_engine_check_runs) == 1

        # ensure a failure check is posted on related branch
        match_ctxt = context.Context(self.repository_ctxt, match_pr, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "failure" == match_check["conclusion"]
        assert "'be neutral' failed" == match_check["output"]["title"]

        # Now add the label
        await self.add_label(match_pr["number"], "be neutral")

        await self.run_engine()

        # ensure a neutral check is posted on related branch
        match_ctxt = context.Context(self.repository_ctxt, match_pr, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "neutral" == match_check["conclusion"]

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
                },
            ],
        }

        unrelated_branch = self.get_full_branch_name("unrelated")
        await self.setup_repo(yaml.dump(rules), test_branches=[unrelated_branch])
        match_p = await self.create_pr(message="Fixes MRGFY-123")
        unmatch_p = await self.create_pr()
        unrelated_p = await self.create_pr(
            base=unrelated_branch,
            message="Fixes MRGFY-123",
        )
        await self.run_engine()

        # ensure check is also posted on unrelated branch as failure
        unrelated_ctxt = context.Context(self.repository_ctxt, unrelated_p, [])
        unrelated_sorted_checks = sorted(
            await unrelated_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(unrelated_sorted_checks) == 2
        unrelated_check = unrelated_sorted_checks[0]
        assert "failure" == unrelated_check["conclusion"]

        # ensure a success check is posted on related branch
        match_ctxt = context.Context(self.repository_ctxt, match_p, [])
        match_sorted_checks = sorted(
            await match_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(match_sorted_checks) == 2
        match_check = match_sorted_checks[0]
        assert "success" == match_check["conclusion"]

        # ensure a failure check is posted on related branch
        unmatch_ctxt = context.Context(self.repository_ctxt, unmatch_p, [])
        unmatch_sorted_checks = sorted(
            await unmatch_ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(unmatch_sorted_checks) == 2
        unmatch_check = unmatch_sorted_checks[0]
        assert "failure" == unmatch_check["conclusion"]
        assert "'body need sentry ticket' failed" == unmatch_check["output"]["title"]

        r = await self.admin_app.get(
            f"/v1/repos/{settings.TESTING_ORGANIZATION_NAME}/{self.RECORD_CONFIG['repository_name']}/pulls/{unmatch_p['number']}/events",
        )
        assert r.status_code == 200
        assert r.json() == {
            "events": [
                {
                    "id": anys.ANY_INT,
                    "event": "action.post_check",
                    "type": "action.post_check",
                    "metadata": {
                        "conclusion": "failure",
                        "summary": "- [ ] `body~=(?m)^(Fixes|Related|Closes) "
                        "(MERGIFY-ENGINE|MRGFY)-`\n"
                        "- [X] `#body<4096`\n"
                        "- [X] `#files<100`\n"
                        "- [X] `#title<100`\n"
                        "- [X] `#title>10`\n"
                        "- [X] `-label=ignore-guideline`\n"
                        f"- [X] `base={self.main_branch_name}`",
                        "title": "'body need sentry ticket' failed",
                    },
                    "repository": unmatch_p["base"]["repo"]["full_name"],
                    "pull_request": unmatch_p["number"],
                    "base_ref": self.main_branch_name,
                    "timestamp": anys.ANY_AWARE_DATETIME_STR,
                    "received_at": anys.ANY_AWARE_DATETIME_STR,
                    "trigger": "Rule: body need sentry ticket",
                },
            ],
            "per_page": 10,
            "size": 1,
            "total": None,
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
                                "Pull request #{{ number }} does"
                                "{% if not check_succeed %} not{% endif %}"
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
                        },
                    },
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        p = await self.get_pull(p["number"])

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 2
        check = sorted_checks[0]
        assert (
            f"Pull request #{p['number']} does not follow our guideline"
            == check["output"]["title"]
        )
        assert "failure" == check["conclusion"]


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestPostCheckActionNoSub(base.FunctionalTestBase):
    async def test_checks_feature_disabled(self) -> None:
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
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()
        p = await self.get_pull(p["number"])

        ctxt = context.Context(self.repository_ctxt, p, [])
        sorted_checks = sorted(
            await ctxt.pull_engine_check_runs,
            key=operator.itemgetter("name"),
        )
        assert len(sorted_checks) == 1
        check = sorted_checks[0]
        assert "action_required" == check["conclusion"]
        assert (
            "The current Mergify configuration is invalid" == check["output"]["title"]
        )
        assert "Custom checks are disabled" in check["output"]["summary"]
