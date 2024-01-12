import logging
import re
from urllib import parse

import pytest

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


LOG = logging.getLogger(__name__)


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestSummary(base.FunctionalTestBase):
    """Mergify engine summary tests.

    GitHub resources are slow, so we must reduce the number
    of scenario as much as possible for now.
    """

    async def test_failed_base_changeable_attributes_rules_in_not_applicable_summary_section_with_basic_conditions(
        self,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "invalid rule dummy",
                    "conditions": [
                        "base=dummy",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "valid rule main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])

        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["title"] == "1 potential rule"
        assert summary["output"]["summary"] is not None
        assert "1 not applicable rule" in summary["output"]["summary"]

    async def test_failed_base_changeable_attributes_rules_in_not_applicable_summary_section_with_or_condition(
        self,
    ) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "invalid rule dummy",
                    "conditions": [
                        "base=dummy",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "valid rule label",
                    "conditions": [
                        {
                            "or": [
                                "label=test",
                                "base=dummy",
                            ],
                        },
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.add_label(p["number"], "test")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])

        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["title"] == "1 potential rule"
        assert summary["output"]["summary"] is not None
        assert "1 not applicable rule" in summary["output"]["summary"]

    @pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
    async def test_pull_request_rules_order_0_depth(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "test",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=test",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert f"""### Rule: test (merge)
- [ ] `label=test`
- [X] `-closed` [📌 merge requirement]
- [X] `-conflict` [📌 merge requirement]
- [X] `-draft` [📌 merge requirement]
- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]
- [X] `base={self.main_branch_name}`
""" in summary["output"]["summary"]

        await self.add_label(p["number"], "test")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert f"""### Rule: test (merge)
- [ ] `-closed` [📌 merge requirement]
- [X] `-conflict` [📌 merge requirement]
- [X] `-draft` [📌 merge requirement]
- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]
- [X] `base={self.main_branch_name}`
- [X] `label=test`
""" in summary["output"]["summary"]

    async def test_pull_request_rules_order_operator_and(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "test",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=test",
                        {
                            "and": [
                                "label=test2",
                                "label=test3",
                            ],
                        },
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"### Rule: test (merge)\n- [ ] `label=test`\n- [ ] all of:\n  - [ ] `label=test2`\n  - [ ] `label=test3`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n"
            in summary["output"]["summary"]
        )

        await self.add_label(p["number"], "test2")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"\n### Rule: test (merge)\n- [ ] `label=test`\n- [ ] all of:\n  - [ ] `label=test3`\n  - [X] `label=test2`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n"
            in summary["output"]["summary"]
        )

        await self.add_label(p["number"], "test3")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"### Rule: test (merge)\n- [ ] `label=test`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n- [X] all of:\n  - [X] `label=test2`\n  - [X] `label=test3`\n"
            in summary["output"]["summary"]
        )

    async def test_pull_request_rules_order_operator_or(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "test",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=test",
                        {
                            "or": [
                                "label=test2",
                                "label=test3",
                            ],
                        },
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"### Rule: test (merge)\n- [ ] `label=test`\n- [ ] any of:\n  - [ ] `label=test2`\n  - [ ] `label=test3`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n"
            in summary["output"]["summary"]
        )

        await self.add_label(p["number"], "test2")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"### Rule: test (merge)\n- [ ] `label=test`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n- [X] any of:\n  - [X] `label=test2`\n  - [ ] `label=test3`\n"
            in summary["output"]["summary"]
        )

        await self.add_label(p["number"], "test3")
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"]["summary"] is not None
        assert (
            f"### Rule: test (merge)\n- [ ] `label=test`\n- [X] `-conflict` [📌 merge requirement]\n- [X] `-draft` [📌 merge requirement]\n- [X] `-mergify-configuration-changed` [📌 merge -> allow_merging_configuration_change setting requirement]\n- [X] `base={self.main_branch_name}`\n- [X] any of:\n  - [X] `label=test2`\n  - [X] `label=test3`\n"
            in summary["output"]["summary"]
        )


class TestQueueCISummary(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_pr_embarked_check_runs_statuses_ci_summary(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fail-ci",
                        "status-success=continuous-integration/pending-ci",
                        "status-success=continuous-integration/success-ci",
                    ],
                    "speculative_checks": 5,
                },
            ],
            "pull_request_rules": [
                {
                    "name": "merge default",
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
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])

        await self.add_label(p1["number"], "queue")
        await self.add_label(p2["number"], "queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_pull_1 = pulls[1]
        await self.create_status(
            tmp_pull_1,
            "continuous-integration/fail-ci",
            "failure",
        )
        await self.create_status(
            tmp_pull_1,
            "continuous-integration/pending-ci",
            "pending",
        )
        await self.create_status(
            tmp_pull_1,
            "continuous-integration/success-ci",
            "success",
        )

        await self.run_engine()

        p1 = await self.get_pull(p1["number"])
        check_runs = await self.get_check_runs(p1)

        assert len(check_runs) == 3
        assert check_runs[0]["name"] == "Queue: Embarked in merge queue"
        assert check_runs[0]["output"]["summary"] is not None

        regex = rf"Check-runs and statuses of the embarked pull request #{tmp_pull_1['number']}:.*The CI is failure.*The CI is pending.*The CI is success"
        assert (
            re.search(regex, check_runs[0]["output"]["summary"], flags=re.DOTALL)
            is not None
        )
        base_repo = p1["base"]["repo"]
        assert (
            check_runs[0]["details_url"]
            == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{base_repo['owner']['login']}/repo/{base_repo['name']}/queues?branch={parse.quote(p1['base']['ref'], safe='')}&pull={p1['number']}"
        )

    async def test_summary_html_escape(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no <i>manual</i> merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=<h1>foo</h1>bar",
                    ],
                    "actions": {"comment": {"message": "no way"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, pr)
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"] is not None
        assert summary["output"]["summary"] is not None
        assert f"""
### Rule: no &lt;i&gt;manual&lt;/i&gt; merge (comment)
- [ ] `label=<h1>foo</h1>bar`
- [X] `base={self.main_branch_name}`
""" in summary["output"]["summary"]

    async def test_invalid_config_html_escape(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "no <i>manual</i> merge",
                },
            ],
            "pull_request_rules": [
                {
                    "name": "no <i>manual</i> merge",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=<h1>foo</h1>bar",
                    ],
                    "actions": {"queue": {"name": "not <h1>exists</h1> !!"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        pr = await self.create_pr()
        await self.run_engine()
        ctxt = context.Context(self.repository_ctxt, pr)
        summary = await ctxt.get_engine_check_run(constants.SUMMARY_NAME)
        assert summary is not None
        assert summary["output"] is not None
        assert (
            summary["output"]["summary"]
            == "In the rule `no <i>manual</i> merge`, the action `queue` configuration is invalid:\n`not <h1>exists</h1> !!` queue not found\n\n"
        )
