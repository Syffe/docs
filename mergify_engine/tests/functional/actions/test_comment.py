from mergify_engine import config
from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestCommentActionWithSub(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_comment_with_bot_account(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "comment": {"message": "WTF?", "bot_account": "{{ body }}"}
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr(message="mergify-test4")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert comment["comment"]["body"] == "WTF?"
        assert comment["comment"]["user"]["login"] == "mergify-test4"


class TestCommentAction(base.FunctionalTestBase):
    async def test_comment(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"comment": {"message": "WTF?"}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert comment["comment"]["body"] == "WTF?"

        # Add a label to trigger mergify
        await self.add_label(p["number"], "stable")
        await self.run_engine()

        # Ensure nothing changed
        new_comments = await self.get_issue_comments(p["number"])
        assert new_comments[-1]["body"] == "WTF?"

        # Add new commit to ensure Summary get copied and comment not reposted
        open(self.git.repository + "/new_file", "wb").close()
        await self.git("add", self.git.repository + "/new_file")
        await self.git("commit", "--no-edit", "-m", "new commit")
        await self.git(
            "push",
            "--quiet",
            "origin",
            self.get_full_branch_name(f"integration/pr{self.pr_counter}"),
        )

        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        # Ensure nothing changed
        new_comments = await self.get_issue_comments(p["number"])
        assert len(new_comments) == 1
        assert new_comments[-1]["body"] == "WTF?"

    async def test_comment_template(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"comment": {"message": "Thank you {{author}}"}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(p["number"]), "created")

        assert comment["comment"]["body"] == f"Thank you {config.BOT_USER_LOGIN}"

    async def _test_comment_template_error(
        self, msg: str
    ) -> github_types.GitHubCheckRun:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {"comment": {"message": msg}},
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed", status="completed", conclusion="failure"
        )

        assert (
            "The current Mergify configuration is invalid"
            == check_run["check_run"]["output"]["title"]
        )
        return check_run["check_run"]

    async def test_comment_template_syntax_error(self) -> None:
        check = await self._test_comment_template_error(
            msg="Thank you {{",
        )
        assert (
            """Template syntax error @ pull_request_rules → item 0 → actions → comment → message → line 1
```
unexpected 'end of template'
```"""
            == check["output"]["summary"]
        )

    async def test_comment_template_attribute_error(self) -> None:
        check = await self._test_comment_template_error(
            msg="Thank you {{hello}}",
        )
        assert (
            """Template syntax error for dictionary value @ pull_request_rules → item 0 → actions → comment → message
```
Unknown pull request attribute: hello
```"""
            == check["output"]["summary"]
        )

    async def test_comment_with_bot_account(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "comment": {"message": "WTF?", "bot_account": "mergify-test4"}
                    },
                }
            ]
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        check_run = await self.wait_for_check_run(
            action="completed", status="completed", conclusion="action_required"
        )

        # Make sure no message have been posted
        comments = await self.get_issue_comments(p["number"])
        assert len(comments) == 0

        assert (
            check_run["check_run"]["output"]["title"]
            == "The current Mergify configuration is invalid"
        )
        assert check_run["check_run"]["output"]["summary"].startswith(
            "### Comments with `bot_account` set are disabled"
        )

    async def test_comment_without_default_message(self) -> None:
        rules = {
            "defaults": {
                "actions": {
                    "comment": {},
                }
            },
            "pull_request_rules": [
                {
                    "name": "comment without default message",
                    "conditions": [f"base={self.main_branch_name}"],
                    "actions": {
                        "comment": {"message": "Hello World!"},
                    },
                }
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()
        await self.run_engine()

        await self.wait_for_check_run(
            action="completed", status="completed", conclusion="success"
        )
        comment = await self.wait_for_issue_comment(str(p["number"]), "created")
        assert comment["comment"]["body"] == "Hello World!"
