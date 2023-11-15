import pytest

from mergify_engine import subscription
from mergify_engine import yaml
from mergify_engine.tests.functional import base


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestCommandsRestrictions(base.FunctionalTestBase):
    async def test_commands_restrictions_base_branch(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        feature_branch = self.get_full_branch_name("feature/one")

        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "copy": {"conditions": [f"base={self.main_branch_name}"]},
                    },
                },
            ),
            test_branches=[stable_branch, feature_branch],
        )
        p_ok = await self.create_pr()
        p_nok = await self.create_pr(base=feature_branch)

        await self.create_command(
            p_ok["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )
        await self.create_command(
            p_nok["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        # only one copy done
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 1 == len(pulls_stable)

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(
                self.url_origin,
                [f"mergify/{self.mocked_copy_branch_prefix}"],
            )
        ]
        assert (
            f"refs/heads/mergify/{self.mocked_copy_branch_prefix}/{stable_branch}/pr-{p_ok['number']}"
            in refs
        )

        # success comment
        comments = await self.get_issue_comments(p_ok["number"])
        assert len(comments) == 2
        assert "Pull request copies have been created" in comments[1]["body"]

        # failure comment
        comments = await self.get_issue_comments(p_nok["number"])
        assert len(comments) == 2
        assert "Command disallowed due to [command restrictions]" in comments[1]["body"]

    async def test_false_user_commands_restrictions(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "copy": {
                            "conditions": ["sender=toto"],
                        },  # sender is mergify-test1 but toto is expected
                    },
                },
            ),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr()
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        # No copy done due to false sender
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 0 == len(pulls_stable)

        # failure comment
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Command disallowed due to [command restrictions]" in comments[1]["body"]

    async def test_user_in_team_commands_restrictions(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")

        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "copy": {"conditions": ["sender=@mergifyio-testing/testing"]},
                    },
                },
            ),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr()
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        # Copy done successfully because sender is in the team
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 1 == len(pulls_stable)

        # success comment
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Pull request copies have been created" in comments[1]["body"]

    async def test_user_not_in_team_commands_restrictions(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")

        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "copy": {"conditions": ["sender=@mergifyio-testing/bot"]},
                    },
                },
            ),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr()
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        # No copy done
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 0 == len(pulls_stable)

        # Failure comment
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Command disallowed due to [command restrictions]" in comments[1]["body"]

    async def test_commands_restrictions_sender_permission(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")

        await self.setup_repo(
            yaml.dump(
                {
                    "commands_restrictions": {
                        "copy": {"conditions": ["sender-permission=admin"]},
                    },
                },
            ),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr()

        # Create command as non-admin user
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="fork",
        )

        # No copy done
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 0 == len(pulls_stable)

        # Failure comment
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Command disallowed due to" in comments[-1]["body"]

        # Create command as admin user
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        # Copy done successfully because sender is in the team
        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 1 == len(pulls_stable)

        # Success comment
        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 4
        assert "Pull request copies have been created" in comments[-1]["body"]

    async def test_jinja_template_condition_fail(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        rules = {
            "commands_restrictions": {
                "copy": {
                    "conditions": ["sender={{author}}"],
                },
            },
        }
        await self.setup_repo(
            yaml.dump(rules),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr()

        # Admin creates a command, he is NOT allowed to run it, as he is not the
        # author
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 0 == len(pulls_stable)

        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Command disallowed due to [command restrictions]" in comments[1]["body"]

    async def test_jinja_template_condition_success(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        rules = {
            "commands_restrictions": {
                "copy": {
                    "conditions": ["sender={{author}}"],
                },
            },
        }
        await self.setup_repo(
            yaml.dump(rules),
            test_branches=[stable_branch],
        )
        pr = await self.create_pr(as_="admin")

        # Author of the PR creates a command, he is allowed to run it
        await self.create_command(
            pr["number"],
            f"@mergifyio copy {stable_branch}",
            as_="admin",
        )

        pulls_stable = await self.get_pulls(
            params={"state": "all", "base": stable_branch},
        )
        assert 1 == len(pulls_stable)

        comments = await self.get_issue_comments(pr["number"])
        assert len(comments) == 2
        assert "Pull request copies have been created" in comments[-1]["body"]
