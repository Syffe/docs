import typing

import pytest

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class BackportActionTestBase(base.FunctionalTestBase):
    async def _do_test_backport(
        self,
        method: str,
        config: None | dict[str, typing.Any] = None,
        expected_title: None | str = None,
        expected_body: None | str = None,
        expected_author: None | str = None,
    ) -> github_types.GitHubPullRequest:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"merge": {"method": method}},
                },
                {
                    "name": "Backport to stable/#3.1",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"backport": config or {"branches": [stable_branch]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), test_branches=[stable_branch])

        p = await self.create_pr(two_commits=True)

        # Create another PR to be sure we don't mess things up
        # see https://github.com/Mergifyio/mergify-engine/issues/849
        await self.create_pr(base=stable_branch)

        await self.add_label(p["number"], "backport-#3.1")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        assert await self.is_pull_merged(p["number"])

        await self.run_engine()
        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls(params={"state": "all"})
        assert len(pulls) == 1
        assert pulls[0]["state"] == "closed"

        pulls = await self.get_pulls(params={"state": "all", "base": stable_branch})
        assert len(pulls) == 2
        assert not await self.is_pull_merged(pulls[0]["number"])
        assert not await self.is_pull_merged(pulls[1]["number"])

        bp_pull = pulls[0]
        if expected_title is None:
            assert bp_pull["title"].endswith(
                f": pull request n1 from integration (backport #{p['number']})",
            )
        else:
            assert bp_pull["title"] == expected_title

        if expected_body is not None:
            assert bp_pull["body"]
            assert bp_pull["body"].startswith(expected_body)

        if expected_author is not None:
            assert bp_pull["user"]["login"] == expected_author

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = [
            c
            for c in await ctxt.pull_engine_check_runs
            if c["name"] == "Rule: Backport to stable/#3.1 (backport)"
        ]
        assert checks[0]["conclusion"] == "success"
        assert checks[0]["output"]["title"] == "Backports have been created"
        assert (
            f"* [#%d %s](%s) has been created for branch `{stable_branch}`"
            % (
                bp_pull["number"],
                bp_pull["title"],
                bp_pull["html_url"],
            )
            == checks[0]["output"]["summary"]
        )

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(
                self.url_origin,
                [
                    f"mergify/{self.mocked_backport_branch_prefix}/{stable_branch}/pr-{p['number']}",
                ],
            )
        ]
        assert [
            f"refs/heads/mergify/{self.mocked_backport_branch_prefix}/{stable_branch}/pr-{p['number']}",
        ] == refs
        return await self.get_pull(pulls[0]["number"])


@pytest.mark.subscription(subscription.Features.WORKFLOW_AUTOMATION)
class TestBackportAction(BackportActionTestBase):
    async def test_backport_no_branch(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"merge": {"method": "merge"}},
                },
                {
                    "name": "Backport",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"backport": {"branches": ["crashme"]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), test_branches=[])

        p = await self.create_pr(two_commits=True)

        await self.add_label(p["number"], "backport-#3.1")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        checks = [
            c
            for c in await ctxt.pull_engine_check_runs
            if c["name"] == "Rule: Backport (backport)"
        ]
        assert checks[0]["conclusion"] == "failure"
        assert checks[0]["output"]["title"] == "No backport have been created"
        assert (
            checks[0]["output"]["summary"]
            == "* Backport to branch `crashme` failed\n\n"
            "GitHub error: ```Branch not found```"
        )

    async def _do_backport_conflicts(
        self,
        ignore_conflicts: bool,
        labels: None | list[str] = None,
    ) -> tuple[github_types.GitHubPullRequest, list[github_types.CachedGitHubCheckRun]]:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"merge": {"method": "rebase"}},
                },
                {
                    "name": "Backport to stable/#3.1",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {
                        "backport": {
                            "branches": [stable_branch],
                            "ignore_conflicts": ignore_conflicts,
                        },
                    },
                },
            ],
        }
        if labels is not None:
            rules["pull_request_rules"][1]["actions"]["backport"]["labels"] = labels  # type: ignore[index]

        await self.setup_repo(yaml.dump(rules), test_branches=[stable_branch])

        # Commit something in stable
        await self.git("checkout", "--quiet", stable_branch)
        # Write in the file that create_pr will create in main
        with (self.git.repository / "conflicts").open("wb") as f:
            f.write(b"conflicts incoming")
        await self.git("add", "conflicts")
        await self.git("commit", "--no-edit", "-m", "add conflict")
        await self.git("push", "--quiet", "origin", stable_branch)

        p = await self.create_pr(files={"conflicts": "ohoh"})

        await self.add_label(p["number"], "backport-#3.1")
        await self.run_engine()
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()

        ctxt = context.Context(self.repository_ctxt, p, [])
        return (
            p,
            [
                c
                for c in await ctxt.pull_engine_check_runs
                if c["name"] == "Rule: Backport to stable/#3.1 (backport)"
            ],
        )

    async def test_backport_conflicts(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        p, checks = await self._do_backport_conflicts(False)

        # Retrieve the new commit id that has been be cherry-picked
        await self.git("fetch", "origin")
        commit_id = (
            await self.git("show-ref", "--hash", f"origin/{self.main_branch_name}")
        ).strip()

        assert checks[0]["conclusion"] == "failure"
        assert checks[0]["output"]["title"] == "No backport have been created"
        expected_body = f"""* Backport to branch `{stable_branch}` failed due to conflicts

Cherry-pick of {commit_id} has failed:
```
On branch mergify/{self.mocked_backport_branch_prefix}/{stable_branch}/pr-{p['number']}
Your branch is up to date with 'origin/{stable_branch}'.

You are currently cherry-picking commit {commit_id[:7]}.
  (fix conflicts and run "git cherry-pick --continue")
  (use "git cherry-pick --skip" to skip this patch)
  (use "git cherry-pick --abort" to cancel the cherry-pick operation)

Unmerged paths:
  (use "git add <file>..." to mark resolution)
	both added:      conflicts

no changes added to commit (use "git add" and/or "git commit -a")
```


"""
        assert expected_body == checks[0]["output"]["summary"]

    async def test_backport_ignore_conflicts(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        p, checks = await self._do_backport_conflicts(True, ["backported"])

        pull = (await self.get_pulls(params={"base": stable_branch}))[0]

        assert checks[0]["conclusion"] == "success"
        assert checks[0]["output"]["title"] == "Backports have been created"
        assert (
            f"* [#%d %s](%s) has been created for branch `{stable_branch}` but encountered conflicts"
            % (
                pull["number"],
                pull["title"],
                pull["html_url"],
            )
            == checks[0]["output"]["summary"]
        )
        assert sorted(label["name"] for label in pull["labels"]) == [
            "backported",
            "conflicts",
        ]
        assert pull["assignees"] == []

    async def test_backport_with_labels(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        p = await self._do_test_backport(
            "merge",
            config={"branches": [stable_branch], "labels": ["backported"]},
        )
        assert [label["name"] for label in p["labels"]] == ["backported"]

    async def test_backport_merge_commit(self) -> None:
        p = await self._do_test_backport("merge")
        assert p["commits"] == 2

    async def test_backport_merge_commit_regexes(self) -> None:
        prefix = self.get_full_branch_name("stable")
        p = await self._do_test_backport(
            "merge",
            config={"regexes": [f"^{prefix}/.*$"], "assignees": ["mergify-test4"]},
        )
        assert p["commits"] == 2
        assert len(p["assignees"]) == 1
        assert p["assignees"][0]["login"] == "mergify-test4"

    async def test_backport_squash_and_merge(self) -> None:
        p = await self._do_test_backport("squash")
        assert p["commits"] == 1

    async def test_backport_rebase_and_merge(self) -> None:
        p = await self._do_test_backport("rebase")
        assert p["commits"] == 2

    async def test_backport_with_title_and_body(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self._do_test_backport(
            "merge",
            config={
                "branches": [stable_branch],
                "title": "foo: {{destination_branch}}",
                "body": "foo: {{destination_branch}}",
            },
            expected_title=f"foo: {stable_branch}",
            expected_body=f"foo: {stable_branch}",
        )

    async def test_backport_skips_commit_when_change_already_in_destination_branch(
        self,
    ) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        rules = {
            "pull_request_rules": [
                {
                    "name": "Merge on stable branch",
                    "conditions": [
                        f"base={stable_branch}",
                        "label=merge stable branch",
                    ],
                    "actions": {"merge": {"method": "merge"}},
                },
                {
                    "name": "Merge on main",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"merge": {"method": "merge"}},
                },
                {
                    "name": "Backport to stable/#3.1",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=backport-#3.1",
                    ],
                    "actions": {"backport": {"branches": [stable_branch]}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules), test_branches=[stable_branch])

        # We create a PR with 2 commits
        p1 = await self.create_pr(
            files={
                "test.txt": "test",
            },
            two_commits=True,
        )

        # We create one of the previous two commits into the stable branch
        p2 = await self.create_pr(
            base=stable_branch,
            files={
                "test.txt": "test",
            },
        )
        # We merge the change in the stable branch
        await self.add_label(p2["number"], "merge stable branch")
        await self.run_engine()
        await self.wait_for_pull_request("closed", p2["number"], merged=True)

        await self.add_label(p1["number"], "backport-#3.1")
        await self.run_engine()
        await self.wait_for_pull_request("closed", p1["number"], merged=True)

        await self.run_engine()
        bp_pull_event = await self.wait_for_pull_request("opened")
        bp_pull = await self.get_pull(bp_pull_event["pull_request"]["number"])
        assert not bp_pull["merged"]

        assert bp_pull["title"].endswith(
            f": pull request n1 from integration (backport #{p1['number']})",
        )
        commits = await self.get_commits(bp_pull["number"])
        # Asserts the commit already present, introduced by P2, in the stable branch has been skipped
        assert len(commits) == 1

        ctxt = context.Context(self.repository_ctxt, p1, [])
        checks = [
            c
            for c in await ctxt.pull_engine_check_runs
            if c["name"] == "Rule: Backport to stable/#3.1 (backport)"
        ]
        assert checks[0]["conclusion"] == "success"
        assert checks[0]["output"]["title"] == "Backports have been created"
        assert (
            f"* [#%d %s](%s) has been created for branch `{stable_branch}`"
            % (
                bp_pull["number"],
                bp_pull["title"],
                bp_pull["html_url"],
            )
            == checks[0]["output"]["summary"]
        )

        refs = [
            ref["ref"]
            async for ref in self.find_git_refs(
                self.url_origin,
                [
                    f"mergify/{self.mocked_backport_branch_prefix}/{stable_branch}/pr-{p1['number']}",
                ],
            )
        ]
        assert [
            f"refs/heads/mergify/{self.mocked_backport_branch_prefix}/{stable_branch}/pr-{p1['number']}",
        ] == refs


class TestBackportActionWithSub(BackportActionTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_backport_with_bot_account(self) -> None:
        stable_branch = self.get_full_branch_name("stable/#3.1")
        await self._do_test_backport(
            "merge",
            config={
                "branches": [stable_branch],
                "bot_account": "mergify-test4",
            },
            expected_author="mergify-test4",
        )
