from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.queue import merge_train
from mergify_engine.tests.functional import base


class TestQueueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_command_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "speculative_checks": 5,
                }
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.create_comment_as_admin(p2["number"], "@mergifyio queue default")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})
        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        tmp_pull_1 = await self.get_pull(
            github_types.GitHubPullRequestNumber(p["number"] + 1)
        )
        tmp_pull_2 = await self.get_pull(
            github_types.GitHubPullRequestNumber(p["number"] + 2)
        )

        ctxt = context.Context(self.repository_ctxt, p)
        q = await merge_train.Train.from_context(ctxt)
        assert p["merge_commit_sha"]
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull_2["number"],
                ),
            ],
        )

        async def assert_queued(
            pull: github_types.GitHubPullRequest, position: str
        ) -> None:
            comments = await self.get_issue_comments(pull["number"])
            assert (
                f"The pull request is the {position} in the queue to be merged"
                in comments[-1]["body"]
            )

        await self.run_engine()
        await assert_queued(p1, "1st")
        await assert_queued(p2, "2nd")

        assert tmp_pull_1["commits"] == 2
        assert tmp_pull_1["changed_files"] == 1
        assert tmp_pull_2["commits"] == 5
        assert tmp_pull_2["changed_files"] == 2

        await self.create_status(tmp_pull_1)
        await self.run_engine()
        await assert_queued(p2, "1st")

        await self.create_status(tmp_pull_2)
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_without_config(self) -> None:
        await self.setup_repo()

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_linear_history": False,
            "required_pull_request_reviews": None,
            "restrictions": None,
            "enforce_admins": False,
        }

        p1 = await self.create_pr()
        p2 = await self.create_pr(two_commits=True)

        # To force others to be rebased
        p = await self.create_pr()
        await self.merge_pull(p["number"])
        await self.wait_for("pull_request", {"action": "closed"})
        await self.run_engine()
        p = await self.get_pull(p["number"])

        await self.branch_protection_protect(self.main_branch_name, protection)

        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.create_comment_as_admin(p2["number"], "@mergifyio queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        p1 = await self.get_pull(p1["number"])

        ctxt = context.Context(self.repository_ctxt, p)
        q = await merge_train.Train.from_context(ctxt)
        assert p["merge_commit_sha"]
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    merge_train.TrainCarChecksType.INPLACE,
                    p1["number"],
                ),
            ],
            [p2["number"]],
        )

        async def assert_queued(
            pull: github_types.GitHubPullRequest, position: str
        ) -> None:
            comments = await self.get_issue_comments(pull["number"])
            assert (
                f"The pull request is the {position} in the queue to be merged"
                in comments[-1]["body"]
            )

        await self.run_engine()
        await assert_queued(p1, "1st")
        await assert_queued(p2, "2nd")

        await self.create_status(p1)
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "synchronize"})
        p2 = await self.get_pull(p2["number"])
        await assert_queued(p2, "1st")

        await self.create_status(p2)
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 0

        await self.assert_merge_queue_contents(q, None, [])

    async def test_unqueue_on_synchronize(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                    "allow_inplace_checks": False,
                    "speculative_checks": 5,
                }
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p = await self.create_pr()

        await self.create_comment_as_admin(p["number"], "@mergifyio queue")
        await self.run_engine()

        await self.wait_for("pull_request", {"action": "opened"})

        pulls = await self.get_pulls()
        assert len(pulls) == 2

        tmp_pull = await self.get_pull(
            github_types.GitHubPullRequestNumber(p["number"] + 1)
        )

        ctxt = context.Context(self.repository_ctxt, p)
        q = await merge_train.Train.from_context(ctxt)
        await self.assert_merge_queue_contents(
            q,
            p["base"]["sha"],
            [
                base.MergeQueueCarMatcher(
                    [p["number"]],
                    [],
                    p["base"]["sha"],
                    merge_train.TrainCarChecksType.DRAFT,
                    tmp_pull["number"],
                ),
            ],
        )

        async def assert_queued(
            pull: github_types.GitHubPullRequest, position: str
        ) -> None:
            comments = await self.get_issue_comments(pull["number"])
            assert (
                f"The pull request is the {position} in the queue to be merged"
                in comments[-1]["body"]
            )

        await self.run_engine()
        await assert_queued(p, "1st")

        with open(self.git.repository + "/random", "w") as f:
            f.write("yo")
        await self.git("add", "random")
        await self.git("commit", "--no-edit", "-m", "random update")

        pr_branch = self.get_full_branch_name(f"integration/pr{self.pr_counter}")
        await self.git("push", "--quiet", "origin", pr_branch)
        await self.wait_for("pull_request", {"action": "synchronize"})
        await self.run_engine()

        pulls = await self.get_pulls()
        assert len(pulls) == 1

        await self.assert_merge_queue_contents(q, None, [])
