import pytest

from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.queue import merge_train
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import conftest


class TestQueueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_command_queue_with_queue_conditions_not_matching_and_no_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "queue_conditions": [
                        "label=toto",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()
        comments = await self.get_issue_comments(p1["number"])
        assert (
            "> queue\n\n#### 🟠 Waiting for conditions to match" in comments[-1]["body"]
        )

    async def test_command_queue_with_queue_conditions_not_matching_and_fallback(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()
        comments = await self.get_issue_comments(p1["number"])
        assert (
            "The pull request is the 1st in the queue to be merged"
            in comments[-1]["body"]
        )

    async def test_command_queue_with_queue_conditions_matching(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "hotfix")
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()

        async def assert_queued(
            pull: github_types.GitHubPullRequest, position: str
        ) -> None:
            comments = await self.get_issue_comments(pull["number"])
            assert (
                f"The pull request is the {position} in the queue to be merged"
                in comments[-1]["body"]
            )
            assert (
                "**Required conditions of queue** `hotfix` **for merge:**"
                in comments[-1]["body"]
            )

        await assert_queued(p1, "1st")

    async def test_command_queue_with_queue_conditions_matching_and_default_queue_forced(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "hotfix",
                    "queue_conditions": [
                        "label=hotfix",
                    ],
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))

        p1 = await self.create_pr()
        await self.add_label(p1["number"], "hotfix")
        await self.create_comment_as_admin(p1["number"], "@mergifyio queue default")
        await self.run_engine()

        async def assert_queued(
            pull: github_types.GitHubPullRequest, position: str
        ) -> None:
            comments = await self.get_issue_comments(pull["number"])
            assert (
                f"The pull request is the {position} in the queue to be merged"
                in comments[-1]["body"]
            )
            assert (
                "**Required conditions of queue** `default` **for merge:**"
                in comments[-1]["body"]
            )

        await assert_queued(p1, "1st")

    async def test_command_queue(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
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

        tmp_pull_1 = await self.wait_for_pull_request("opened")
        tmp_pull_2 = await self.wait_for_pull_request("opened")

        pulls = await self.get_pulls()
        assert len(pulls) == 4

        q = await self.get_train()
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

        assert tmp_pull_1["pull_request"]["commits"] == 2
        assert tmp_pull_1["pull_request"]["changed_files"] == 1
        assert tmp_pull_2["pull_request"]["commits"] == 5
        assert tmp_pull_2["pull_request"]["changed_files"] == 2

        await self.create_status(tmp_pull_1["pull_request"])
        await self.run_engine()
        await assert_queued(p2, "1st")

        await self.create_status(tmp_pull_2["pull_request"])
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
        await self.wait_for_pull_request("closed", p["number"], merged=True)
        await self.run_engine()

        await self.branch_protection_protect(self.main_branch_name, protection)

        await self.create_comment_as_admin(p1["number"], "@mergifyio queue")
        await self.run_engine()
        comment_p1 = await self.wait_for_issue_comment(str(p1["number"]), "created")

        assert (
            """Waiting for conditions to match

<details>

- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] any of: [:twisted_rightwards_arrows: queue conditions]
  - [X] all of [:pushpin: queue conditions of queue `default`]

</details>
"""
            in comment_p1["comment"]["body"]
        )

        await self.create_comment_as_admin(p2["number"], "@mergifyio queue")
        await self.run_engine()
        comment_p2 = await self.wait_for_issue_comment(str(p2["number"]), "created")

        assert (
            """Waiting for conditions to match

<details>

- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] any of: [:twisted_rightwards_arrows: queue conditions]
  - [X] all of [:pushpin: queue conditions of queue `default`]

</details>
"""
            in comment_p2["comment"]["body"]
        )

        await self.create_status(p1)
        await self.run_engine()

        # NOTE: p1 and p2 both lose their status after the run_engine because they are synchronized,
        # so we need to re-create the status on the updated PR
        p1 = (await self.wait_for_pull_request("synchronize", p1["number"]))[
            "pull_request"
        ]
        await self.create_status(p1)
        await self.run_engine()

        await self.wait_for_pull_request("closed", p1["number"], merged=True)

        await self.create_status(p2)
        await self.run_engine()

        p2 = (await self.wait_for_pull_request("synchronize", p2["number"]))[
            "pull_request"
        ]
        await self.create_status(p2)
        await self.run_engine()

        await self.wait_for_pull_request("closed", p2["number"], merged=True)

    async def test_unqueue_on_synchronize(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "merge_conditions": [
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

        tmp_pull = await self.wait_for_pull_request("opened")

        pulls = await self.get_pulls()
        assert len(pulls) == 2

        q = await self.get_train()
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

    async def test_queue_conditions_with_branch_protections(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "label=default",
                    ],
                },
                {
                    "name": "quwu",
                    "queue_conditions": [
                        "label=uwu",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": None,
            "required_pull_request_reviews": {
                "required_approving_review_count": 1,
            },
            "required_linear_history": False,
            "restrictions": None,
            "enforce_admins": True,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.add_label(pr["number"], "uwu")
        await self.create_comment_as_admin(pr["number"], "@mergifyio queue")
        await self.run_engine()

        # Make sure the queue action is waiting for the branch protections
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert "Waiting for conditions to match" in comment["comment"]["body"]
        assert (
            """<details>

- [ ] `#approved-reviews-by>=1` [🛡 GitHub branch protection]
- [X] `#changes-requested-reviews-by=0` [🛡 GitHub branch protection]
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]
- [X] any of: [:twisted_rightwards_arrows: queue conditions]
  - [X] all of: [:pushpin: queue conditions of queue `quwu`]
    - [X] `label=uwu`
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `label=default`

</details>
"""
            in comment["comment"]["body"]
        )

    async def test_command_queue_non_matching_queue_conditions_and_specifying_queue_name(
        self,
    ) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        "label=default",
                    ],
                },
                {
                    "name": "specialq",
                    "queue_conditions": [
                        "label=special",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()
        await self.create_comment_as_admin(pr["number"], "@mergifyio queue default")
        await self.add_label(pr["number"], "special")
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")

        assert (
            """#### 🟠 Waiting for conditions to match

<details>

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `label=default`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

</details>"""
            in comment["comment"]["body"]
        )
        train = await self.get_train()
        assert len(train._cars) == 0
        assert len(train._waiting_pulls) == 0

        await self.create_comment_as_admin(pr["number"], "@mergifyio queue")
        await self.run_engine()

        await self.wait_for_pull_request("closed", pr["number"], merged=True)

    async def test_command_queue_infinite_loop_bug(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "routing_conditions": [
                        "label=default",
                    ],
                },
                {
                    "name": "specialq",
                    "routing_conditions": [
                        "label=special",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()
        await self.run_engine()

        await self.create_comment_as_admin(pr["number"], "@mergifyio queue default")
        await self.run_engine()
        await self.wait_for_issue_comment(str(pr["number"]), "created")

        await self.add_label(pr["number"], "special")
        await self.run_engine()

        # The label "special" was added, but we asked to queue in "default", so the PR should
        # not be queued in "specialq"
        with pytest.raises(
            (
                base.MissingEventTimeout,
                conftest.ShutUpVcrCannotOverwriteExistingCassetteException,
            )
        ):
            await self.wait_for_issue_comment(str(pr["number"]), "created")
