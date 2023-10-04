import pytest
import respx

from mergify_engine import github_types
from mergify_engine import settings
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

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

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

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

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

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `#approved-reviews-by>=1` [🛡 GitHub branch protection]
    - [ ] `label=default`
    - [X] `#changes-requested-reviews-by=0` [🛡 GitHub branch protection]
  - [ ] all of: [:pushpin: queue conditions of queue `quwu`]
    - [ ] `#approved-reviews-by>=1` [🛡 GitHub branch protection]
    - [X] `#changes-requested-reviews-by=0` [🛡 GitHub branch protection]
    - [X] `label=uwu`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

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

    async def test_multiple_queue_commands_on_different_queues(self) -> None:
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
                    "merge_conditions": [
                        "status-success=continuous-integration/fake-ci",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        pr = await self.create_pr()
        await self.run_engine()

        await self.add_label(pr["number"], "special")
        await self.create_comment_as_admin(pr["number"], "@mergifyio queue specialq")
        await self.run_engine()
        first_comment = await self.wait_for_issue_comment(str(pr["number"]), "created")

        await self.create_comment_as_admin(pr["number"], "@mergifyio queue default")
        await self.run_engine()
        edited_comment = await self.wait_for_issue_comment(str(pr["number"]), "edited")
        assert edited_comment["comment"]["id"] == first_comment["comment"]["id"]
        assert (
            "🛑 Command `queue specialq` cancelled because of a new `queue` command with different arguments"
            in edited_comment["comment"]["body"]
        )

        second_comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert "Waiting for conditions to match" in second_comment["comment"]["body"]

        train = await self.get_train()
        assert len(train._cars) == 0
        assert len(train._waiting_pulls) == 0

    async def test_multiple_queue_commands_on_different_queues_with_restrictions(
        self,
    ) -> None:
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

        await self.create_comment_as_admin(pr["number"], "@mergifyio queue specialq")
        await self.run_engine()
        first_response = await self.wait_for_issue_comment(str(pr["number"]), "created")

        await self.create_comment(pr["number"], "@mergifyio queue", as_="fork")
        await self.run_engine()

        second_response = await self.wait_for_issue_comment(
            str(pr["number"]), "created"
        )
        assert "Command disallowed" in second_response["comment"]["body"]

        first_response_again = await self.get_comment(first_response["comment"]["id"])
        assert "Waiting for conditions to match" in first_response_again["body"]

    async def test_queue_command_unqueue_pr_after_ci_failure(self) -> None:
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

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

</details>
"""
            in comment_p1["comment"]["body"]
        )

        await self.create_status(p1, state="success")
        await self.run_engine()

        comment_p1 = await self.wait_for_issue_comment(str(p1["number"]), "edited")
        assert (
            "The pull request is the 1st in the queue to be merged"
            in comment_p1["comment"]["body"]
        )
        p1 = (await self.wait_for_pull_request("synchronize", p1["number"]))[
            "pull_request"
        ]
        train = await self.get_train()
        assert len(train._cars) == 1
        assert len(train._waiting_pulls) == 0

        await self.create_status(p1, state="failure")
        await self.run_full_engine()

        train = await self.get_train()
        assert len(train._cars) == 0
        assert len(train._waiting_pulls) == 0

        comment_p1 = await self.wait_for_issue_comment(str(p1["number"]), "edited")

        assert (
            "The pull request has been removed from the queue"
            in comment_p1["comment"]["body"]
        )
        assert (
            "The queue conditions cannot be satisfied due to failing checks."
            in comment_p1["comment"]["body"]
        )

    async def test_branch_protections_reporting_when_pr_is_not_yet_in_traincar(
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
                        "label=specialq",
                    ],
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        protection = {
            "required_status_checks": {
                "strict": False,
                "contexts": [
                    "continuous-integration/fake-ci",
                ],
            },
            "required_pull_request_reviews": None,
            "required_linear_history": False,
            "restrictions": None,
            "enforce_admins": True,
        }
        await self.branch_protection_protect(self.main_branch_name, protection)

        pr = await self.create_pr()
        await self.add_label(pr["number"], "specialq")
        await self.create_comment_as_admin(pr["number"], "@mergifyio queue")
        await self.run_engine()

        # Make sure the queue action is waiting for the branch protections
        comment = await self.wait_for_issue_comment(str(pr["number"]), "created")
        assert "Waiting for conditions to match" in comment["comment"]["body"]
        assert (
            """<details>

- [ ] any of: [:twisted_rightwards_arrows: queue conditions]
  - [ ] all of: [:pushpin: queue conditions of queue `default`]
    - [ ] `label=default`
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
  - [ ] all of: [:pushpin: queue conditions of queue `specialq`]
    - [ ] any of: [🛡 GitHub branch protection]
      - [ ] `check-neutral=continuous-integration/fake-ci`
      - [ ] `check-skipped=continuous-integration/fake-ci`
      - [ ] `check-success=continuous-integration/fake-ci`
    - [X] `label=specialq`
- [X] `-draft` [:pushpin: queue requirement]
- [X] `-mergify-configuration-changed` [:pushpin: queue -> allow_merging_configuration_change setting requirement]

</details>
"""
            in comment["comment"]["body"]
        )

        await self.create_status(pr)
        await self.run_engine()

        comment = await self.wait_for_issue_comment(str(pr["number"]), "edited")
        assert (
            """**Required conditions of queue** `specialq` **for merge:**

- [ ] any of: [🛡 GitHub branch protection]
  - [ ] `check-neutral=continuous-integration/fake-ci`
  - [ ] `check-skipped=continuous-integration/fake-ci`
  - [ ] `check-success=continuous-integration/fake-ci`
"""
            in comment["comment"]["body"]
        )

    # NOTE(charly): we don't check log errors for missing TrainCarState. The
    # test volontarily mock GitHub to return an error, so the engine retries the
    # merge with a limit of retries. In a normal situation, the merge conflict
    # would be detected when the TrainCar is created and checked, and we
    # shouldn't hit this limit of retries.
    @pytest.mark.skip("FIXME(sileht): we need to revist this behavior")
    @pytest.mark.logger_checker_ignore(
        "Merge queue check doesn't contain any TrainCarState",
        "failed to merge after 15 refresh attempts",
    )
    async def test_pull_request_is_not_mergeable(self) -> None:
        await self.setup_repo()

        p = await self.create_pr()
        await self.create_comment(p["number"], "@mergifyio queue", as_="admin")

        # First queue attempt fails
        with respx.mock(assert_all_called=False) as respx_mock:
            respx_mock.put(
                f"{settings.GITHUB_REST_API_URL}/repos/{self.RECORD_CONFIG['organization_name']}/{self.RECORD_CONFIG['repository_name']}/pulls/{p['number']}/merge"
            ).respond(405, json={"message": "Pull Request is not mergeable"})
            respx_mock.route(host="api.github.com").pass_through()

            await self.run_engine()

        queue_comment = await self.wait_for_issue_comment(
            action="created", test_id=str(p["number"])
        )
        assert (
            "🟠 The pull request is the 1st in the queue to be merged"
            in queue_comment["comment"]["body"]
        )

        await self.wait_for_issue_comment(action="edited", test_id=str(p["number"]))

        queue_comment = await self.wait_for_issue_comment(
            action="edited", test_id=str(p["number"])
        )
        assert (
            "GitHub can't merge the pull request after 15 retries."
            in queue_comment["comment"]["body"]
        )

        # Second queue attempt succeeds
        await self.create_comment(p["number"], "@mergifyio requeue", as_="admin")
        await self.run_engine()
        queue_comment = await self.wait_for_issue_comment(
            action="created", test_id=str(p["number"])
        )
        assert (
            "This pull request will be re-embarked automatically"
            in queue_comment["comment"]["body"]
        )

        queue_comment = await self.wait_for_issue_comment(
            action="created", test_id=str(p["number"])
        )
        assert (
            "🟠 The pull request is the 1st in the queue to be merged"
            in queue_comment["comment"]["body"]
        )
        await self.wait_for_pull_request("closed", p["number"], merged=True)
