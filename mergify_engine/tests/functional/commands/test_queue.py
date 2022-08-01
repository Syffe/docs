# -*- encoding: utf-8 -*-
#
# Copyright © 2022 Mergify SAS
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
import yaml

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.tests.functional import base


class TestQueueCommand(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def test_command_queue(self):
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

        tmp_pull_1 = await self.get_pull(p["number"] + 1)
        tmp_pull_2 = await self.get_pull(p["number"] + 2)

        ctxt = context.Context(self.repository_ctxt, p)
        q = await merge_train.Train.from_context(ctxt)
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    "created",
                    tmp_pull_1["number"],
                ),
                base.MergeQueueCarMatcher(
                    [p2["number"]],
                    [p1["number"]],
                    p["merge_commit_sha"],
                    "created",
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

    async def test_without_config(self):
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
        await self.assert_merge_queue_contents(
            q,
            p["merge_commit_sha"],
            [
                base.MergeQueueCarMatcher(
                    [p1["number"]],
                    [],
                    p["merge_commit_sha"],
                    "updated",
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
