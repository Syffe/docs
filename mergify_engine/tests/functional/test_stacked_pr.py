import pytest
import yaml

from mergify_engine import subscription
from mergify_engine.tests.functional import base
from mergify_engine.tests.functional import utils as tests_utils


@pytest.mark.xdist_group(group="delete_branch_on_merge")
@pytest.mark.subscription(
    subscription.Features.WORKFLOW_AUTOMATION,
    subscription.Features.MERGE_QUEUE,
)
@pytest.mark.delete_branch_on_merge(True)  # noqa: FBT003
class TestStackedPr(base.FunctionalTestBase):
    async def test_merging_a_stack(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "default",
                    "queue_conditions": [
                        f"base={self.main_branch_name}",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "default merge",
                    "conditions": [
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))

        # Set branches graph
        # main ◄── A  ◄── B ◄── C ◄── D
        #                 ▲
        #                 └──── C1
        branch_a = self.get_full_branch_name("A")
        branch_b = self.get_full_branch_name("B")
        branch_c = self.get_full_branch_name("C")
        branch_c1 = self.get_full_branch_name("C1")
        branch_d = self.get_full_branch_name("D")
        branches_graph = (
            (self.main_branch_name, branch_a),
            (branch_a, branch_b),
            (branch_b, branch_c),
            (branch_b, branch_c1),
            (branch_c, branch_d),
        )

        # Create branches
        branches_to_push = []
        for base_ref, head_ref in branches_graph:
            await self.git("branch", head_ref, base_ref)
            branches_to_push.append(head_ref)
        await self.git("push", "--quiet", "origin", *branches_to_push)

        await self.wait_for_all(
            [
                {
                    "event_type": "push",
                    "payload": tests_utils.get_push_event_payload(
                        branch_name=branch_to_push,
                    ),
                }
                for branch_to_push in branches_to_push
            ],
        )

        # Create PRs
        pr_a = await self.create_pr(
            base=self.main_branch_name,
            branch=branch_a,
            git_tree_ready=True,
        )
        pr_b = await self.create_pr(base=branch_a, branch=branch_b, git_tree_ready=True)
        pr_c = await self.create_pr(base=branch_b, branch=branch_c, git_tree_ready=True)
        pr_c1 = await self.create_pr(
            base=branch_b,
            branch=branch_c1,
            git_tree_ready=True,
        )
        pr_d = await self.create_pr(base=branch_c, branch=branch_d, git_tree_ready=True)

        # Start testing

        await self.add_label(pr_a["number"], "queue")
        await self.add_label(pr_b["number"], "queue")
        await self.add_label(pr_c["number"], "queue")
        await self.add_label(pr_c1["number"], "queue")
        await self.add_label(pr_d["number"], "queue")

        await self.run_engine()

        await self.wait_for_pull_request(
            "closed",
            pr_number=pr_a["number"],
            merged=True,
        )
        pr_b = (
            await self.wait_for_pull_request(
                "edited",
                pr_number=pr_b["number"],
                merged=False,
            )
        )["pull_request"]

        assert pr_b["base"]["ref"] == self.main_branch_name

        await self.wait_for_pull_request(
            "closed",
            pr_number=pr_b["number"],
            merged=True,
        )
        pr_c = (
            await self.wait_for_pull_request(
                "edited",
                pr_number=pr_c["number"],
                merged=False,
            )
        )["pull_request"]
        pr_c1 = (
            await self.wait_for_pull_request(
                "edited",
                pr_number=pr_c1["number"],
                merged=False,
            )
        )["pull_request"]

        assert pr_c["base"]["ref"] == self.main_branch_name
        assert pr_c1["base"]["ref"] == self.main_branch_name

        await self.wait_for_pull_request(
            "closed",
            pr_number=pr_c["number"],
            merged=True,
        )

        pr_d = (
            await self.wait_for_pull_request(
                "edited",
                pr_number=pr_d["number"],
                merged=False,
            )
        )["pull_request"]

        await self.wait_for_pull_request(
            "closed",
            pr_number=pr_c1["number"],
            merged=True,
        )

        assert pr_d["base"]["ref"] == self.main_branch_name

        await self.wait_for_pull_request(
            "closed",
            pr_number=pr_d["number"],
            merged=True,
        )
