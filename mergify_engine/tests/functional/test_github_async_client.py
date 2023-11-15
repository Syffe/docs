from mergify_engine import github_types
from mergify_engine import yaml
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.tests.functional import base


class TestGitHubClient(base.FunctionalTestBase):
    async def test_github_async_client(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "simulator",
                    "conditions": [f"base!={self.main_branch_name}"],
                    "actions": {"merge": {}},
                },
            ],
        }
        other_branch = self.get_full_branch_name("other")
        await self.setup_repo(yaml.dump(rules), test_branches=[other_branch])
        p1 = await self.create_pr()
        p2 = await self.create_pr()
        await self.create_pr(base=other_branch)

        installation_json = await github.get_installation_from_login(
            github_types.GitHubLogin("mergifyio-testing"),
        )
        client = github.aget_client(installation_json)

        url = f"/repos/mergifyio-testing/{self.RECORD_CONFIG['repository_name']}/pulls"

        pulls = [
            p
            async for p in client.items(
                url,
                resource_name="pull",
                page_limit=5,
                params={"base": self.main_branch_name},
            )
        ]
        self.assertEqual(2, len(pulls))

        pulls = [
            p
            async for p in client.items(
                url,
                params={"per_page": "1", "base": self.main_branch_name},
                resource_name="pull",
                page_limit=5,
            )
        ]
        self.assertEqual(2, len(pulls))

        pulls = [
            p
            async for p in client.items(
                url,
                params={"per_page": "1", "page": "2", "base": self.main_branch_name},
                resource_name="pull",
                page_limit=5,
            )
        ]
        self.assertEqual(1, len(pulls))

        pulls = [
            p
            async for p in client.items(
                url,
                params={"base": other_branch, "state": "all"},
                resource_name="pull",
                page_limit=5,
            )
        ]
        self.assertEqual(1, len(pulls))

        pulls = [
            p
            async for p in client.items(
                url,
                params={"base": "unknown"},
                resource_name="pull",
                page_limit=5,
            )
        ]
        self.assertEqual(0, len(pulls))

        pull = await client.item(f"{url}/{p1['number']}")
        self.assertEqual(p1["number"], pull["number"])

        pull = await client.item(f"{url}/{p2['number']}")
        self.assertEqual(p2["number"], pull["number"])

        with self.assertRaises(http.HTTPStatusError) as ctxt:
            await client.item(f"{url}/10000000000")

        self.assertEqual(404, ctxt.exception.response.status_code)

    async def test_github_async_client_with_owner_id(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "fake PR",
                    "conditions": ["base=main"],
                    "actions": {"merge": {}},
                },
            ],
        }

        await self.setup_repo(yaml.dump(rules))
        await self.create_pr()
        url = f"/repos/mergifyio-testing/{self.RECORD_CONFIG['repository_name']}/pulls"
        pulls = [
            p
            async for p in self.client_integration.items(
                url,
                resource_name="pulls",
                page_limit=5,
                params={"base": self.main_branch_name},
            )
        ]
        self.assertEqual(1, len(pulls))
