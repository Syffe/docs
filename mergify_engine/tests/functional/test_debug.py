import pprint
import typing
from unittest import mock

from mergify_engine import context
from mergify_engine import debug
from mergify_engine import github_graphql_types
from mergify_engine import subscription
from mergify_engine.clients import github
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.tests.functional import base
from mergify_engine.yaml import yaml


class TestDebugger(base.FunctionalTestBase):
    async def _mock_get_all_branch_protection_rules(self) -> None:
        # Mock the get_all_branch_protection_rules to retrieve only the "main"
        # branch protections and avoid having comparison errors when running those
        # tests in parallel with other tests that also have branch protections.
        real_get_all_branch_protection_rules = (
            context.Repository.get_all_branch_protection_rules
        )

        main_branch_name = self.main_branch_name

        async def mocked_get_all(
            self: typing.Any,
            pattern_branch_filter: str | None = None,  # noqa: ARG001
        ) -> list[github_graphql_types.GraphqlBranchProtectionRule]:
            return await real_get_all_branch_protection_rules(
                self,
                pattern_branch_filter=main_branch_name,
            )

        m = mock.patch.object(
            context.Repository,
            "get_all_branch_protection_rules",
            mocked_get_all,
        )
        self.register_mock(m)

    async def create_main_branch_protection(self) -> None:
        graphql_allowed_fields = await self.repository_ctxt.get_graphql_allowed_branch_protection_rules_fields()

        base_protection = {
            "allowsDeletions": False,
            "allowsForcePushes": False,
            "dismissesStaleReviews": False,
            "isAdminEnforced": True,
            "pattern": self.main_branch_name,
            "blocksCreations": False,
            "lockBranch": False,
            "requiredApprovingReviewCount": 1,
            "requireLastPushApproval": False,
            "requiresDeployments": False,
            "requiredDeploymentEnvironments": [],
            "requiredStatusCheckContexts": [],
            "requiresApprovingReviews": True,
            "requiresCodeOwnerReviews": False,
            "requiresCommitSignatures": False,
            "requiresConversationResolution": False,
            "requiresLinearHistory": False,
            "requiresStatusChecks": True,
            "requiresStrictStatusChecks": True,
            "restrictsPushes": False,
            "restrictsReviewDismissals": False,
        }
        protection_filtered = typing.cast(
            github_graphql_types.CreateGraphqlBranchProtectionRule,
            dict(
                filter(
                    lambda key_value: key_value[0] in graphql_allowed_fields,
                    base_protection.items(),
                ),
            ),
        )

        # mypy isn't happy because we're telling him that it's a list of something while it's actually
        # a list of something else.
        # But we're transforming that something else into the something it's supposed to be right below so its no big deal.
        self.expected_branch_protection_rules: list[
            github_graphql_types.GraphqlBranchProtectionRule
        ] = [
            protection_filtered.copy(),  # type: ignore[list-item]
        ]
        self.expected_branch_protection_rules[0]["matchingRefs"] = [
            github_graphql_types.GraphqlBranchProtectionRuleMatchingRef(
                {
                    "name": self.main_branch_name,
                    "prefix": "refs/heads/",
                },
            ),
        ]
        await self.create_branch_protection_rule(protection_filtered)

    async def test_debugger(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        {
                            "or": [
                                "label=doubt",
                                "label=suspect",
                            ],
                        },
                        {
                            "and": [
                                "number>0",
                                "title~=pull request",
                            ],
                        },
                    ],
                    "actions": {"comment": {"message": "WTF?"}},
                },
            ],
        }

        # Enable one feature to see the debug output
        self.subscription.features = frozenset(
            [
                subscription.Features.PRIORITY_QUEUES,
                subscription.Features.PUBLIC_REPOSITORY,
                subscription.Features.SHOW_SPONSOR,
                subscription.Features.WORKFLOW_AUTOMATION,
            ],
        )
        self.subscription._all_features = [
            "priority_queues",
            "public_repository",
            "show_sponsor",
            "workflow_automation",
        ]

        await self.setup_repo(yaml.dump(rules))
        await self.create_main_branch_protection()

        p = await self.create_pr()
        assert p["head"]["repo"] is not None

        await self._mock_get_all_branch_protection_rules()

        await self.run_engine()

        # NOTE(sileht): Run in a thread to not mess with the main asyncio loop
        with mock.patch("sys.stdout") as stdout:
            await debug.report(p["html_url"])
            s1 = "".join(call.args[0] for call in stdout.write.mock_calls)

        with mock.patch("sys.stdout") as stdout:
            await debug.report(p["base"]["repo"]["html_url"])
            s2 = "".join(call.args[0] for call in stdout.write.mock_calls)

        with mock.patch("sys.stdout") as stdout:
            await debug.report(p["base"]["user"]["html_url"])  # type: ignore[typeddict-item]
            s3 = "".join(call.args[0] for call in stdout.write.mock_calls)

        assert s1.startswith(s2)

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary_html_url = next(
            check for check in await ctxt.pull_check_runs if check["name"] == "Summary"
        )["html_url"]
        commit = (await ctxt.commits)[0]

        assert (
            s1.strip()
            == f"""* INSTALLATION ID: {self.installation_ctxt.installation["id"]}
* Features (db):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* Features (cache):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* ENGINE-CACHE SUB DETAIL: You're not nice
* DASHBOARD SUB DETAIL: You're not nice
* WORKER: Installation not queued to process
* REPOSITORY IS PUBLIC
* DEFAULT BRANCH: {self.main_branch_name}
* BRANCH PROTECTION RULES:
{pprint.pformat(self.expected_branch_protection_rules, width=160)}
* CONFIGURATION:
Config filename: .mergify.yml
pull_request_rules:
- actions:
    comment:
      message: WTF?
  conditions:
  - base={self.main_branch_name}
  - or:
    - label=doubt
    - label=suspect
  - and:
    - number>0
    - title~=pull request
  name: comment

* TRAIN (partition:{partr_config.DEFAULT_PARTITION_NAME}): 
* PULL REQUEST:
{{'#commits': 1,
 '#commits-behind': 0,
 '#files': 1,
 'added-files': ['test1'],
 'approved-reviews-by': [],
 'assignee': [],
 'assignees': [],
 'author': '{self.RECORD_CONFIG['app_user_login']}',
 'base': '{self.main_branch_name}',
 'body': 'test_debugger: pull request n1 from integration',
 'body-raw': 'test_debugger: pull request n1 from integration',
 'branch-protection-review-decision': 'REVIEW_REQUIRED',
 'changes-requested-reviews-by': [],
 'check-failure': [],
 'check-neutral': [],
 'check-pending': [],
 'check-skipped': [],
 'check-stale': [],
 'check-success': ['Summary'],
 'check-success-or-neutral': ['Summary'],
 'check-timed-out': [],
 'closed': False,
 'co-authors': set(),
 'commented-reviews-by': [],
 'commits': [CachedGitHubBranchCommit(sha='{commit.sha}',
                                      parents={commit.parents},
                                      commit_message='test_debugger: pull request n1 from integration',
                                      commit_verification_verified=False,
                                      author='Mergify',
                                      committer='Mergify',
                                      date_author='{commit.date_author}',
                                      date_committer='{commit.date_committer}',
                                      email_author='{commit.email_author}',
                                      email_committer='{commit.email_committer}',
                                      gh_author_login='{commit.gh_author_login}')],
 'commits-behind': [],
 'commits-unverified': ['test_debugger: pull request n1 from integration'],
 'conflict': False,
 'dismissed-reviews-by': [],
 'files': ['test1'],
 'head': '{p['head']['ref']}',
 'head-repo-full-name': '{p['head']['repo']['full_name']}',
 'label': [],
 'linear-history': True,
 'locked': False,
 'merged': False,
 'merged-by': '',
 'milestone': '',
 'modified-files': [],
 'number': {p['number']},
 'queue-dequeue-reason': None,
 'queue-freeze-reason': None,
 'queue-frozen': False,
 'queue-name': None,
 'queue-position': -1,
 'removed-files': [],
 'repository-full-name': '{self.repository_ctxt.repo["full_name"]}',
 'repository-name': '{self.repository_ctxt.repo["name"]}',
 'review-requested': [],
 'review-threads-resolved': [],
 'review-threads-unresolved': [],
 'status-failure': [],
 'status-neutral': [],
 'status-success': ['Summary'],
 'title': 'test_debugger: pull request n1 from integration'}}
is_behind: False
mergeable_state: blocked
* MERGIFY LAST CHECKS:
[Summary]: success | 1 potential rule | {summary_html_url}
> <!-- eydSdWxlOiBjb21tZW50IChjb21tZW50KSc6IG5ldXRyYWx9Cg== -->
> ### Rule: comment (comment)
> - [ ] any of:
>   - [ ] `label=doubt`
>   - [ ] `label=suspect`
> - [X] `base={self.main_branch_name}`
> - [X] all of:
>   - [X] `number>0`
>   - [X] `title~=pull request`
> 
> <hr />
> :sparkling_heart:&nbsp;&nbsp;Mergify is proud to provide this service for free to open source projects.
> 
> 🚀&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
> <hr />
> 
> <details>
> <summary>Mergify commands and options</summary>
> 
> <br />
> 
> More conditions and actions can be found in the [documentation](https://docs.mergify.com/).
> 
> You can also trigger Mergify actions by commenting on this pull request:
> 
> - `@Mergifyio refresh` will re-evaluate the rules
> - `@Mergifyio rebase` will rebase this PR on its base branch
> - `@Mergifyio update` will merge the base branch into this PR
> - `@Mergifyio backport <destination>` will backport this PR on `<destination>` branch
> 
> Additionally, on Mergify [dashboard](http://localhost:3000) you can:
> 
> - look at your merge queues
> - generate the Mergify configuration with the config editor.
> 
> Finally, you can contact us on https://mergify.com
> </details>
> 
* MERGIFY LIVE MATCHES:
[Summary]: success | 1 potential rule
> ### Rule: comment (comment)
> - [ ] any of:
>   - [ ] `label=doubt`
>   - [ ] `label=suspect`
> - [X] `base={self.main_branch_name}`
> - [X] all of:
>   - [X] `number>0`
>   - [X] `title~=pull request`
> 
> <hr />
> :sparkling_heart:&nbsp;&nbsp;Mergify is proud to provide this service for free to open source projects.
> 
> 🚀&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
> <hr />"""  # noqa: W291
        )

        assert (
            s3.strip()
            == f"""* INSTALLATION ID: {self.installation_ctxt.installation["id"]}
* Features (db):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* Features (cache):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* ENGINE-CACHE SUB DETAIL: You're not nice
* DASHBOARD SUB DETAIL: You're not nice
* WORKER: Installation not queued to process"""
        )

    async def test_debugger_with_missing_branch_protection_rule_fields(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "comment",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        {
                            "or": [
                                "label=doubt",
                                "label=suspect",
                            ],
                        },
                        {
                            "and": [
                                "number>0",
                                "title~=pull request",
                            ],
                        },
                    ],
                    "actions": {"comment": {"message": "WTF?"}},
                },
            ],
        }

        # Enable one feature to see the debug output
        self.subscription.features = frozenset(
            [
                subscription.Features.PRIORITY_QUEUES,
                subscription.Features.PUBLIC_REPOSITORY,
                subscription.Features.SHOW_SPONSOR,
                subscription.Features.WORKFLOW_AUTOMATION,
            ],
        )
        self.subscription._all_features = [
            "priority_queues",
            "public_repository",
            "show_sponsor",
            "workflow_automation",
        ]

        await self.setup_repo(yaml.dump(rules))
        await self.create_main_branch_protection()

        p = await self.create_pr()
        assert p["head"]["repo"] is not None

        await self._mock_get_all_branch_protection_rules()

        await self.run_engine()

        real_graphql_post = github.AsyncGitHubClient.graphql_post

        async def graphql_post_mock(  # type: ignore[no-untyped-def]
            self,
            query: str,
            **kwargs: typing.Any,
        ) -> typing.Any:
            # NOTE: Return an empty list of fields so that the call made to
            # `Repository.get_graphql_allowed_branch_protection_rules_fields`
            # from `Repository.get_all_branch_protection_rules` does not include
            # any of the fields that are missing in older GHES versions
            if ' __type(name: "BranchProtectionRule")' in query:
                return {"data": {"__type": {"fields": []}}}

            return await real_graphql_post(self, query, **kwargs)

        # NOTE(sileht): Run in a thread to not mess with the main asyncio loop
        with (
            mock.patch("sys.stdout") as stdout,
            mock.patch.object(
                github.AsyncGitHubClient,
                "graphql_post",
                graphql_post_mock,
            ),
        ):
            await debug.report(p["html_url"])
            s1 = "".join(call.args[0] for call in stdout.write.mock_calls)

        ctxt = context.Context(self.repository_ctxt, p, [])
        summary_html_url = next(
            check for check in await ctxt.pull_check_runs if check["name"] == "Summary"
        )["html_url"]
        commit = (await ctxt.commits)[0]

        expected_branch_protection_rules = self.expected_branch_protection_rules.copy()
        for k in (
            "blocksCreations",
            "lockBranch",
            "requireLastPushApproval",
            "requiredDeploymentEnvironments",
            "requiresDeployments",
        ):
            # All those keys should not be present in the output of branch protection
            # rule since we simulated a case were they are all missing because
            # of an old GHES version.
            # We use `pop` instead of `del` because when re-recording for GHES
            # the keys are already missing.
            expected_branch_protection_rules[0].pop(k, None)  # type: ignore[misc]

        assert (
            s1.strip()
            == f"""* INSTALLATION ID: {self.installation_ctxt.installation["id"]}
* Features (db):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* Features (cache):
  - priority_queues
  - public_repository
  - show_sponsor
  - workflow_automation
* ENGINE-CACHE SUB DETAIL: You're not nice
* DASHBOARD SUB DETAIL: You're not nice
* WORKER: Installation not queued to process
* REPOSITORY IS PUBLIC
* DEFAULT BRANCH: {self.main_branch_name}
* BRANCH PROTECTION RULES:
{pprint.pformat(expected_branch_protection_rules, width=160)}
* CONFIGURATION:
Config filename: .mergify.yml
pull_request_rules:
- actions:
    comment:
      message: WTF?
  conditions:
  - base={self.main_branch_name}
  - or:
    - label=doubt
    - label=suspect
  - and:
    - number>0
    - title~=pull request
  name: comment

* TRAIN (partition:{partr_config.DEFAULT_PARTITION_NAME}): 
* PULL REQUEST:
{{'#commits': 1,
 '#commits-behind': 0,
 '#files': 1,
 'added-files': ['test1'],
 'approved-reviews-by': [],
 'assignee': [],
 'assignees': [],
 'author': '{self.RECORD_CONFIG['app_user_login']}',
 'base': '{self.main_branch_name}',
 'body': 'test_debugger_with_missing_branch_protection_rule_fields: pull request n1 from integration',
 'body-raw': 'test_debugger_with_missing_branch_protection_rule_fields: pull request n1 from integration',
 'branch-protection-review-decision': 'REVIEW_REQUIRED',
 'changes-requested-reviews-by': [],
 'check-failure': [],
 'check-neutral': [],
 'check-pending': [],
 'check-skipped': [],
 'check-stale': [],
 'check-success': ['Summary'],
 'check-success-or-neutral': ['Summary'],
 'check-timed-out': [],
 'closed': False,
 'co-authors': set(),
 'commented-reviews-by': [],
 'commits': [CachedGitHubBranchCommit(sha='{commit.sha}',
                                      parents={commit.parents},
                                      commit_message='test_debugger_with_missing_branch_protection_rule_fields: pull request n1 from integration',
                                      commit_verification_verified=False,
                                      author='Mergify',
                                      committer='Mergify',
                                      date_author='{commit.date_author}',
                                      date_committer='{commit.date_committer}',
                                      email_author='{commit.email_author}',
                                      email_committer='{commit.email_committer}',
                                      gh_author_login='{commit.gh_author_login}')],
 'commits-behind': [],
 'commits-unverified': ['test_debugger_with_missing_branch_protection_rule_fields: pull request n1 from integration'],
 'conflict': False,
 'dismissed-reviews-by': [],
 'files': ['test1'],
 'head': '{p['head']['ref']}',
 'head-repo-full-name': '{p['head']['repo']['full_name']}',
 'label': [],
 'linear-history': True,
 'locked': False,
 'merged': False,
 'merged-by': '',
 'milestone': '',
 'modified-files': [],
 'number': {p['number']},
 'queue-dequeue-reason': None,
 'queue-freeze-reason': None,
 'queue-frozen': False,
 'queue-name': None,
 'queue-position': -1,
 'removed-files': [],
 'repository-full-name': '{self.repository_ctxt.repo["full_name"]}',
 'repository-name': '{self.repository_ctxt.repo["name"]}',
 'review-requested': [],
 'review-threads-resolved': [],
 'review-threads-unresolved': [],
 'status-failure': [],
 'status-neutral': [],
 'status-success': ['Summary'],
 'title': 'test_debugger_with_missing_branch_protection_rule_fields: pull request n1 from integration'}}
is_behind: False
mergeable_state: blocked
* MERGIFY LAST CHECKS:
[Summary]: success | 1 potential rule | {summary_html_url}
> <!-- eydSdWxlOiBjb21tZW50IChjb21tZW50KSc6IG5ldXRyYWx9Cg== -->
> ### Rule: comment (comment)
> - [ ] any of:
>   - [ ] `label=doubt`
>   - [ ] `label=suspect`
> - [X] `base={self.main_branch_name}`
> - [X] all of:
>   - [X] `number>0`
>   - [X] `title~=pull request`
> 
> <hr />
> :sparkling_heart:&nbsp;&nbsp;Mergify is proud to provide this service for free to open source projects.
> 
> 🚀&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
> <hr />
> 
> <details>
> <summary>Mergify commands and options</summary>
> 
> <br />
> 
> More conditions and actions can be found in the [documentation](https://docs.mergify.com/).
> 
> You can also trigger Mergify actions by commenting on this pull request:
> 
> - `@Mergifyio refresh` will re-evaluate the rules
> - `@Mergifyio rebase` will rebase this PR on its base branch
> - `@Mergifyio update` will merge the base branch into this PR
> - `@Mergifyio backport <destination>` will backport this PR on `<destination>` branch
> 
> Additionally, on Mergify [dashboard](http://localhost:3000) you can:
> 
> - look at your merge queues
> - generate the Mergify configuration with the config editor.
> 
> Finally, you can contact us on https://mergify.com
> </details>
> 
* MERGIFY LIVE MATCHES:
[Summary]: success | 1 potential rule
> ### Rule: comment (comment)
> - [ ] any of:
>   - [ ] `label=doubt`
>   - [ ] `label=suspect`
> - [X] `base={self.main_branch_name}`
> - [X] all of:
>   - [X] `number>0`
>   - [X] `title~=pull request`
> 
> <hr />
> :sparkling_heart:&nbsp;&nbsp;Mergify is proud to provide this service for free to open source projects.
> 
> 🚀&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
> <hr />"""  # noqa: W291
        )
