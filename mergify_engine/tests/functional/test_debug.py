from unittest import mock

from mergify_engine import context
from mergify_engine import debug
from mergify_engine import yaml
from mergify_engine.dashboard import subscription
from mergify_engine.tests.functional import base


class TestDebugger(base.FunctionalTestBase):
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
                            ]
                        },
                        {
                            "and": [
                                "number>0",
                                "title~=pull request",
                            ]
                        },
                    ],
                    "actions": {"comment": {"message": "WTF?"}},
                }
            ]
        }

        # Enable one feature to see the debug output
        self.subscription.features = frozenset(
            [
                subscription.Features.PRIORITY_QUEUES,
                subscription.Features.PUBLIC_REPOSITORY,
                subscription.Features.SHOW_SPONSOR,
            ]
        )

        await self.setup_repo(yaml.dump(rules))
        p = await self.create_pr()

        await self.run_engine()

        # NOTE(sileht): Run is a thread to not mess with the main asyncio loop
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
        summary_html_url = [
            check for check in await ctxt.pull_check_runs if check["name"] == "Summary"
        ][0]["html_url"]
        commit = (await ctxt.commits)[0]
        assert (
            s1.strip()
            == f"""* INSTALLATION ID: {self.installation_ctxt.installation["id"]}
* Features (db):
  - priority_queues
  - public_repository
  - show_sponsor
* Features (cache):
  - priority_queues
  - public_repository
  - show_sponsor
* ENGINE-CACHE SUB DETAIL: You're not nice
* DASHBOARD SUB DETAIL: You're not nice
* WORKER: Installation not queued to process
* REPOSITORY IS PUBLIC
* DEFAULT BRANCH: {self.main_branch_name}
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

* TRAIN: 
* PULL REQUEST:
{{'#commits': 1,
 '#commits-behind': 0,
 '#files': 1,
 'approved-reviews-by': [],
 'assignee': [],
 'author': '{self.RECORD_CONFIG['app_user_login']}',
 'base': '{self.main_branch_name}',
 'body': 'test_debugger: pull request n1 from integration',
 'body-raw': 'test_debugger: pull request n1 from integration',
 'branch-protection-review-decision': None,
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
                                      email_committer='{commit.email_committer}')],
 'commits-unverified': ['test_debugger: pull request n1 from integration'],
 'conflict': False,
 'dismissed-reviews-by': [],
 'files': ['test1'],
 'head': '{p['head']['ref']}',
 'label': [],
 'linear-history': True,
 'locked': False,
 'merged': False,
 'merged-by': '',
 'milestone': '',
 'number': {p['number']},
 'queue-position': -1,
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
mergeable_state: clean
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
> :rocket:&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
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
> Additionally, on Mergify [dashboard](https://dashboard.mergify.com/) you can:
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
> :rocket:&nbsp;&nbsp;You can help us by [becoming a sponsor](/sponsors/Mergifyio)!
> <hr />"""  # noqa:W291
        )

        assert (
            s3.strip()
            == f"""* INSTALLATION ID: {self.installation_ctxt.installation["id"]}
* Features (db):
  - priority_queues
  - public_repository
  - show_sponsor
* Features (cache):
  - priority_queues
  - public_repository
  - show_sponsor
* ENGINE-CACHE SUB DETAIL: You're not nice
* DASHBOARD SUB DETAIL: You're not nice
* WORKER: Installation not queued to process"""
        )
