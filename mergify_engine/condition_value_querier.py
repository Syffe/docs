from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import re
import typing
import warnings

import bs4
import jinja2.exceptions
import jinja2.meta
import jinja2.runtime
import jinja2.sandbox
import jinja2.utils
import markdownify

from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.clients import http


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine import context as context_mod
    from mergify_engine.rules.config import partition_rules as partr_config

MARKDOWN_TITLE_RE = re.compile(r"^#+ ", re.I)
MARKDOWN_COMMIT_MESSAGE_RE = re.compile(r"^#+ Commit Message ?:?\s*$", re.I)
MARKDOWN_COMMENT_RE = re.compile("(<!--.*?-->)", flags=re.DOTALL | re.IGNORECASE)
COMMITS_ARRAY_ATTRIBUTE_RE = re.compile(r"^commits\[(-?\d+|\*)\]\.([\-\w]+)$")


class CommitAuthor(typing.NamedTuple):
    name: str
    email: str


PullRequestAttributeType = (
    None
    | list[bool]
    | bool
    | list[str]
    | str
    | int
    | list[datetime.datetime]
    | list[date.RelativeDatetime]
    | datetime.time
    | datetime.datetime
    | datetime.timedelta
    | date.RelativeDatetime
    | list[github_types.SHAType]
    | list[github_types.GitHubLogin]
    | list[github_types.GitHubBranchCommit]
    | list[github_types.CachedGitHubBranchCommit]
    # Quotes because of circular import
    | list["partr_config.PartitionRuleName"]
    | github_types.GitHubRepositoryPermission
    | set[CommitAuthor]
)

CommitListAttributeType = (
    list[str]
    | list[bool]
    | list[date.RelativeDatetime]
    | list[datetime.datetime]
    | list[github_types.GitHubLogin]
)
CommitAttributeType = (
    str | bool | date.RelativeDatetime | datetime.datetime | github_types.GitHubLogin
)


@dataclasses.dataclass
class RenderTemplateFailure(Exception):
    message: str
    lineno: int | None = None

    def __str__(self) -> str:
        return self.message


@dataclasses.dataclass
class PullRequestAttributeError(AttributeError):
    name: str


class BasePullRequest:
    @classmethod
    async def _get_commits_attribute(
        cls,
        ctxt: context_mod.Context,
        commit_attribute: str,
        relative_time: bool,
    ) -> CommitListAttributeType:
        return_values = [
            cls._get_commit_attribute(ctxt, commit, commit_attribute, relative_time)
            for commit in await ctxt.commits
        ]

        # mypy doesn't seem to be able to understand that commit_attribute
        # always being the same for each values makes it impossible to have
        # different types in the list.
        if commit_attribute in ("date_author", "date_committer"):
            if relative_time:
                return typing.cast(list[date.RelativeDatetime], return_values)
            return typing.cast(list[datetime.datetime], return_values)
        if commit_attribute == "commit_verification_verified":
            return typing.cast(list[bool], return_values)
        if commit_attribute in (
            "email_author",
            "email_committer",
            "commit_message",
        ):
            return typing.cast(list[str], return_values)
        if commit_attribute in ("author", "committer"):
            return typing.cast(list[github_types.GitHubLogin], return_values)

        raise RuntimeError(f"Unknown commit attribute `{commit_attribute}`")

    @staticmethod
    def _get_commit_attribute(
        ctxt: context_mod.Context,
        commit: github_types.CachedGitHubBranchCommit,
        commit_attribute: str,
        relative_time: bool,
    ) -> CommitAttributeType:
        # Circular import
        from mergify_engine.rules import parser

        if (
            commit_attribute
            not in parser.PARSERS_FOR_ARRAY_SUBATTRIBUTES["commits"].keys()
        ):
            raise PullRequestAttributeError(commit_attribute)

        value = getattr(commit, commit_attribute)
        if commit_attribute not in ("date_author", "date_committer"):
            if commit_attribute == "commit_verification_verified":
                return typing.cast(bool, value)
            if commit_attribute in (
                "email_author",
                "email_committer",
                "commit_message",
            ):
                return typing.cast(str, value)
            if commit_attribute in ("author", "committer"):
                return typing.cast(github_types.GitHubLogin, value)
            raise RuntimeError(f"Unknown commit attribute `{commit_attribute}`")

        date_value = date.fromisoformat(value)
        if relative_time:
            return date.RelativeDatetime(date_value)

        return date_value

    @staticmethod
    async def _get_consolidated_queue_data(
        ctxt: context_mod.Context,
        name: str,
    ) -> PullRequestAttributeType:
        # Circular import
        from mergify_engine.actions import utils as action_utils
        from mergify_engine.queue import merge_train
        from mergify_engine.queue.merge_train import train_car_state as tcs

        mergify_config = await ctxt.repository.get_mergify_config()
        queue_rules = mergify_config["queue_rules"]
        partition_rules = mergify_config["partition_rules"]
        convoy = await merge_train.Convoy.from_context(
            ctxt,
            queue_rules,
            partition_rules,
        )

        if name == "queue-position":
            embarked_pulls = await convoy.find_embarked_pull(ctxt.pull["number"])
            if not embarked_pulls:
                return -1
            if len(embarked_pulls) == 1:
                return embarked_pulls[0].position

            return max([ep.position for ep in embarked_pulls])

        if name in ("queued-at", "queued-at-relative"):
            embarked_pulls = await convoy.find_embarked_pull(ctxt.pull["number"])
            if not embarked_pulls:
                return None

            # NOTE(Greesb): Use the embarked_pulls at index 0 because
            # the PR should be queued at the same time for every partitions.
            if name == "queued-at":
                return embarked_pulls[0].embarked_pull.queued_at
            return date.RelativeDatetime(embarked_pulls[0].embarked_pull.queued_at)

        if name in ("queue-merge-started-at", "queue-merge-started-at-relative"):
            # Only used with QueuePullRequest
            cars = convoy.get_train_cars_by_tmp_pull(ctxt)
            if not cars:
                cars = convoy.get_train_cars_by_pull(ctxt)
                if not cars:
                    return None
                if len(cars) > 1:
                    # NOTE(Greesb): Attribute not yet handled for multiple cars (monorepo case)
                    raise PullRequestAttributeError(name)
                started_at = cars[0].train_car_state.ci_started_at

            elif len(cars) > 1:
                # NOTE(Greesb): Attribute not yet handled for multiple cars (monorepo case)
                raise PullRequestAttributeError(name)
            else:
                started_at = cars[0].train_car_state.ci_started_at

            if started_at is None:
                return None

            if name == "queue-merge-started-at":
                return started_at
            return date.RelativeDatetime(started_at)

        if name == "queue-partition-name":
            return await partition_rules.get_evaluated_partition_names_from_context(
                ctxt,
            )

        # NOTE(Syffe): this attribute in only used for merge-conditions internally. Because at this point
        # we want only the partitions the PR is currently queued in. Whereas with "queue-partition-name"
        # we return all the partitions the PR could be queued in.
        if name == "queue-current-partition-name":
            return convoy.get_queue_pull_request_partition_names_from_context(ctxt)

        if name == "queue-dequeue-reason":
            try:
                return (
                    (
                        await action_utils.get_dequeue_reason_from_outcome(
                            ctxt,
                            error_if_unknown=False,
                        )
                    )
                    .dequeue_code.lower()
                    .replace("_", "-")
                )
            except (action_utils.MissingMergeQueueCheckRun, tcs.UnexpectedOutcome):
                # NOTE(lecrepont01): We don't expect the check to be present here at
                # all cost during the evaluation, and we can't ensure that the
                # state can be mapped to an abort reason either. In this case
                # the rule should just evaluate to false.
                return None

        raise PullRequestAttributeError(name)

    @staticmethod
    async def _get_consolidated_checks_data(
        ctxt: context_mod.Context,
        states: tuple[str | None, ...] | None,
    ) -> PullRequestAttributeType:
        checks = [
            check_name
            for check_name, state in (await ctxt.checks).items()
            if states is None or state in states
        ]
        # NOTE(sileht): backward compatiblity for user that have writting condition
        # on this name
        if constants.MERGE_QUEUE_SUMMARY_NAME in checks:
            checks.append(constants.MERGE_QUEUE_OLD_SUMMARY_NAME)
        return checks

    @classmethod
    async def _get_consolidated_data(
        cls,
        ctxt: context_mod.Context,
        name: str,
    ) -> PullRequestAttributeType:
        if name in ("assignee", "assignees"):
            return [a["login"] for a in ctxt.pull["assignees"]]

        if name.startswith("queue"):
            return await cls._get_consolidated_queue_data(ctxt, name)

        if name == "label":
            return [label["name"] for label in ctxt.pull["labels"]]

        if name == "review-requested":
            return (
                [typing.cast(str, u["login"]) for u in ctxt.pull["requested_reviewers"]]
                + [f"@{t['slug']}" for t in ctxt.pull["requested_teams"]]
                + [
                    f"@{ctxt.repository.installation.owner_login}/{t['slug']}"
                    for t in ctxt.pull["requested_teams"]
                ]
            )
        if name == "draft":
            return ctxt.pull["draft"]

        if name == "mergify-configuration-changed":
            # NOTE(sileht): only internally used
            return ctxt.configuration_changed

        if name == "author":
            return ctxt.pull["user"]["login"]

        if name == "merged-by":
            return (
                ctxt.pull["merged_by"]["login"]
                if ctxt.pull["merged_by"] is not None
                else ""
            )

        if name == "merged":
            return ctxt.pull["merged"]

        if name == "closed":
            return ctxt.closed

        if name == "milestone":
            return (
                ctxt.pull["milestone"]["title"]
                if ctxt.pull["milestone"] is not None
                else ""
            )

        if name == "number":
            return typing.cast(int, ctxt.pull["number"])

        if name == "#commits-behind":
            return await ctxt.commits_behind_count

        if name == "conflict":
            return await ctxt.is_conflicting()

        if name == "linear-history":
            return await ctxt.has_linear_history()

        if name == "base":
            return ctxt.pull["base"]["ref"]

        if name == "head":
            return ctxt.pull["head"]["ref"]

        if name == "head-repo-full-name":
            return (
                repo["full_name"]
                if (repo := ctxt.pull["head"]["repo"]) is not None
                else ""
            )

        if name == "locked":
            return ctxt.pull["locked"]

        if name == "title":
            return ctxt.pull["title"] or ""

        if name == "body":
            return MARKDOWN_COMMENT_RE.sub(
                "",
                ctxt.body,
            )

        if name == "body-raw":
            return ctxt.body

        if name == "#files":
            return ctxt.pull["changed_files"]

        if name == "files":
            return [f["filename"] for f in await ctxt.files]

        if name in ("added-files", "removed-files", "modified-files"):
            status = name.split("-")[0]
            return [f["filename"] for f in await ctxt.files if f["status"] == status]

        if name == "#commits":
            return ctxt.pull["commits"]

        if name == "commits":
            return await ctxt.commits

        if name.startswith("commits["):
            match = COMMITS_ARRAY_ATTRIBUTE_RE.match(name)
            if match is None:
                raise PullRequestAttributeError(name)

            commit_attribute = match.group(2)

            relative_time = False
            if commit_attribute.endswith("-relative"):
                relative_time = True
                commit_attribute = re.sub(r"-relative$", "", commit_attribute)

            commit_attribute = commit_attribute.replace("-", "_")

            nb_commit = match.group(1)
            if nb_commit == "*":
                return await cls._get_commits_attribute(
                    ctxt,
                    commit_attribute,
                    relative_time,
                )

            try:
                commit = (await ctxt.commits)[int(nb_commit)]
            except IndexError:
                return None

            return cls._get_commit_attribute(
                ctxt,
                commit,
                commit_attribute,
                relative_time,
            )

        if name == "approved-reviews-by":
            _, approvals = await ctxt.consolidated_reviews()
            return [
                r["user"]["login"]
                for r in approvals
                if r["state"] == "APPROVED" and r["user"] is not None
            ]

        if name == "dismissed-reviews-by":
            _, approvals = await ctxt.consolidated_reviews()
            return [
                r["user"]["login"]
                for r in approvals
                if r["state"] == "DISMISSED" and r["user"] is not None
            ]

        if name == "changes-requested-reviews-by":
            _, approvals = await ctxt.consolidated_reviews()
            return [
                r["user"]["login"]
                for r in approvals
                if r["state"] == "CHANGES_REQUESTED" and r["user"] is not None
            ]

        if name == "commented-reviews-by":
            comments, _ = await ctxt.consolidated_reviews()
            return [
                r["user"]["login"]
                for r in comments
                if r["state"] == "COMMENTED" and r["user"] is not None
            ]

        # NOTE(jd) The Check API set conclusion to None for pending.
        if name == "check-success-or-neutral-or-pending":
            return await cls._get_consolidated_checks_data(
                ctxt,
                ("success", "neutral", "pending", None),
            )

        if name == "check-success-or-neutral":
            return await cls._get_consolidated_checks_data(ctxt, ("success", "neutral"))

        if name in ("status-success", "check-success"):
            return await cls._get_consolidated_checks_data(ctxt, ("success",))

        if name in ("status-failure", "check-failure"):
            # hopefully "cancelled" is actually a failure state to github.
            # I think it is, however it could be the same thing as the
            # "skipped" status.
            return await cls._get_consolidated_checks_data(
                ctxt,
                ("failure", "action_required", "cancelled", "timed_out", "error"),
            )

        if name in ("status-neutral", "check-neutral"):
            return await cls._get_consolidated_checks_data(ctxt, ("neutral",))

        if name == "check-timed-out":
            return await cls._get_consolidated_checks_data(ctxt, ("timed_out",))

        if name == "check-skipped":
            # hopefully this handles the gray "skipped" state that github actions
            # workflows can send when a job that depends on a job and the job it
            # depends on fails, making it get skipped automatically then.
            return await cls._get_consolidated_checks_data(ctxt, ("skipped",))

        if name == "check":
            return await cls._get_consolidated_checks_data(ctxt, None)

        if name == "check-pending":
            return await cls._get_consolidated_checks_data(ctxt, (None, "pending"))

        if name == "check-stale":
            return await cls._get_consolidated_checks_data(ctxt, ("stale",))

        if name == "depends-on":
            depends_on = []
            for pull_request_number in ctxt.get_depends_on():
                try:
                    depends_on_ctxt = await ctxt.repository.get_pull_request_context(
                        pull_request_number,
                    )
                except http.HTTPNotFound:
                    continue

                is_pr_queued_above = await ctxt.is_pr_queued_above(pull_request_number)

                if depends_on_ctxt.pull["merged"] or is_pr_queued_above:
                    depends_on.append(f"#{pull_request_number}")
            return depends_on

        if name == "current-datetime":
            return date.utcnow()

        if name == "updated-at-relative":
            return date.RelativeDatetime(date.fromisoformat(ctxt.pull["updated_at"]))
        if name == "created-at-relative":
            return date.RelativeDatetime(date.fromisoformat(ctxt.pull["created_at"]))
        if name == "closed-at-relative":
            if ctxt.pull["closed_at"] is None:
                return None
            return date.RelativeDatetime(date.fromisoformat(ctxt.pull["closed_at"]))
        if name == "merged-at-relative":
            if ctxt.pull["merged_at"] is None:
                return None
            return date.RelativeDatetime(date.fromisoformat(ctxt.pull["merged_at"]))

        if name == "updated-at":
            return date.fromisoformat(ctxt.pull["updated_at"])

        if name == "created-at":
            return date.fromisoformat(ctxt.pull["created_at"])

        if name == "closed-at":
            if ctxt.pull["closed_at"] is None:
                return None
            return date.fromisoformat(ctxt.pull["closed_at"])

        if name == "merged-at":
            if ctxt.pull["merged_at"] is None:
                return None
            return date.fromisoformat(ctxt.pull["merged_at"])

        if name == "commits-unverified":
            return await ctxt.retrieve_unverified_commits()

        if name == "review-threads-resolved":
            return [
                t["first_comment"]
                for t in await ctxt.retrieve_review_threads()
                if t["isResolved"]
            ]

        if name == "review-threads-unresolved":
            return [
                t["first_comment"]
                for t in await ctxt.retrieve_review_threads()
                if not t["isResolved"]
            ]

        if name == "repository-name":
            return ctxt.repository.repo["name"]

        if name == "repository-full-name":
            return ctxt.repository.repo["full_name"]

        if name in (
            "dependabot-dependency-name",
            "dependabot-dependency-type",
            "dependabot-update-type",
        ):
            dependabot_attributes = await ctxt.dependabot_attributes
            if name == "dependabot-dependency-name":
                return [da["dependency-name"] for da in dependabot_attributes]
            if name == "dependabot-dependency-type":
                return [da["dependency-type"] for da in dependabot_attributes]
            if name == "dependabot-update-type":
                return [
                    da["update-type"]
                    for da in dependabot_attributes
                    if "update-type" in da
                ]
            raise PullRequestAttributeError(name)

        if name == "branch-protection-review-decision":
            return await ctxt.retrieve_review_decision()

        if name == "co-authors":
            return {
                CommitAuthor(commit.author, commit.email_author)
                for commit in await ctxt.commits
                if commit.gh_author_login != ctxt.pull["user"]["login"]
                and len(commit.parents) == 1
                and not commit.author.endswith("[bot]")
            }

        raise PullRequestAttributeError(name)


@dataclasses.dataclass
class PullRequest(BasePullRequest):
    """A high level pull request object.

    This object is used for templates and rule evaluations.
    """

    context: context_mod.Context

    BOOLEAN_ATTRIBUTES: typing.ClassVar[set[str]] = {
        "merged",
        "closed",
        "locked",
        "linear-history",
        "conflict",
        "branch-protection-review-decision",
    }
    NUMBER_ATTRIBUTES: typing.ClassVar[set[str]] = {
        "number",
        "queue-position",
    }
    STRING_ATTRIBUTES: typing.ClassVar[set[str]] = {
        "author",
        "merged-by",
        "milestone",
        "base",
        "head",
        "head-repo-full-name",
        "title",
        "body",
        "body-raw",
        "repository-name",
        "repository-full-name",
        "queue-dequeue-reason",
    }

    LIST_ATTRIBUTES: typing.ClassVar[set[str]] = {
        "assignee",
        "assignees",
        "label",
        "review-requested",
        "approved-reviews-by",
        "dismissed-reviews-by",
        "changes-requested-reviews-by",
        "commented-reviews-by",
        "check-success",
        "check-success-or-neutral",
        "check-failure",
        "check-neutral",
        "check-timed-out",
        "status-success",
        "status-failure",
        "status-neutral",
        "check-skipped",
        "check-pending",
        "check-stale",
        "commits",
        "commits-unverified",
        "review-threads-resolved",
        "review-threads-unresolved",
        "files",
        "added-files",
        "modified-files",
        "removed-files",
        "co-authors",
    }

    LIST_ATTRIBUTES_WITH_LENGTH_OPTIMIZATION: typing.ClassVar[set[str]] = {
        "#files",
        "#commits",
        "#commits-behind",
    }

    async def __getattr__(self, name: str) -> PullRequestAttributeType:
        return await self._get_consolidated_data(self.context, name.replace("_", "-"))

    def __iter__(self) -> abc.Iterator[str]:
        return iter(
            self.STRING_ATTRIBUTES
            | self.NUMBER_ATTRIBUTES
            | self.BOOLEAN_ATTRIBUTES
            | self.LIST_ATTRIBUTES
            | self.LIST_ATTRIBUTES_WITH_LENGTH_OPTIMIZATION,
        )

    @staticmethod
    def _markdownify(s: str) -> str:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=bs4.MarkupResemblesLocatorWarning,
            )
            return typing.cast(str, markdownify.markdownify(s))

    async def render_template(
        self,
        template: str,
        extra_variables: None | (dict[str, str | bool]) = None,
        allow_get_section: bool = True,
        mandatory_template_variables: dict[str, str] | None = None,
    ) -> str:
        if mandatory_template_variables is None:
            mandatory_template_variables = {}

        """Render a template interpolating variables based on pull request attributes."""
        env = jinja2.sandbox.ImmutableSandboxedEnvironment(
            undefined=jinja2.StrictUndefined,
            enable_async=True,
        )
        env.filters["markdownify"] = self._markdownify
        if allow_get_section:
            env.filters["get_section"] = functools.partial(
                self._filter_get_section,
                self,
            )

        with self._template_exceptions_mapping():
            used_variables = jinja2.meta.find_undeclared_variables(env.parse(template))

            for variable, template_to_inject in mandatory_template_variables.items():
                if variable not in used_variables:
                    template += template_to_inject
                    used_variables.add(variable)

            infos = {}
            for k in sorted(used_variables):
                if extra_variables and k in extra_variables:
                    infos[k] = extra_variables[k]
                else:
                    infos[k] = await getattr(self, k)
            return await env.from_string(template).render_async(**infos)

    @staticmethod
    async def _filter_get_section(
        pull: PullRequest,
        v: str,
        section: str,
        default: str | None = None,
    ) -> str:
        if not isinstance(section, str):
            raise jinja2.exceptions.TemplateError("level must be a string")

        section_escaped = re.escape(section)
        level = MARKDOWN_TITLE_RE.match(section)

        if level is None:
            raise jinja2.exceptions.TemplateError("section level not found")

        level_str = level[0].strip()

        level_re = re.compile(rf"^{level_str} +", re.I)
        section_re = re.compile(rf"^{section_escaped}\s*$", re.I)

        found = False
        section_lines = []
        for line in v.split("\n"):
            if section_re.match(line):
                found = True
            elif found and level_re.match(line):
                break
            elif found:
                section_lines.append(line.strip())

        if found:
            text = ("\n".join(section_lines)).strip()
        elif default is None:
            raise jinja2.exceptions.TemplateError("section not found")
        else:
            text = default

        # We don't allow get_section to avoid never-ending recursion
        return await pull.render_template(text, allow_get_section=False)

    @staticmethod
    @contextlib.contextmanager
    def _template_exceptions_mapping() -> abc.Iterator[None]:
        try:
            yield
        except KeyError:
            # NOTE(sileht): it's far from being ideal, but I don't find a better place
            # to catch issue with str.format(), Jinja2 has a customer "safe" version of this
            # but KeyError from this formatter are not converted into a TemplateError
            # NOTE(sileht): we cannot use the KeyError message to avoid code/html injection
            raise RenderTemplateFailure("invalid arguments passed to format()")
        except jinja2.exceptions.TemplateSyntaxError as tse:
            raise RenderTemplateFailure(tse.message or "", tse.lineno)
        except jinja2.exceptions.TemplateError as te:
            raise RenderTemplateFailure(te.message or "")
        except PullRequestAttributeError as e:
            raise RenderTemplateFailure(f"Unknown pull request attribute: {e.name}")

    async def get_commit_message(
        self,
        template: str | None = None,
    ) -> tuple[str, str] | None:
        if template is None:
            # No template from configuration, looks at template from body
            body = typing.cast(str, await self.body)
            if not body:
                return None
            found = False
            message_lines = []

            for line in body.split("\n"):
                if MARKDOWN_COMMIT_MESSAGE_RE.match(line):
                    found = True
                elif found and MARKDOWN_TITLE_RE.match(line):
                    break
                elif found:
                    message_lines.append(line)
                if found:
                    self.context.log.debug("Commit message template found in body")
                    template = "\n".join(line.strip() for line in message_lines)

        if template is None:
            return None

        commit_message = await self.render_template(template.strip())
        if not commit_message:
            return None

        template_title, _, template_message = commit_message.partition("\n")
        return (template_title, template_message.lstrip())


@dataclasses.dataclass
class QueuePullRequest(BasePullRequest):
    """Same as PullRequest but for temporary pull request used by merge train.

    This object is used for templates and rule evaluations.
    """

    context: context_mod.Context
    queue_context: context_mod.Context

    # These attributes are evaluated on the temporary pull request or are
    # always the same within the same batch
    QUEUE_ATTRIBUTES = (
        "base",
        "status-success",
        "status-failure",
        "status-neutral",
        "check",
        "check-success",
        "check-success-or-neutral",
        "check-success-or-neutral-or-pending",
        "check-failure",
        "check-neutral",
        "check-skipped",
        "check-timed-out",
        "check-pending",
        "check-stale",
        "current-datetime",
        "schedule",
        "queue-merge-started-at",
        "queue-merge-started-at-relative",
        "files",
        "added-files",
        "modified-files",
        "removed-files",
        "queue-partition-name",
    )

    async def __getattr__(self, name: str) -> PullRequestAttributeType:
        fancy_name = name.replace("_", "-")
        if fancy_name in self.QUEUE_ATTRIBUTES:
            if fancy_name == "queue-partition-name":
                fancy_name = "queue-current-partition-name"
            return await self._get_consolidated_data(self.queue_context, fancy_name)
        return await self._get_consolidated_data(self.context, fancy_name)


@dataclasses.dataclass
class CommandPullRequest(PullRequest):
    """A high level pull request object aimed for commands.

    This object is used for evalutating command restrictions on users.
    """

    context: context_mod.Context
    sender: github_types.GitHubLogin
    sender_permission: github_types.GitHubRepositoryPermission

    async def __getattr__(self, name: str) -> PullRequestAttributeType:
        if name == "sender":
            return self.sender
        if name == "sender-permission":
            return self.sender_permission
        return await super().__getattr__(name)
