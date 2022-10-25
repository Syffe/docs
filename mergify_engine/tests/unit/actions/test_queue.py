from collections import abc
import typing
from unittest import mock

import pytest
import voluptuous

from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.clients import http
from mergify_engine.rules import checks_status
from mergify_engine.tests.unit import conftest


def pull_request_rule_from_list(lst: typing.Any) -> rules.PullRequestRules:
    return typing.cast(
        rules.PullRequestRules,
        voluptuous.Schema(rules.get_pull_request_rules_schema())(lst),
    )


@pytest.fixture
def fake_client() -> mock.Mock:
    async def items_call(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> abc.AsyncGenerator[github_types.GitHubCheckRun, None]:
        if url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/status":
            return
        elif url == "/repos/Mergifyio/mergify-engine/commits/the-head-sha/check-runs":
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "failure",
                    "name": "failure",
                    "id": 1234,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "success",
                    "name": "success",
                    "id": 1235,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "completed",
                    "conclusion": "neutral",
                    "name": "neutral",
                    "id": 1236,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
            yield github_types.GitHubCheckRun(
                {
                    "head_sha": github_types.SHAType(
                        "ce587453ced02b1526dfb4cb910479d431683101"
                    ),
                    "details_url": "https://example.com",
                    "status": "in_progress",
                    "conclusion": None,
                    "name": "pending",
                    "id": 1237,
                    "app": {
                        "id": 1234,
                        "name": "CI",
                        "owner": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "external_id": "",
                    "pull_requests": [],
                    "before": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "after": github_types.SHAType(
                        "4eef79d038b0327a5e035fd65059e556a55c6aa4"
                    ),
                    "started_at": github_types.ISODateTimeType(""),
                    "completed_at": None,
                    "html_url": "https://example.com",
                    "check_suite": {"id": 1234},
                    "output": {
                        "summary": "",
                        "title": "It runs!",
                        "text": "",
                        "annotations": [],
                        "annotations_count": 0,
                        "annotations_url": "https://example.com",
                    },
                }
            )
        else:
            raise Exception(f"url not mocked: {url}")

    def item_call(
        url: str, *args: typing.Any, **kwargs: typing.Any
    ) -> github_types.GitHubBranch:
        if url == "/repos/Mergifyio/mergify-engine/branches/main":
            return github_types.GitHubBranch(
                {
                    "commit": {
                        "sha": github_types.SHAType("sha1"),
                        "parents": [],
                        "commit": {
                            "message": "",
                            "verification": {"verified": False},
                            "committer": {
                                "email": "",
                                "name": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                            "author": {
                                "email": "",
                                "name": "",
                                "date": github_types.ISODateTimeType(""),
                            },
                        },
                        "committer": {
                            "type": "User",
                            "id": github_types.GitHubAccountIdType(1234),
                            "login": github_types.GitHubLogin("goo"),
                            "avatar_url": "https://example.com",
                        },
                    },
                    "protection": {
                        "enabled": False,
                        "required_status_checks": {"contexts": [], "strict": False},
                    },
                    "protected": False,
                    "name": github_types.GitHubRefType("foobar"),
                }
            )
        if url == "/repos/Mergifyio/mergify-engine/branches/main/protection":
            raise http.HTTPNotFound(
                message="boom", response=mock.Mock(), request=mock.Mock()
            )
        else:
            raise Exception(f"url not mocked: {url}")

    client = mock.Mock()
    client.item = mock.AsyncMock(side_effect=item_call)
    client.items = items_call
    return client


@pytest.mark.parametrize(
    "conditions,conclusion",
    (
        (
            ["title=awesome", "check-neutral:neutral", "check-success:success"],
            check_api.Conclusion.SUCCESS,
        ),
        (
            ["title!=awesome", "check-neutral:neutral", "check-success:success"],
            check_api.Conclusion.FAILURE,
        ),
        (
            ["title=awesome", "check-neutral:neutral", "check-success:pending"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:pending", "check-success:pending"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:notexists", "check-success:success"],
            check_api.Conclusion.PENDING,
        ),
        (
            ["title=awesome", "check-neutral:failure", "check-success:success"],
            check_api.Conclusion.FAILURE,
        ),
        (
            [
                {
                    "or": [
                        {"and": ["title=awesome", "check-success:pending"]},
                        {"and": ["title!=awesome", "check-success:pending"]},
                    ]
                }
            ],
            check_api.Conclusion.PENDING,
        ),
        (
            [
                {
                    "or": [
                        {"and": ["title!=awesome", "check-success:pending"]},
                        {"and": ["title=foobar", "check-success:pending"]},
                    ]
                }
            ],
            check_api.Conclusion.FAILURE,
        ),
        (
            [
                {
                    "not": {
                        "and": [
                            "title!=awesome",
                            "check-neutral:neutral",
                            "check-success:success",
                        ]
                    },
                },
            ],
            check_api.Conclusion.SUCCESS,
        ),
    ),
)
async def test_get_rule_checks_status(
    conditions: typing.Any,
    conclusion: check_api.Conclusion,
    context_getter: conftest.ContextGetterFixture,
    fake_client: mock.Mock,
) -> None:
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = fake_client
    rules = pull_request_rule_from_list(
        [
            {
                "name": "hello",
                "conditions": conditions,
                "actions": {},
            }
        ]
    )
    match = await rules.get_pull_request_rule(ctxt)
    evaluated_rule = match.matching_rules[0]
    assert (
        await checks_status.get_rule_checks_status(
            ctxt.log, ctxt.repository, [ctxt.pull_request], evaluated_rule
        )
    ) == conclusion
