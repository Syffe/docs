import argparse
from collections import abc
import io
import operator
import time

import anys
import daiquiri
import pydantic
import pytest

from mergify_engine import count_seats
from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import settings
from mergify_engine import yaml
from mergify_engine.tests.functional import base


LOG = daiquiri.getLogger(__name__)


class TestCountSeats(base.FunctionalTestBase):
    @pytest.fixture(autouse=True)
    def prepare_fixture(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> abc.Generator[None, None, None]:
        monkeypatch.setattr(
            settings, "SUBSCRIPTION_TOKEN", pydantic.SecretStr("something")
        )
        yield

    async def _prepare_repo(self) -> count_seats.Seats:
        await self.setup_repo()
        await self.create_pr(as_="admin")
        await self.create_pr(as_="fork")
        await self.run_engine()

        # NOTE(sileht): we add active users only on the repository used for
        # recording the fixture

        organization = {}
        active_users = None
        key_repo = count_seats.SeatRepository(
            github_types.GitHubRepositoryIdType(self.repository_ctxt.repo["id"]),
            github_types.GitHubRepositoryName(self.repository_ctxt.repo["name"]),
        )
        active_users = {
            count_seats.ActiveUser(
                github_types.GitHubAccountIdType(settings.TESTING_MERGIFY_TEST_1_ID),
                github_types.GitHubLogin("mergify-test1"),
                date.utcnow(),
            ),
            count_seats.ActiveUser(
                github_types.GitHubAccountIdType(settings.TESTING_MERGIFY_TEST_2_ID),
                github_types.GitHubLogin("mergify-test2"),
                date.utcnow(),
            ),
        }
        organization[key_repo] = count_seats.CollaboratorsSetsT(
            {
                "active_users": active_users,
            }
        )

        collaborators = {
            count_seats.SeatAccount(
                github_types.GitHubAccountIdType(settings.TESTING_ORGANIZATION_ID),
                github_types.GitHubLogin(settings.TESTING_ORGANIZATION_NAME),
            ): organization
        }

        return count_seats.Seats(collaborators)

    async def test_get_collaborators(self) -> None:
        expected_seats = await self._prepare_repo()
        assert (
            await count_seats.Seats.get(self.redis_links.active_users)
        ).seats == expected_seats.seats

    async def test_count_seats(self) -> None:
        await self._prepare_repo()
        seats_count = (
            await count_seats.Seats.get(self.redis_links.active_users)
        ).count()
        assert seats_count.active_users == 2

    async def test_run_count_seats_report(self) -> None:
        await self.setup_repo()
        await self.create_pr(as_="admin")
        await self.create_pr(as_="fork")
        await self.run_engine()
        if github_types.GitHubAccountIdType(settings.TESTING_MERGIFY_TEST_1_ID) is None:
            raise RuntimeError("client_admin owner_id is None")
        if github_types.GitHubAccountIdType(settings.TESTING_MERGIFY_TEST_2_ID) is None:
            raise RuntimeError("client_fork owner_id is None")
        if github_types.GitHubLogin("mergify-test1") is None:
            raise RuntimeError("client_admin owner is None")
        if github_types.GitHubLogin("mergify-test2") is None:
            raise RuntimeError("client_fork owner is None")
        args = argparse.Namespace(json=True, daemon=False)
        assert database.APP_STATE is not None
        await database.APP_STATE["engine"].dispose()
        database.APP_STATE = None

        fake_stdout = io.StringIO()
        await count_seats.report(args, fake_stdout)
        json_reports = json.loads(fake_stdout.getvalue())

        assert list(json_reports.keys()) == ["organizations"]
        assert len(json_reports["organizations"]) == 1

        org = json_reports["organizations"][0]
        assert org["id"] == settings.TESTING_ORGANIZATION_ID
        assert org["login"] == settings.TESTING_ORGANIZATION_NAME

        assert len(org["repositories"]) == 1
        repo = org["repositories"][0]
        assert sorted(
            repo["collaborators"]["active_users"],
            key=operator.itemgetter("id"),
        ) == sorted(
            [
                {
                    "id": github_types.GitHubAccountIdType(
                        settings.TESTING_MERGIFY_TEST_1_ID
                    ),
                    "login": github_types.GitHubLogin("mergify-test1"),
                    "seen_at": anys.ANY_AWARE_DATETIME_STR,
                },
                {
                    "id": github_types.GitHubAccountIdType(
                        settings.TESTING_MERGIFY_TEST_2_ID
                    ),
                    "login": github_types.GitHubLogin("mergify-test2"),
                    "seen_at": anys.ANY_AWARE_DATETIME_STR,
                },
            ],
            key=operator.itemgetter("id"),
        )
        assert len(repo["collaborators"]["active_users"]) == 2

    async def test_stored_user_in_redis(self) -> None:
        rules = {
            "pull_request_rules": [
                {
                    "name": "no-draft",
                    "conditions": ["created-at<9999 days ago"],
                    "actions": {"comment": {"message": "it's time"}},
                }
            ]
        }
        await self.setup_repo(yaml.dump(rules))
        await self.create_pr(as_="admin")
        await self.create_pr(as_="fork")
        await self.run_engine()
        repository_id = self.RECORD_CONFIG["repository_id"]
        organization_id = self.RECORD_CONFIG["organization_id"]
        repository_name = self.RECORD_CONFIG["repository_name"]
        organization_name = self.RECORD_CONFIG["organization_name"]
        key = f"active-users~{organization_id}~{organization_name}~{repository_id}~{repository_name}"
        active_users: list[
            tuple[bytes, float]
        ] = await self.redis_links.active_users.zrangebyscore(
            key, min="-inf", max="+inf", withscores=True
        )
        now = time.time()
        assert len(active_users) == 2
        user_admin, timestamp_admin = active_users[0]
        user_fork, timestamp_fork = active_users[1]
        assert timestamp_admin <= now and timestamp_admin > now - 60
        assert (
            user_admin == f"{settings.TESTING_MERGIFY_TEST_1_ID}~mergify-test1".encode()
        )
        assert timestamp_fork <= now and timestamp_fork > now - 60
        assert (
            user_fork == f"{settings.TESTING_MERGIFY_TEST_2_ID}~mergify-test2".encode()
        )
