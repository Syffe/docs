import datetime

import sqlalchemy

from mergify_engine import database
from mergify_engine import settings
from mergify_engine.models import github_account
from mergify_engine.models import github_actions as sql_models
from mergify_engine.tests.functional import base


class TestCIApi(base.FunctionalTestBase):
    SUBSCRIPTION_ACTIVE = True

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        async with database.create_session() as db:
            await db.execute(
                sqlalchemy.insert(github_account.GitHubAccount).values(
                    id=1, login=settings.TESTING_ORGANIZATION_NAME
                )
            )
            sql = sqlalchemy.insert(sql_models.JobRun).values(
                id=1,
                workflow_run_id=1,
                workflow_id=1,
                name="some-job-1",
                owner_id=1,
                repository=self.RECORD_CONFIG["repository_name"],
                conclusion=sql_models.JobRunConclusion.FAILURE,
                triggering_event=sql_models.JobRunTriggerEvent.PULL_REQUEST,
                triggering_actor_id=1,
                started_at=datetime.datetime.now(),
                completed_at=datetime.datetime.now(),
                run_attempt=2,
                operating_system=sql_models.JobRunOperatingSystem.LINUX,
                cores=4,
            )
            await db.execute(sql)
            await db.commit()

    async def test_report(self) -> None:
        r = await self.app.get(f"/v1/ci/{settings.TESTING_ORGANIZATION_NAME}")

        assert r.status_code == 200
        assert r.json()["total_costs"] == {"amount": 0.02, "currency": "USD"}
        assert r.json()["total_difference"] == {"amount": 0, "currency": "USD"}
        assert "date_range" in r.json()
        assert r.json()["date_range"]["start_at"] is None
        assert r.json()["date_range"]["end_at"] is None
        assert "compared_date_range" in r.json()
        assert r.json()["compared_date_range"]["start_at"] is None
        assert r.json()["compared_date_range"]["end_at"] is None
        assert "deployments" in r.json()["categories"]
        assert "scheduled_jobs" in r.json()["categories"]
        assert "pull_requests" in r.json()["categories"]

        r = await self.app.get(
            f"/v1/ci/{settings.TESTING_ORGANIZATION_NAME}?repository={self.RECORD_CONFIG['repository_name']}",
        )
        assert r.status_code == 200

        r = await self.app.get(
            f"/v1/ci/{settings.TESTING_ORGANIZATION_NAME}?repository={self.RECORD_CONFIG['repository_name']}&start_at=2023-01-01&end_at=2023-01-15",
        )
        assert r.status_code == 200
        assert "date_range" in r.json()
        assert r.json()["date_range"]["start_at"] == "2023-01-01"
        assert r.json()["date_range"]["end_at"] == "2023-01-15"
        assert "compared_date_range" in r.json()
        assert r.json()["compared_date_range"]["start_at"] == "2022-12-17"
        assert r.json()["compared_date_range"]["end_at"] == "2022-12-31"
