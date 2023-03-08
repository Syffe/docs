import datetime

from mergify_engine.tests.unit.ci import utils


def test_timing() -> None:
    job = utils.create_job(timing=10)
    assert job.timing == datetime.timedelta(seconds=10)


def test_no_lifecycle() -> None:
    job = utils.create_job(triggering_event="schedule")
    assert job.lifecycle is None


def test_lifecycle_manual_retry() -> None:
    job = utils.create_job(triggering_event="pull_request_target", run_attempt=2)
    assert job.lifecycle == "retry"


def test_lifecycle_initial_push() -> None:
    job = utils.create_job(triggering_event="pull_request", run_attempt=1)
    assert job.lifecycle == "push"
