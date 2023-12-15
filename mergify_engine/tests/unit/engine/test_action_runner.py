from unittest import mock

from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.engine import actions_runner
from mergify_engine.queue import merge_train
from mergify_engine.tests.unit import conftest


async def test_cleanup_pending_actions_with_no_associated_rules(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(42)
    previous_conclusions = {
        "Rule: title contains cleanup (label)": check_api.Conclusion.CANCELLED,
        "Rule: ask to resolve conflict (comment)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains Helm (label)": check_api.Conclusion.CANCELLED,
        "Rule: remove outdated approvals (queue)": check_api.Conclusion.PENDING,
        "Rule: title contains build (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge (queue)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains rebase (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains CI, testing or e2e (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge (delete_head_branch)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains DNM (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains CephFS (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains doc (label)": check_api.Conclusion.SUCCESS,
        "Rule: automatic merge PR having ready-to-merge label (delete_head_branch)": check_api.Conclusion.NEUTRAL,
        "Rule: automatic merge PR having ready-to-merge label (dismiss_reviews)": check_api.Conclusion.CANCELLED,
        "Rule: title indicates a bug fix (label)": check_api.Conclusion.CANCELLED,
        "Rule: backport patches to release-v3.4 branch (backport)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains RBD (label)": check_api.Conclusion.CANCELLED,
        "Rule: ask to resolve conflict (queue)": check_api.Conclusion.NEUTRAL,
        "Rule: automatic merge PR having ready-to-merge label (queue)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains Mergify (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge (dismiss_reviews)": check_api.Conclusion.CANCELLED,
        "Rule: remove outdated approvals (dismiss_reviews)": check_api.Conclusion.SUCCESS,
    }
    current_conclusions = {
        "Rule: title indicates a bug fix (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains doc (label)": check_api.Conclusion.SUCCESS,
        "Rule: backport patches to release-v3.4 branch (backport)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains rebase (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains build (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains RBD (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge (queue)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains Mergify (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains cleanup (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge PR having ready-to-merge label (delete_head_branch)": check_api.Conclusion.NEUTRAL,
        "Rule: automatic merge PR having ready-to-merge label (queue)": check_api.Conclusion.NEUTRAL,
        "Rule: automatic merge PR having ready-to-merge label (dismiss_reviews)": check_api.Conclusion.CANCELLED,
        "Rule: ask to resolve conflict (comment)": check_api.Conclusion.NEUTRAL,
        "Rule: automatic merge (dismiss_reviews)": check_api.Conclusion.CANCELLED,
        "Rule: title contains CephFS (label)": check_api.Conclusion.CANCELLED,
        "Rule: automatic merge (delete_head_branch)": check_api.Conclusion.NEUTRAL,
        "Rule: title contains DNM (label)": check_api.Conclusion.CANCELLED,
        "Rule: title contains CI, testing or e2e (label)": check_api.Conclusion.CANCELLED,
        "Rule: remove outdated approvals (dismiss_reviews)": check_api.Conclusion.SUCCESS,
        "Rule: title contains Helm (label)": check_api.Conclusion.CANCELLED,
    }
    checks = [
        github_types.CachedGitHubCheckRun(
            name=check,
            id=1,
            app_id=settings.GITHUB_APP_ID,
            app_name="mergify",
            app_avatar_url="",
            app_slug="mergify",
            external_id="",
            head_sha=github_types.SHAType("azertyio"),
            status="in_progress",
            output={
                "title": "",
                "summary": "",
                "text": None,
                "annotations_count": 0,
                "annotations_url": "",
            },
            conclusion=None,
            completed_at=github_types.ISODateTimeType(""),
            html_url="",
        )
        for check, state in previous_conclusions.items()
        if state == check_api.Conclusion.PENDING
    ]
    ctxt._caches.pull_check_runs.set(checks)
    with (
        mock.patch.object(merge_train.Convoy, "force_remove_pull") as force_remove_pull,
        mock.patch.object(check_api, "set_check_run") as set_check_run,
    ):
        await actions_runner.cleanup_pending_actions_with_no_associated_rules(
            ctxt,
            current_conclusions,
            previous_conclusions,
        )
        assert set_check_run.called
        assert force_remove_pull.called
