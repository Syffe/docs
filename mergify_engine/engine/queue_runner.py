import datetime
import typing

from first import first

from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.queue import merge_train
from mergify_engine.rules import checks_status


async def have_unexpected_draft_pull_request_changes(
    ctxt: context.Context, car: merge_train.TrainCar
) -> bool:
    unexpected_event = first(
        (source for source in ctxt.sources),
        key=lambda s: s["event_type"] == "pull_request"
        and typing.cast(github_types.GitHubEventPullRequest, s["data"])["action"]
        in ["closed", "reopened", "synchronize"],
    )
    if unexpected_event:
        ctxt.log.info(
            "train car received an unexpected event",
            unexpected_event=unexpected_event,
        )
        return True

    return False


async def handle(queue_rules: rules.QueueRules, ctxt: context.Context) -> None:
    # FIXME: Maybe create a command to force the retesting to put back the PR in the queue?

    train = await merge_train.Train.from_context(ctxt)

    car = train.get_car_by_tmp_pull(ctxt)
    if not car:
        if ctxt.closed:
            ctxt.log.info(
                "train car temporary pull request has been closed", sources=ctxt.sources
            )
        else:
            # NOTE(sileht): no need to close the PR, GitHub will do it for us.
            ctxt.log.info(
                "train car not found, deleting the merge-queue branch",
                sources=ctxt.sources,
                branch=ctxt.pull["head"]["ref"],
            )
            await ctxt.repository.delete_branch_if_exists(ctxt.pull["head"]["ref"])
        return

    if (
        car.train_car_state.outcome != merge_train.TrainCarOutcome.UNKNWON
        and ctxt.closed
    ):
        ctxt.log.info(
            "train car temporary pull request has been closed", sources=ctxt.sources
        )
        return

    if car.queue_pull_request_number is None:
        raise RuntimeError(
            "Got draft pull request event on car without queue_pull_request_number"
        )

    ctxt.log.info(
        "handling train car temporary pull request event",
        sources=ctxt.sources,
        gh_pulls_queued=[
            ep.user_pull_request_number for ep in car.still_queued_embarked_pulls
        ],
    )

    queue_name = car.still_queued_embarked_pulls[0].config["name"]
    try:
        queue_rule = queue_rules[queue_name]
    except KeyError:
        ctxt.log.warning(
            "queue_rule not found for this train car",
            gh_pulls_queued=[
                ep.user_pull_request_number for ep in car.still_queued_embarked_pulls
            ],
            queue_rules=queue_rules,
            queue_name=queue_name,
        )
        return

    pull_requests = await car.get_pull_requests_to_evaluate()
    evaluated_queue_rule = await queue_rule.get_evaluated_queue_rule(
        ctxt.repository,
        ctxt.pull["base"]["ref"],
        pull_requests,
    )

    for pull_request in pull_requests:
        await delayed_refresh.plan_next_refresh(
            ctxt, [evaluated_queue_rule], pull_request
        )

    if not ctxt.sources:
        # NOTE(sileht): Only comment/command, don't need to go further
        return None

    try:
        current_base_sha = await train.get_base_sha()
    except merge_train.BaseBranchVanished:
        ctxt.log.warning("target branch vanished, the merge queue will be deleted soon")
        return None

    unexpected_changes: typing.Optional[merge_train.UnexpectedChange] = None
    queue_config = await train.get_queue_rule(
        car.initial_embarked_pulls[0].config["name"]
    )
    if not queue_config.config[
        "allow_queue_branch_edit"
    ] and await have_unexpected_draft_pull_request_changes(ctxt, car):
        unexpected_changes = merge_train.UnexpectedDraftPullRequestChange(
            car.queue_pull_request_number
        )
    else:
        if not await train.is_synced_with_the_base_branch(current_base_sha):
            unexpected_changes = merge_train.UnexpectedBaseBranchChange(
                current_base_sha
            )

    status = await checks_status.get_rule_checks_status(
        ctxt.log,
        ctxt.repository,
        pull_requests,
        evaluated_queue_rule,
        unmatched_conditions_return_failure=False,
    )

    await car.update_state(
        status, evaluated_queue_rule, unexpected_change=unexpected_changes
    )
    await car.update_summaries()
    await train.save()

    ctxt.log.info(
        "train car temporary pull request evaluation",
        gh_pull_queued=[
            ep.user_pull_request_number for ep in car.still_queued_embarked_pulls
        ],
        evaluated_queue_rule=evaluated_queue_rule.conditions.get_summary(),
        unexpected_changes=unexpected_changes,
        status=status,
        event_types=[se["event_type"] for se in ctxt.sources],
        outcome=car.train_car_state.outcome,
        ci_state=car.train_car_state.ci_state,
        ci_ended_at=car.train_car_state.ci_ended_at,
        checks_end_at=car.checks_ended_timestamp,
    )

    # NOTE(Syffe): In order to differentiate the two types of unexpected_changes, and
    # only reset the train when it is a base branch change
    if isinstance(unexpected_changes, merge_train.UnexpectedBaseBranchChange):
        ctxt.log.info(
            "train will be reset",
            gh_pull_queued=[
                ep.user_pull_request_number for ep in car.still_queued_embarked_pulls
            ],
            unexpected_changes=unexpected_changes,
        )
        await train.reset(unexpected_changes)

        await ctxt.client.post(
            f"{ctxt.base_url}/issues/{ctxt.pull['number']}/comments",
            json={
                "body": f"This pull request has unexpected changes: {unexpected_changes}. The whole train will be reset."
            },
        )

    # NOTE(sileht): we are supposed to be triggered by GitHub events, but in
    # case we miss some of them due to an outage, this is a seatbelt to recover
    # automatically after 3 minutes
    refresh_at = date.utcnow() + datetime.timedelta(minutes=3)
    await delayed_refresh.plan_refresh_at_least_at(
        ctxt.repository, ctxt.pull["number"], refresh_at
    )
