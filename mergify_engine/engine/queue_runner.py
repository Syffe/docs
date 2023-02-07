from mergify_engine import context
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import queue_rules as qr_config


async def handle(queue_rules: qr_config.QueueRules, ctxt: context.Context) -> None:
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
                "train car not found, deleting the merge queue branch",
                sources=ctxt.sources,
                branch=ctxt.pull["head"]["ref"],
            )
            await ctxt.repository.delete_branch_if_exists(ctxt.pull["head"]["ref"])
        return

    if (
        car.train_car_state.outcome != merge_train.TrainCarOutcome.UNKNOWN
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
        queue_rules[queue_name]
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

    await car.check_mergeability(
        queue_rules,
        origin="draft_pull_request",
        original_pull_request_rule=None,
        original_pull_request_number=None,
    )
