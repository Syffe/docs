from mergify_engine import context
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config


async def handle(
    ctxt: context.Context,
    queue_rules: qr_config.QueueRules,
    partition_rules: partr_config.PartitionRules,
) -> None:
    # FIXME: Maybe create a command to force the retesting to put back the PR in the queue?

    convoy = await merge_train.Convoy.from_context(ctxt, queue_rules, partition_rules)

    cars = convoy.get_train_cars_by_tmp_pull(ctxt)
    if not cars:
        if ctxt.closed:
            ctxt.log.info(
                "train car temporary pull request have been closed",
                sources=ctxt.sources,
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

    for car in cars:
        await _handle_car(ctxt, car, queue_rules)


async def _handle_car(
    ctxt: context.Context,
    car: merge_train.TrainCar,
    queue_rules: qr_config.QueueRules,
) -> None:
    if (
        car.train_car_state.outcome != merge_train.TrainCarOutcome.UNKNOWN
        and ctxt.closed
    ):
        ctxt.log.info(
            "train car temporary pull request has been closed",
            sources=ctxt.sources,
            partition_name=car.train.partition_name,
        )
        return

    if car.queue_pull_request_number is None:
        raise RuntimeError(
            "Got draft pull request event on car without queue_pull_request_number",
        )

    ctxt.log.info(
        "handling train car temporary pull request event",
        sources=ctxt.sources,
        gh_pulls_queued=[
            ep.user_pull_request_number for ep in car.still_queued_embarked_pulls
        ],
        partition_name=car.train.partition_name,
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
            partition_name=car.train.partition_name,
        )
        return

    await car.check_mergeability(
        origin="draft_pull_request",
        original_pull_request_rule=None,
        original_pull_request_number=None,
    )
