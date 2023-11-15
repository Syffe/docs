import dataclasses
import datetime

import fastapi
import pydantic

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.queue import merge_train
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.web import api
from mergify_engine.web.api import security
from mergify_engine.web.api.queues import estimated_time_to_merge
from mergify_engine.web.api.queues import types as queue_types
from mergify_engine.web.api.queues.index import BriefMergeabilityCheck


router = fastapi.APIRouter(
    tags=["queues"],
    dependencies=[
        fastapi.Security(security.require_authentication),
    ],
)


@pydantic.dataclasses.dataclass
class PullRequestQueued:
    number: github_types.GitHubPullRequestNumber = pydantic.Field(
        description="The number of the pull request",
    )
    position: int = pydantic.Field(
        description="The position of the pull request in the queue. The first pull request in the queue is at position 0",
    )
    priority: int = pydantic.Field(description="The priority of this pull request")
    effective_priority: int = pydantic.Field(
        description="The effective priority of this pull request",
    )
    queue_rule: queue_types.QueueRule = pydantic.Field(
        description="The queue rule associated to this pull request",
    )
    queued_at: datetime.datetime = pydantic.Field(
        description="The timestamp when the pull requested has entered in the queue",
    )
    mergeability_check: BriefMergeabilityCheck | None = pydantic.Field(
        description="Information about the mergeability check currently processed",
    )
    estimated_time_of_merge: datetime.datetime | None = pydantic.Field(
        description="The estimated timestamp when this pull request will be merged",
    )


@pydantic.dataclasses.dataclass
class Partition:
    pull_requests: list[PullRequestQueued] = dataclasses.field(
        metadata={"description": "The pull requests queued in this partition"},
    )


@pydantic.dataclasses.dataclass
class BranchPartitions:
    branch_name: github_types.GitHubRefType = dataclasses.field(
        metadata={"description": ""},
    )

    partitions: dict[
        partr_config.PartitionRuleName,
        list[PullRequestQueued],
    ] = dataclasses.field(
        metadata={
            "description": (
                "A dictionary containing partition names as keys and, as a value of those key, the list of pull requests queued in the partition."
                f" If partition are not used in this repository, the default partition name used will be `{partr_config.DEFAULT_PARTITION_NAME}`."
            ),
        },
    )


@router.get(
    "/partitions",
    summary="Get all the partitions",
    description="Get the list of pull requests queued in each merge queue, organized by partition name",
    response_model=list[BranchPartitions],
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def repository_partitions(
    session: database.Session,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
) -> list[BranchPartitions]:
    partition_list = []

    async for convoy in merge_train.Convoy.iter_convoys(
        repository_ctxt,
        queue_rules,
        partition_rules,
    ):
        branch_partitions = BranchPartitions(branch_name=convoy.ref, partitions={})

        for rule in partition_rules:
            branch_partitions.partitions.setdefault(rule.name, [])
        if not partition_rules:
            branch_partitions.partitions.setdefault(
                partr_config.DEFAULT_PARTITION_NAME,
                [],
            )

        for train in convoy.iter_trains():
            previous_eta = None
            previous_car = None
            previous_queue = None
            previous_queue_idx = 0
            for position, (embarked_pull, car) in enumerate(
                train._iter_embarked_pulls(),
            ):
                try:
                    queue_rule = queue_rules[embarked_pull.config["name"]]
                except KeyError:
                    # This car is going to be deleted so skip it
                    continue

                if previous_queue is None:
                    previous_queue = embarked_pull.config["name"]

                # We need this to get a position per queue in order for
                # the ETA calculation to be correct, otherwise we might get
                # two PRs in different queues with the same ETA, while the
                # lower priority queue one doesn't have any checks started yet.
                if embarked_pull.config["name"] != previous_queue:
                    # not `- position - 1` in order to get 0,
                    # since embarked_pull index starts at 0
                    previous_queue_idx = position

                previous_eta = (
                    estimated_time_of_merge
                ) = await estimated_time_to_merge.get_estimation(
                    session,
                    partition_rules,
                    train,
                    embarked_pull,
                    position - previous_queue_idx,
                    car,
                    previous_eta,
                    previous_car,
                )
                previous_car = car

                previous_queue = embarked_pull.config["name"]

                branch_partitions.partitions[train.partition_name].append(
                    PullRequestQueued(
                        number=embarked_pull.user_pull_request_number,
                        position=position,
                        priority=embarked_pull.config["priority"],
                        effective_priority=embarked_pull.config["effective_priority"],
                        queue_rule=queue_types.QueueRule(
                            name=embarked_pull.config["name"],
                            config=queue_rule.config,
                        ),
                        queued_at=embarked_pull.queued_at,
                        mergeability_check=BriefMergeabilityCheck.from_train_car(car),
                        estimated_time_of_merge=estimated_time_of_merge,
                    ),
                )

        partition_list.append(branch_partitions)

    return partition_list


@router.get(
    "/partitions/branch/{branch_name:path}",
    summary="Get all the partitions of a specific branch",
    description="Get the list of pull requests queued in each merge queue, organized by partition name",
    response_model=BranchPartitions,
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def repository_partitions_branch(
    session: database.Session,
    branch_name: security.BranchFromPath,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
) -> BranchPartitions:
    branch_partitions = BranchPartitions(branch_name=branch_name, partitions={})

    convoy = merge_train.Convoy(
        repository_ctxt,
        queue_rules,
        partition_rules,
        branch_name,
    )
    await convoy.load_from_redis()

    for rule in partition_rules:
        branch_partitions.partitions.setdefault(rule.name, [])
    if not partition_rules:
        branch_partitions.partitions.setdefault(partr_config.DEFAULT_PARTITION_NAME, [])

    for train in convoy.iter_trains():
        previous_eta = None
        for position, (embarked_pull, car) in enumerate(train._iter_embarked_pulls()):
            try:
                queue_rule = queue_rules[embarked_pull.config["name"]]
            except KeyError:
                # This car is going to be deleted so skip it
                continue

            previous_eta = (
                estimated_time_of_merge
            ) = await estimated_time_to_merge.get_estimation(
                session,
                partition_rules,
                train,
                embarked_pull,
                position,
                car,
                previous_eta,
            )
            branch_partitions.partitions[train.partition_name].append(
                PullRequestQueued(
                    number=embarked_pull.user_pull_request_number,
                    position=position,
                    priority=embarked_pull.config["priority"],
                    effective_priority=embarked_pull.config["effective_priority"],
                    queue_rule=queue_types.QueueRule(
                        name=embarked_pull.config["name"],
                        config=queue_rule.config,
                    ),
                    queued_at=embarked_pull.queued_at,
                    mergeability_check=BriefMergeabilityCheck.from_train_car(car),
                    estimated_time_of_merge=estimated_time_of_merge,
                ),
            )

    return branch_partitions


@router.get(
    "/partition/{partition_name}/branch/{branch_name:path}",
    summary="Get a partition's merge queues",
    description="Get the list of pull requests queued in each merge queue of a partition",
    response_model=Partition | None,
    responses={
        **api.default_responses,  # type: ignore
    },
)
async def repository_partition_branch(
    session: database.Session,
    partition_name: security.PartitionNameFromPath,
    branch_name: security.BranchFromPath,
    repository_ctxt: security.Repository,
    queue_rules: security.QueueRules,
    partition_rules: security.PartitionRules,
) -> Partition | None:
    if (
        partition_name != partr_config.DEFAULT_PARTITION_NAME
        and partition_name not in partition_rules
    ):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Partition `{partition_name}` does not exist",
        )

    partition = Partition(pull_requests=[])
    convoy = merge_train.Convoy(
        repository_ctxt,
        queue_rules,
        partition_rules,
        branch_name,
    )
    await convoy.load_from_redis()

    for train in convoy.iter_trains():
        if train.partition_name != partition_name:
            continue

        previous_eta = None
        previous_car = None
        for position, (embarked_pull, car) in enumerate(train._iter_embarked_pulls()):
            try:
                queue_rule = queue_rules[embarked_pull.config["name"]]
            except KeyError:
                # This car is going to be deleted so skip it
                continue

            previous_eta = (
                estimated_time_of_merge
            ) = await estimated_time_to_merge.get_estimation(
                session,
                partition_rules,
                train,
                embarked_pull,
                position,
                car,
                previous_eta,
                previous_car,
            )

            previous_car = car

            partition.pull_requests.append(
                PullRequestQueued(
                    number=embarked_pull.user_pull_request_number,
                    position=position,
                    priority=embarked_pull.config["priority"],
                    effective_priority=embarked_pull.config["effective_priority"],
                    queue_rule=queue_types.QueueRule(
                        name=embarked_pull.config["name"],
                        config=queue_rule.config,
                    ),
                    queued_at=embarked_pull.queued_at,
                    mergeability_check=BriefMergeabilityCheck.from_train_car(car),
                    estimated_time_of_merge=estimated_time_of_merge,
                ),
            )

    return partition
