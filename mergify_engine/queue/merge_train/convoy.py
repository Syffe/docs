from collections import abc
import dataclasses
import typing

from mergify_engine import check_api
from mergify_engine import constants
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import train as train_import
from mergify_engine.queue.merge_train import types as merge_train_types


if typing.TYPE_CHECKING:
    from mergify_engine.queue import freeze
    from mergify_engine.queue.merge_train import train_car as tc_import
    from mergify_engine.rules.config import partition_rules as partr_config
    from mergify_engine.rules.config import pull_request_rules as prr_config
    from mergify_engine.rules.config import queue_rules as qr_config


@dataclasses.dataclass
class Convoy:
    repository: context.Repository
    queue_rules: "qr_config.QueueRules"
    partition_rules: "partr_config.PartitionRules"
    ref: github_types.GitHubRefType

    _trains: list[train_import.Train] = dataclasses.field(default_factory=list)
    _frozen_queues: dict[
        "qr_config.QueueName", "freeze.QueueFreeze | None"
    ] | None = None

    @classmethod
    async def from_context(
        cls,
        ctxt: context.Context,
        queue_rules: "qr_config.QueueRules",
        partition_rules: "partr_config.PartitionRules",
    ) -> "Convoy":
        convoy = cls(
            ctxt.repository,
            queue_rules,
            partition_rules,
            ctxt.pull["base"]["ref"],
        )
        await convoy.load()
        return convoy

    def get_queue_pull_request_partition_names_from_context(
        self,
        ctxt: context.Context,
    ) -> list[str]:
        partition_names = set()
        for train in self._trains:
            if (
                train.partition_name is not None
                and train.get_car_by_tmp_pull(ctxt) is not None
            ):
                partition_names.add(train.partition_name)

        return list(partition_names)

    async def load(self) -> None:
        self._trains = []
        partition_names = [part_rule.name for part_rule in self.partition_rules]
        for part_name in partition_names:
            train = train_import.Train(self, part_name)
            await train.load()
            self._trains.append(train)

        if not partition_names:
            train = train_import.Train(self, None)
            await train.load()
            self._trains.append(train)

    async def save(self) -> None:
        for train in self._trains:
            await train.save()

    def iter_trains(self) -> abc.Iterator[train_import.Train]:
        return iter(self._trains)

    def iter_trains_from_partition_names(
        self, partition_names: list["partr_config.PartitionRuleName"]
    ) -> abc.Iterator[train_import.Train]:
        for train in self._trains:
            # If partition_names is an empty list, we return every train
            if not partition_names or train.partition_name in partition_names:
                yield train

    def get_trains_and_car_from_context_and_partition_names(
        self,
        ctxt: context.Context,
        partition_names: list["partr_config.PartitionRuleName"],
    ) -> list[merge_train_types.TrainAndTrainCar]:
        trains_and_cars = []
        for train in self.iter_trains_from_partition_names(partition_names):
            car = train.get_car(ctxt)
            if car is not None:
                trains_and_cars.append(
                    merge_train_types.TrainAndTrainCar(train=train, train_car=car)
                )

        return trains_and_cars

    async def remove_pull_from_trains_if_queued(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        for train in self._trains:
            if train.is_queued(pull_number):
                await train.remove_pull(pull_number, signal_trigger, unqueue_reason)

    async def remove_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        await self.force_remove_pull(
            pull_number,
            signal_trigger,
            exclude_ref=self.ref,
        )

        if isinstance(unqueue_reason, queue_utils.PrMerged):
            await self._remove_merged_head_of_trains(
                pull_number,
                signal_trigger,
                unqueue_reason,
            )
        else:
            for train in self._trains:
                await train._remove_pull(pull_number, signal_trigger, unqueue_reason)

    async def force_remove_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> None:
        async for train in train_import.Train.iter_trains(
            self.repository,
            self.queue_rules,
            self.partition_rules,
            exclude_ref=exclude_ref,
        ):
            await train._remove_pull_from_context(
                pull_number,
                signal_trigger,
                queue_utils.TargetBranchChanged(),
            )

    async def _remove_merged_head_of_trains(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.PrMerged,
    ) -> None:
        pr_queued_in = [train for train in self._trains if train.is_queued(pull_number)]
        for train in pr_queued_in:
            await train._remove_merged_head_of_train(
                pull_number,
                signal_trigger,
                unqueue_reason,
                send_eventlog_signal=False,
            )

    async def add_pull(
        self,
        ctxt: context.Context,
        config: queue.PullQueueConfig,
        partition_names: list["partr_config.PartitionRuleName"],
        signal_trigger: str,
    ) -> None:
        # Ensure the pull is not in another branch
        # (base branch of a PR can be changed when editing the title)
        await self.force_remove_pull(
            ctxt.pull["number"],
            signal_trigger,
            exclude_ref=ctxt.pull["base"]["ref"],
        )

        real_partition_names: list["partr_config.PartitionRuleName | None"]
        if not partition_names:
            real_partition_names = [None]
        else:
            real_partition_names = list(partition_names)

        for train in self._trains:
            if len(real_partition_names) == 0:
                break

            # Add the PR in existing trains
            try:
                idx_part = real_partition_names.index(train.partition_name)
            except ValueError:
                continue

            real_partition_names.pop(idx_part)
            await train.add_pull(ctxt, config, signal_trigger)

    async def is_queue_frozen(self, queue_name: "qr_config.QueueName") -> bool:
        return queue_name in await self.get_frozen_queue_names()

    async def get_current_queue_freeze(
        self,
        queue_name: "qr_config.QueueName",
    ) -> "freeze.QueueFreeze | None":
        # TODO(MRGFY-1975): Handle partition_name in queue freeze
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return queues_with_freeze.get(queue_name)

    async def get_queue_freezes_by_names(
        self,
    ) -> dict["qr_config.QueueName", "freeze.QueueFreeze | None"]:
        # Circular import
        from mergify_engine.queue import freeze

        # NOTE(sileht): this does not return the queue freeze associated with the queue, but
        # queue freeze that block the queue (think cascading effect)
        if self._frozen_queues is None:
            queue_freezes = {
                queue_freeze.name: queue_freeze
                async for queue_freeze in freeze.QueueFreeze.get_all(
                    self.repository, self.queue_rules
                )
            }

            self._frozen_queues = {}

            # NOTE(sileht): queue_rules are always ordered by priority
            ongoing_freeze = None
            for queue_rule in self.queue_rules:
                # If the queue is not freeze we pick the nearest queue
                new_freeze = queue_freezes.get(queue_rule.name, ongoing_freeze)
                self._frozen_queues[queue_rule.name] = new_freeze
                if new_freeze is not None and new_freeze.cascading:
                    ongoing_freeze = new_freeze

        return self._frozen_queues

    async def get_frozen_queue_names(self) -> set["qr_config.QueueName"]:
        # NOTE(Syffe): When checking for where to position a newly added PR in queues,
        # all unfrozen queues with lower priorities than the highest frozen queue have
        # to be considered as non-usable to queue the newly added PR.
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return {name for name, qf in queues_with_freeze.items() if qf is not None}

    async def find_embarked_pull(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[merge_train_types.ConvoyEmbarkedPullWithCarAndPos]:
        where_pull_is_embarked = []

        for train in self._trains:
            position, ep_with_car = train.find_embarked_pull(pull_number)
            if position is not None and ep_with_car is not None:
                where_pull_is_embarked.append(
                    merge_train_types.ConvoyEmbarkedPullWithCarAndPos(
                        car=ep_with_car.car,
                        embarked_pull=ep_with_car.embarked_pull,
                        partition_name=train.partition_name,
                        position=position,
                    )
                )

        return where_pull_is_embarked

    async def find_embarked_pull_with_train(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> list[merge_train_types.ConvoyEmbarkedPullWithTrain]:
        where_pull_is_embarked = []

        for train in self._trains:
            position, ep_with_car = train.find_embarked_pull(pull_number)
            if position is not None and ep_with_car is not None:
                where_pull_is_embarked.append(
                    merge_train_types.ConvoyEmbarkedPullWithTrain(
                        train=train,
                        convoy_embarked_pull=merge_train_types.ConvoyEmbarkedPullWithCarAndPos(
                            car=ep_with_car.car,
                            embarked_pull=ep_with_car.embarked_pull,
                            partition_name=train.partition_name,
                            position=position,
                        ),
                    )
                )

        return where_pull_is_embarked

    def is_pull_embarked(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> bool:
        for train in self._trains:
            position, _ = train.find_embarked_pull(pull_number)
            if position is not None:
                return True

        return False

    async def get_pull_summary(
        self,
        ctxt: context.Context,
        queue_rule: "qr_config.QueueRule",
        pull_rule: "prr_config.EvaluatedPullRequestRule | None" = None,
    ) -> str:
        embarked_pulls = await self.find_embarked_pull_with_train(ctxt.pull["number"])
        if not embarked_pulls:
            return ""

        if len(embarked_pulls) == 1:
            train = embarked_pulls[0].train
            return await train.get_pull_summary(
                ctxt,
                queue_rule,
                pull_rule,
                merge_train_types.EmbarkedPullWithCar(
                    embarked_pull=embarked_pulls[0].convoy_embarked_pull.embarked_pull,
                    car=embarked_pulls[0].convoy_embarked_pull.car,
                ),
            )

        partition_names = [ep[1].partition_name for ep in embarked_pulls]
        if any(pname is None for pname in partition_names):
            raise RuntimeError(
                "Should be impossible to have a partition name set to `None` here"
            )

        # mypy still think the partition_names can have None elements even
        # with the if above
        pull_summary = f"#{ctxt.pull['number']} is queued in the following partitions: {', '.join(partition_names)}"  # type: ignore[arg-type]
        return pull_summary

    def get_train_cars_by_tmp_pull(
        self, ctxt: context.Context
    ) -> list["tc_import.TrainCar"]:
        train_cars = []
        for train in self._trains:
            car = train.get_car_by_tmp_pull(ctxt)
            if car is not None:
                train_cars.append(car)

        return train_cars

    def get_train_cars_by_pull(
        self, ctxt: context.Context
    ) -> list["tc_import.TrainCar"]:
        train_cars = []
        for train in self._trains:
            car = train.get_car(ctxt)
            if car is not None:
                train_cars.append(car)

        return train_cars

    async def refresh_trains(self) -> None:
        for train in self._trains:
            await train.refresh()

    async def find_prs_in_train_cars_and_add_delegation(
        self,
        pr_numbers: list[github_types.GitHubPullRequestNumber],
        delegating_partition: "partr_config.PartitionRuleName",
    ) -> github_types.GitHubPullRequestNumber | None:
        # Reload trains instances
        await self.load()

        pr_numbers = sorted(pr_numbers)
        for train in self._trains:
            for car in train._cars:
                if car.queue_pull_request_number is None:
                    continue

                if pr_numbers == sorted(
                    [ep.user_pull_request_number for ep in car.initial_embarked_pulls]
                ):
                    await car.add_delegating_partition(delegating_partition)
                    return car.queue_pull_request_number

        return None

    async def update_user_pull_request_summary(
        self,
        user_pull_request_number: github_types.GitHubPullRequestNumber,
        checked_pull: github_types.GitHubPullRequestNumber,
        default_car: "tc_import.TrainCar",
    ) -> None:
        user_pr_context = await self.repository.get_pull_request_context(
            user_pull_request_number
        )
        train_cars = self.get_train_cars_by_pull(user_pr_context)
        if len(train_cars) == 0:
            # Means we haven't saved the train yet
            train_cars.append(default_car)

        if len(train_cars) == 1 and train_cars[0].train.partition_name is None:
            queue_check_run_conclusion = train_cars[0].get_queue_check_run_conclusion()
            report = check_api.Result(
                queue_check_run_conclusion,
                title=train_cars[0].get_original_pull_request_title(),
                summary=train_cars[0].get_original_pull_request_summary(checked_pull),
            )

            await check_api.set_check_run(
                user_pr_context,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                report,
            )
        else:
            if any(
                tc.get_queue_check_run_conclusion() == check_api.Conclusion.FAILURE
                for tc in train_cars
            ):
                queue_check_run_conclusion = check_api.Conclusion.FAILURE
            elif all(
                tc.get_queue_check_run_conclusion() == check_api.Conclusion.SUCCESS
                for tc in train_cars
            ):
                queue_check_run_conclusion = check_api.Conclusion.SUCCESS
            else:
                queue_check_run_conclusion = check_api.Conclusion.PENDING

            if any(tc.train.partition_name is None for tc in train_cars):
                raise RuntimeError("Cannot have a `partition_name` set to `None` here")

            # mypy doesn't understand that we already check that each partition_name
            # cannot be None with the if above
            check_title = f"The pull request is embarked in partitions {', '.join([tc.train.partition_name for tc in train_cars])}"  # type: ignore[misc]

            summary = "\n\n".join(
                [
                    f"**Partition {tc.train.partition_name}**: {tc.get_original_pull_request_title()}"
                    for tc in train_cars
                ]
            )

            report = check_api.Result(
                queue_check_run_conclusion,
                title=check_title,
                summary=summary,
            )

            await check_api.set_check_run(
                user_pr_context,
                constants.MERGE_QUEUE_SUMMARY_NAME,
                report,
            )
