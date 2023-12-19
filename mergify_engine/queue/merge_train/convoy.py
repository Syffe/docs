from __future__ import annotations

import collections
from collections import abc
import dataclasses
import typing

import daiquiri
from ddtrace import tracer
import first

from mergify_engine import check_api
from mergify_engine import context
from mergify_engine import dashboard
from mergify_engine import github_types
from mergify_engine import queue
from mergify_engine.clients import http
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import train as train_import
from mergify_engine.queue.merge_train import types as merge_train_types
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import partition_rules as partr_config
from mergify_engine.rules.config import queue_rules as qr_config


if typing.TYPE_CHECKING:
    from mergify_engine.queue import freeze
    from mergify_engine.queue.merge_train import train_car as tc_import
    from mergify_engine.rules.config import pull_request_rules as prr_config

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class Convoy:
    repository: context.Repository
    ref: github_types.GitHubRefType

    _trains: list[train_import.Train] = dataclasses.field(default_factory=list)
    _frozen_queues: dict[qr_config.QueueName, freeze.QueueFreeze | None] | None = None

    @classmethod
    async def from_context(
        cls,
        ctxt: context.Context,
    ) -> Convoy:
        convoy = cls(ctxt.repository, ctxt.pull["base"]["ref"])
        await convoy.load_from_redis()
        return convoy

    def get_queue_pull_request_partition_names_from_context(
        self,
        ctxt: context.Context,
    ) -> list[str]:
        partition_names = set()
        for train in self._trains:
            if (
                train.partition_name != partr_config.DEFAULT_PARTITION_NAME
                and train.get_car_by_tmp_pull(ctxt) is not None
            ):
                partition_names.add(train.partition_name)

        return list(partition_names)

    async def load_from_redis(
        self,
    ) -> None:
        query: collections.OrderedDict[
            str,
            partr_config.PartitionRuleName | None,
        ] = collections.OrderedDict()
        if self.repository.mergify_config["partition_rules"]:
            for part_rule in self.repository.mergify_config["partition_rules"]:
                query[
                    f"{self.repository.repo['id']}~{self.ref}~{part_rule.name}"
                ] = part_rule.name
        else:
            query[f"{self.repository.repo['id']}~{self.ref}"] = None
            query[
                f"{self.repository.repo['id']}~{self.ref}~{partr_config.DEFAULT_PARTITION_NAME}"
            ] = partr_config.DEFAULT_PARTITION_NAME

        redis_train_key = train_import.get_redis_train_key(self.repository.installation)
        raw_trains = await self.repository.installation.redis.cache.hmget(
            redis_train_key,
            query.keys(),
        )

        # NOTE(Greesb): Default partition retrocompatibility, remove once
        # all trains have been saved with the new default partition name.
        if not self.repository.mergify_config["partition_rules"]:
            # raw_trains[0] = partition_name set to None
            # raw_trains[1] = partition_name set to partr_config.DEFAULT_PARTITION_NAME
            if raw_trains[0] is not None and raw_trains[1] is not None:
                # Remove the old train key, so we don't accidentally load the old train
                # instead of the new one.
                await self.repository.installation.redis.cache.hdel(
                    redis_train_key,
                    f"{self.repository.repo['id']}~{self.ref}",
                )

            if raw_trains[0] is not None and raw_trains[1] is None:
                raw_trains.pop(1)
            else:
                # In all other cases we just want to keep the raw_trains[1].
                # If raw_trains[0] is None then we don't have anything to retrieve
                # If raw_trains[0] is not None and raw_trains[1] is not None,
                # this shouldn't happen, but if it somehow does we want to keep
                # only the `raw_trains[1]` because that is the train saved with
                # the new default partition name, so it should be the one most up to date.
                raw_trains.pop(0)

            del query[f"{self.repository.repo['id']}~{self.ref}"]

        await self.load_from_bytes(
            {
                part_name: raw_trains[idx]  # type: ignore[misc]
                for idx, part_name in enumerate(query.values())
            },
        )

    async def load_from_bytes(
        self,
        raw_trains: abc.Mapping[partr_config.PartitionRuleName, bytes | None],
    ) -> None:
        self._trains = []
        partition_names = [
            part_rule.name
            for part_rule in self.repository.mergify_config["partition_rules"]
        ]
        if not partition_names:
            partition_names = [partr_config.DEFAULT_PARTITION_NAME]

        for part_name in partition_names:
            train = train_import.Train(self, part_name)
            await train.load_from_bytes(raw_trains.get(part_name))
            self._trains.append(train)

    async def save(self) -> None:
        for train in self._trains:
            await train.save()

    def iter_trains(self) -> abc.Iterator[train_import.Train]:
        return iter(self._trains)

    def iter_trains_from_partition_names(
        self,
        partition_names: list[partr_config.PartitionRuleName],
    ) -> abc.Iterator[train_import.Train]:
        for train in self._trains:
            # If partition_names is an empty list, we return every train
            if not partition_names or train.partition_name in partition_names:
                yield train

    def get_trains_and_car_from_context_and_partition_names(
        self,
        ctxt: context.Context,
        partition_names: list[partr_config.PartitionRuleName],
    ) -> list[merge_train_types.TrainAndTrainCar]:
        trains_and_cars = []
        for train in self.iter_trains_from_partition_names(partition_names):
            car = train.get_car(ctxt)
            if car is not None:
                trains_and_cars.append(
                    merge_train_types.TrainAndTrainCar(train=train, train_car=car),
                )

        return trains_and_cars

    async def remove_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        queue_cancel_reason: queue_utils.BaseDequeueReason,
    ) -> None:
        await self.force_remove_pull(
            self.repository,
            pull_number,
            signal_trigger,
            queue_utils.BaseBranchChanged(),
            exclude_ref=self.ref,
        )
        for train in self._trains:
            await train.remove_pull(pull_number, signal_trigger, queue_cancel_reason)

    @classmethod
    async def force_remove_pull(
        cls,
        repository: context.Repository,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        dequeue_reason: queue_utils.BaseDequeueReason,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> None:
        async for convoy in cls.iter_convoys(repository):
            if exclude_ref is not None and exclude_ref == convoy.ref:
                continue
            for train in convoy.iter_trains():
                await train.remove_pull(pull_number, signal_trigger, dequeue_reason)

    async def add_pull(
        self,
        ctxt: context.Context,
        config: queue.PullQueueConfig,
        partition_names: list[partr_config.PartitionRuleName],
        signal_trigger: str,
    ) -> None:
        # Ensure the pull is not in another branch
        # (base branch of a PR can be changed when editing the title)
        await self.force_remove_pull(
            self.repository,
            ctxt.pull["number"],
            signal_trigger,
            queue_utils.BaseBranchChanged(),
            exclude_ref=ctxt.pull["base"]["ref"],
        )

        real_partition_names: list[partr_config.PartitionRuleName]
        if not partition_names:
            real_partition_names = [partr_config.DEFAULT_PARTITION_NAME]
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

    async def is_queue_frozen(self, queue_name: qr_config.QueueName) -> bool:
        return queue_name in await self.get_frozen_queue_names()

    async def get_current_queue_freeze(
        self,
        queue_name: qr_config.QueueName,
    ) -> freeze.QueueFreeze | None:
        # TODO(MRGFY-1975): Handle partition_name in queue freeze
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return queues_with_freeze.get(queue_name)

    async def get_queue_freezes_by_names(
        self,
    ) -> dict[qr_config.QueueName, freeze.QueueFreeze | None]:
        # Circular import
        from mergify_engine.queue import freeze

        # NOTE(sileht): this does not return the queue freeze associated with the queue, but
        # queue freeze that block the queue (think cascading effect)
        if self._frozen_queues is None:
            queue_freezes = {
                queue_freeze.name: queue_freeze
                async for queue_freeze in freeze.QueueFreeze.get_all(
                    self.repository,
                )
            }

            self._frozen_queues = {}

            # NOTE(sileht): queue_rules are always ordered by priority
            ongoing_freeze = None
            for queue_rule in self.repository.mergify_config["queue_rules"]:
                # If the queue is not freeze we pick the nearest queue
                new_freeze = queue_freezes.get(queue_rule.name, ongoing_freeze)
                self._frozen_queues[queue_rule.name] = new_freeze
                if new_freeze is not None and new_freeze.cascading:
                    ongoing_freeze = new_freeze

        return self._frozen_queues

    async def get_frozen_queue_names(self) -> set[qr_config.QueueName]:
        # NOTE(Syffe): When checking for where to position a newly added PR in queues,
        # all unfrozen queues with lower priorities than the highest frozen queue have
        # to be considered as non-usable to queue the newly added PR.
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return {
            name
            for name, queue_freeze in queues_with_freeze.items()
            if queue_freeze is not None
        }

    async def find_embarked_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
                    ),
                )

        return where_pull_is_embarked

    async def find_embarked_pull_with_train(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
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
                    ),
                )

        return where_pull_is_embarked

    def is_pull_embarked(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
    ) -> bool:
        for train in self._trains:
            position, _ = train.find_embarked_pull(pull_number)
            if position is not None:
                return True

        return False

    async def get_pull_summary(
        self,
        ctxt: context.Context,
        queue_rule: qr_config.QueueRule,
        pull_rule: prr_config.EvaluatedPullRequestRule | None = None,
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
        if any(
            pname == partr_config.DEFAULT_PARTITION_NAME for pname in partition_names
        ):
            raise RuntimeError(
                f"Should be impossible to have a partition name set to the default `{partr_config.DEFAULT_PARTITION_NAME}` here",
            )

        return f"#{ctxt.pull['number']} is queued in the following partitions: {', '.join(partition_names)}"

    def get_train_cars_by_tmp_pull(
        self,
        ctxt: context.Context,
    ) -> list[tc_import.TrainCar]:
        train_cars = []
        for train in self._trains:
            car = train.get_car_by_tmp_pull(ctxt)
            if car is not None:
                train_cars.append(car)

        return train_cars

    def get_train_cars_by_pull(self, ctxt: context.Context) -> list[tc_import.TrainCar]:
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
        delegating_partition: partr_config.PartitionRuleName,
    ) -> github_types.GitHubPullRequest | None:
        # FIXMEsileht): self._trains is always correctly initialized, we must drop this.
        # Reload trains instances
        await self.load_from_redis()

        pr_numbers = sorted(pr_numbers)
        for train in self._trains:
            for car in train._cars:
                if car.queue_pull_request_number is None:
                    continue

                if pr_numbers == sorted(
                    [ep.user_pull_request_number for ep in car.initial_embarked_pulls],
                ):
                    await car.add_delegating_partition(delegating_partition)
                    pull_request_context = (
                        await self.repository.get_pull_request_context(
                            car.queue_pull_request_number,
                        )
                    )
                    return pull_request_context.pull

        return None

    async def update_user_pull_request_summary(
        self,
        user_pull_request_number: github_types.GitHubPullRequestNumber,
        temporary_car: tc_import.TrainCar,
    ) -> None:
        user_pr_context = await self.repository.get_pull_request_context(
            user_pull_request_number,
        )
        train_cars = self.get_train_cars_by_pull(user_pr_context)
        if temporary_car not in train_cars:
            # Means we haven't saved the train yet or just got removed
            train_cars.append(temporary_car)

        if (
            len(train_cars) == 1
            and train_cars[0].train.partition_name
            == partr_config.DEFAULT_PARTITION_NAME
        ):
            queue_check_run_conclusion = train_cars[0].get_queue_check_run_conclusion(
                user_pull_request_number,
            )
            original_pr_summary = train_cars[0].get_original_pr_summary(
                user_pull_request_number,
            )
            report = check_api.Result(
                queue_check_run_conclusion,
                title=original_pr_summary.title,
                summary=original_pr_summary.body,
                details_url=await dashboard.get_queues_url_from_context(
                    user_pr_context,
                    self,
                ),
            )

            await check_api.set_check_run(
                user_pr_context,
                await user_pr_context.get_merge_queue_check_run_name(),
                report,
            )
        else:
            failed_car = first.first(
                tc
                for tc in train_cars
                if tc.get_queue_check_run_conclusion(
                    user_pull_request_number,
                )
                == check_api.Conclusion.CANCELLED
            )
            if failed_car is not None:
                queue_check_run_conclusion = check_api.Conclusion.CANCELLED
            else:
                failed_car = first.first(
                    tc
                    for tc in train_cars
                    if tc.get_queue_check_run_conclusion(
                        user_pull_request_number,
                    )
                    == check_api.Conclusion.FAILURE
                )
                if failed_car is not None:
                    queue_check_run_conclusion = check_api.Conclusion.FAILURE
                elif all(
                    tc.get_queue_check_run_conclusion(
                        user_pull_request_number,
                    )
                    == check_api.Conclusion.SUCCESS
                    for tc in train_cars
                ):
                    queue_check_run_conclusion = check_api.Conclusion.SUCCESS
                else:
                    queue_check_run_conclusion = check_api.Conclusion.PENDING

            if any(
                tc.train.partition_name == partr_config.DEFAULT_PARTITION_NAME
                for tc in train_cars
            ):
                raise RuntimeError(
                    f"Cannot have a `partition_name` set to the default `{partr_config.DEFAULT_PARTITION_NAME}` here",
                )

            if failed_car is not None:
                check_title = failed_car.get_original_pr_summary(
                    user_pull_request_number,
                ).title
            else:
                check_title = f"The pull request is embarked in partitions {', '.join([tc.train.partition_name for tc in train_cars])}"

            summary = "".join(
                [
                    tc.build_original_pr_summary_for_partition_report(
                        user_pull_request_number,
                    )
                    for tc in train_cars
                ],
            )

            report = check_api.Result(
                queue_check_run_conclusion,
                title=check_title,
                summary=summary,
                details_url=await dashboard.get_queues_url_from_context(
                    user_pr_context,
                    self,
                ),
            )

            await check_api.set_check_run(
                user_pr_context,
                await user_pr_context.get_merge_queue_check_run_name(),
                report,
            )

    @classmethod
    async def _get_raw_trains_by_convoy(
        cls,
        installation: context.Installation,
    ) -> dict[
        tuple[github_types.GitHubRepositoryIdType, github_types.GitHubRefType],
        dict[partr_config.PartitionRuleName, bytes],
    ]:
        trains_by_convoy: dict[
            tuple[github_types.GitHubRepositoryIdType, github_types.GitHubRefType],
            dict[partr_config.PartitionRuleName, bytes],
        ] = collections.defaultdict(dict)

        trains_key = train_import.get_redis_train_key(installation)
        trains_raw = await installation.redis.cache.hgetall(trains_key)

        # TODO(Greesb): Retrocompatibility, remove once all the trains are
        # using the new default partition name.
        if len(trains_raw.keys()) == 2:
            trains_raw_keys = sorted(trains_raw.keys())
            # Remove the old train key only if there is both the new one
            # and the old one.
            if (
                trains_raw_keys[1]
                .decode()
                .endswith(partr_config.DEFAULT_PARTITION_NAME)
                and trains_raw_keys[0].decode().count("~") == 1
            ):
                # Remove the key both from the dict and redis
                await installation.redis.cache.hdel(
                    trains_key,
                    trains_raw_keys[0].decode(),
                )
                del trains_raw[trains_raw_keys[0]]

        for key, train_raw in trains_raw.items():
            partition_name = partr_config.DEFAULT_PARTITION_NAME
            repo_id_str, ref_str = key.decode().split("~", 1)
            if "~" in ref_str:
                ref_str, part_name_str = ref_str.split("~")
                partition_name = partr_config.PartitionRuleName(part_name_str)

            ref = github_types.GitHubRefType(ref_str)
            repo_id = github_types.GitHubRepositoryIdType(int(repo_id_str))
            trains_by_convoy[(repo_id, ref)][partition_name] = train_raw

        return trains_by_convoy

    @classmethod
    @tracer.wrap("Train.refresh_convoys")
    async def refresh_convoys(cls, installation: context.Installation) -> None:
        trains_key = train_import.get_redis_train_key(installation)
        trains_by_convoy = await cls._get_raw_trains_by_convoy(installation)
        for (repo_id, ref), convoy_raw in trains_by_convoy.items():
            try:
                repository = await installation.get_repository_by_id(repo_id)
            except http.HTTPNotFound:
                LOG.warning(
                    "repository with active merge queue is unaccessible, deleting merge queue",
                    gh_owner=installation.owner_login,
                    gh_repo_id=repo_id,
                )
                if len(convoy_raw) == 1 and None in convoy_raw:
                    keys = [f"{repo_id}~{ref}"]
                else:
                    keys = [f"{repo_id}~{ref}~{part_name}" for part_name in convoy_raw]
                await installation.redis.cache.hdel(trains_key, *keys)
                continue

            try:
                try:
                    await repository.load_mergify_config()
                except context.ConfigurationFileAlreadyLoaded as e:
                    e.reraise_configuration_error()
            except mergify_conf.InvalidRules as e:  # pragma: no cover
                LOG.warning(
                    "convoy can't be refreshed, the mergify configuration is invalid",
                    gh_owner=repository.installation.owner_login,
                    gh_repo=repository.repo["name"],
                    gh_branch=ref,
                    summary=str(e),
                    annotations=e.get_annotations(e.filename),
                )
                continue

            conv = cls(repository, ref)
            await conv.load_from_bytes(convoy_raw)
            for train in conv.iter_trains():
                await train.refresh()

    @classmethod
    async def iter_convoys(
        cls,
        repository: context.Repository,
        ref: github_types.GitHubRefType | None = None,
    ) -> abc.AsyncIterator[Convoy]:
        trains_by_convoy = await cls._get_raw_trains_by_convoy(repository.installation)
        for (repo_id, convoy_ref), convoy_raw in trains_by_convoy.items():
            if repo_id != repository.repo["id"]:
                continue
            if ref is None or ref == convoy_ref:
                conv = cls(repository, convoy_ref)
                await conv.load_from_bytes(convoy_raw)
                yield conv

    async def get_queue_name_from_pull_request_number(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
    ) -> qr_config.QueueName | None:
        for embarked_pull in await self.find_embarked_pull(pr_number):
            return embarked_pull.embarked_pull.config["name"]
        return None
