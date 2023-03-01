from collections import abc
import dataclasses
import datetime
import functools
import itertools
import typing
from urllib import parse

import daiquiri
from ddtrace import tracer
import first

from mergify_engine import constants
from mergify_engine import context
from mergify_engine import date
from mergify_engine import delayed_refresh
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import queue
from mergify_engine import refresher
from mergify_engine import signals
from mergify_engine import utils
from mergify_engine import worker_pusher
from mergify_engine.clients import http
from mergify_engine.queue import freeze
from mergify_engine.queue import utils as queue_utils
from mergify_engine.queue.merge_train import embarked_pull as ep_import
from mergify_engine.queue.merge_train import train_car
from mergify_engine.queue.merge_train import train_car_state as tcs_import
from mergify_engine.queue.merge_train import types as merge_train_types
from mergify_engine.queue.merge_train import utils as train_utils
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.rules.config import queue_rules as qr_config


LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class Train:
    repository: context.Repository
    queue_rules: "qr_config.QueueRules"
    ref: github_types.GitHubRefType

    # Stored in redis
    _cars: list[train_car.TrainCar] = dataclasses.field(default_factory=list)
    _waiting_pulls: list[ep_import.EmbarkedPull] = dataclasses.field(
        default_factory=list
    )
    _current_base_sha: github_types.SHAType | None = dataclasses.field(default=None)
    _queues_with_freeze: dict[
        qr_config.QueueName, freeze.QueueFreeze | None
    ] | None = None

    class Serialized(typing.TypedDict):
        cars: list[train_car.TrainCar.Serialized]
        waiting_pulls: list[ep_import.EmbarkedPull.Serialized]
        current_base_sha: github_types.SHAType | None

    @classmethod
    async def from_context(
        cls, ctxt: context.Context, queue_rules: "qr_config.QueueRules"
    ) -> "Train":
        q = cls(ctxt.repository, queue_rules, ctxt.pull["base"]["ref"])
        await q.load()
        return q

    def _get_redis_key(self) -> str:
        return f"merge-trains~{self.repository.installation.owner_id}"

    def _get_redis_hash_key(self) -> str:
        return f"{self.repository.repo['id']}~{self.ref}"

    @classmethod
    @tracer.wrap("Train.refresh_trains", span_type="worker")
    async def refresh_trains(
        cls,
        installation: context.Installation,
    ) -> None:
        trains_key = f"merge-trains~{installation.owner_id}"
        for key in await installation.redis.cache.hkeys(trains_key):
            repo_id_str, ref_str = key.split("~")
            ref = github_types.GitHubRefType(ref_str)
            repo_id = github_types.GitHubRepositoryIdType(int(repo_id_str))
            try:
                repository = await installation.get_repository_by_id(repo_id)
            except http.HTTPNotFound:
                LOG.warning(
                    "repository with active merge queue is unaccessible, deleting merge queue",
                    gh_owner=installation.owner_login,
                    gh_repo_id=repo_id,
                )
                await installation.redis.cache.hdel(trains_key, key)
                continue

            try:
                mergify_config = await repository.get_mergify_config()
            except mergify_conf.InvalidRules as e:  # pragma: no cover
                LOG.warning(
                    "train can't be refreshed, the mergify configuration is invalid",
                    gh_owner=repository.installation.owner_login,
                    gh_repo=repository.repo["name"],
                    gh_branch=ref,
                    summary=str(e),
                    annotations=e.get_annotations(e.filename),
                )
                continue

            queue_rules = mergify_config["queue_rules"]

            train = cls(repository, queue_rules, ref)
            await train.load()
            await train.refresh()

    @classmethod
    async def iter_trains(
        cls,
        repository: context.Repository,
        queue_rules: "qr_config.QueueRules",
        *,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> abc.AsyncIterator["Train"]:
        repo_filter: (github_types.GitHubRepositoryIdType | typing.Literal["*"]) = "*"
        if repository is not None:
            repo_filter = repository.repo["id"]

        async for key, train_raw in repository.installation.redis.cache.hscan_iter(
            f"merge-trains~{repository.installation.owner_id}",
            f"{repo_filter}~*",
            count=10000,
        ):
            repo_id_str, ref_str = key.split("~")
            ref = github_types.GitHubRefType(ref_str)
            if exclude_ref is not None and ref == exclude_ref:
                continue

            train = cls(repository, queue_rules, ref)
            await train.load(train_raw)
            yield train

    async def load(self, train_raw: str | None = None) -> None:
        if train_raw is None:
            train_raw = await self.repository.installation.redis.cache.hget(
                self._get_redis_key(), self._get_redis_hash_key()
            )

        if train_raw:
            train = typing.cast(Train.Serialized, json.loads(train_raw))
            self._waiting_pulls = [
                ep_import.EmbarkedPull.deserialize(self, wp)
                for wp in train["waiting_pulls"]
            ]
            self._current_base_sha = train["current_base_sha"]
            self._cars = [
                train_car.TrainCar.deserialize(self, c) for c in train["cars"]
            ]
        else:
            self._cars = []
            self._waiting_pulls = []
            self._current_base_sha = None

        self._queues_with_freeze = None

    @property
    def log_queue_extras(self) -> dict[str, typing.Any]:
        return {
            "train_cars": [
                [ep.user_pull_request_number for ep in c.still_queued_embarked_pulls]
                for c in self._cars
            ],
            "train_waiting_pulls": [
                wp.user_pull_request_number for wp in self._waiting_pulls
            ],
        }

    @functools.cached_property
    def log(self) -> daiquiri.KeywordArgumentAdapter:
        return daiquiri.getLogger(
            __name__,
            gh_owner=self.repository.installation.owner_login,
            gh_repo=self.repository.repo["name"],
            gh_branch=self.ref,
            **self.log_queue_extras,
        )

    async def save(self) -> None:
        if self._waiting_pulls or self._cars:
            prepared = self.Serialized(
                waiting_pulls=[ep.serialized() for ep in self._waiting_pulls],
                current_base_sha=self._current_base_sha,
                cars=[c.serialized() for c in self._cars],
            )
            raw = json.dumps(prepared)
            await self.repository.installation.redis.cache.hset(
                self._get_redis_key(), self._get_redis_hash_key(), raw
            )
        else:
            await self.repository.installation.redis.cache.hdel(
                self._get_redis_key(), self._get_redis_hash_key()
            )

    def get_car(self, ctxt: context.Context) -> train_car.TrainCar | None:
        return first.first(
            self._cars,
            key=lambda car: ctxt.pull["number"]
            in [ep.user_pull_request_number for ep in car.still_queued_embarked_pulls],
        )

    def get_car_by_tmp_pull(self, ctxt: context.Context) -> train_car.TrainCar | None:
        return first.first(
            self._cars,
            key=lambda car: car.queue_pull_request_number == ctxt.pull["number"],
        )

    async def refresh(self) -> None:
        # NOTE(sileht): workaround for cleaning unwanted PRs queued by this bug:
        # https://github.com/Mergifyio/mergify-engine/pull/2958
        await self._remove_duplicate_pulls()
        await self._sync_configuration_change()
        await self._split_failed_batches()
        try:
            await self._populate_cars()
        except train_utils.BaseBranchVanished:
            self.log.warning("target branch vanished, deleting merge queue.")
            for embarked_pull, _ in list(self._iter_embarked_pulls()):
                await self._remove_pull(
                    embarked_pull.user_pull_request_number,
                    "merge queue internal",
                    queue_utils.TargetBranchMissing(self.ref),
                )
        await self.save()

    async def _remove_duplicate_pulls(self) -> None:
        known_prs = set()
        for i, car in enumerate(self._cars):
            for embarked_pull in car.still_queued_embarked_pulls:
                if embarked_pull.user_pull_request_number in known_prs:
                    await self._slice_cars(
                        i,
                        reason=queue_utils.PrQueuedTwice(),
                    )
                    break
                else:
                    known_prs.add(embarked_pull.user_pull_request_number)
            else:
                continue
            break

        wp_to_keep = []
        for wp in self._waiting_pulls:
            if wp.user_pull_request_number not in known_prs:
                known_prs.add(wp.user_pull_request_number)
                wp_to_keep.append(wp)
        self._waiting_pulls = wp_to_keep

    async def _sync_configuration_change(self) -> None:
        for i, (embarked_pull, _) in enumerate(list(self._iter_embarked_pulls())):
            queue_rule = self.queue_rules.get(embarked_pull.config["name"])
            if queue_rule is None:
                # NOTE(sileht): We just slice the cars list here, so when the
                # car will be recreated if the rule doesn't exists anymore, the
                # failure will be reported properly
                await self._slice_cars(
                    i,
                    reason=queue_utils.QueueRuleMissing(),
                )
                return None

    async def reset(self, unexpected_change: train_car.UnexpectedChange) -> None:
        await self._slice_cars(
            0, reason=queue_utils.UnexpectedQueueChange(change=str(unexpected_change))
        )
        await self.save()
        self.log.info("train cars reset")

    async def _slice_cars(
        self,
        new_queue_size: int,
        reason: queue_utils.BaseUnqueueReason,
        drop_pull_requests: dict[
            github_types.GitHubPullRequestNumber, queue_utils.BaseUnqueueReason
        ]
        | None = None,
    ) -> None:
        if drop_pull_requests is None:
            drop_pull_requests = {}

        sliced = False
        new_cars: list[train_car.TrainCar] = []
        new_waiting_pulls: list[ep_import.EmbarkedPull] = []
        for c in self._cars:
            new_queue_size -= len(c.still_queued_embarked_pulls)
            if new_queue_size >= 0:
                new_cars.append(c)
            else:
                sliced = True
                new_waiting_pulls.extend(c.still_queued_embarked_pulls)
                for ep in c.still_queued_embarked_pulls:
                    signal_reason = drop_pull_requests.get(
                        ep.user_pull_request_number, reason
                    )
                    await c.send_checks_end_signal(
                        ep.user_pull_request_number,
                        signal_reason,
                        "DEFINITIVE"
                        if ep.user_pull_request_number in drop_pull_requests
                        else "REEMBARKED",
                    )
                await c.end_checking(
                    reason, not_reembarked_pull_requests=drop_pull_requests
                )

        if sliced:
            self.log.info(
                "queue has been sliced",
                new_queue_size=new_queue_size,
                reason=str(reason),
            )

        self._cars = new_cars
        self._waiting_pulls = [
            ep
            for ep in new_waiting_pulls + self._waiting_pulls
            if ep.user_pull_request_number not in drop_pull_requests
        ]

    def find_embarked_pull(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> (
        tuple[int, merge_train_types.EmbarkedPullWithCar]
        | tuple[typing.Literal[None], typing.Literal[None]]
    ):
        for position, embarked_pull_with_car in enumerate(self._iter_embarked_pulls()):
            if (
                embarked_pull_with_car.embarked_pull.user_pull_request_number
                == pull_number
            ):
                return position, embarked_pull_with_car
        return None, None

    @staticmethod
    def _waiting_pulls_sorter(
        pull: ep_import.EmbarkedPull,
    ) -> tuple[int, datetime.datetime]:
        return (
            pull.config["effective_priority"] * -1,
            pull.queued_at,
        )

    def _get_waiting_pulls_ordered_by_priority(
        self,
        ignored_queues: set[str] | frozenset[str] = frozenset(),
    ) -> tuple[list[ep_import.EmbarkedPull], list[ep_import.EmbarkedPull]]:
        ignored_pulls = []
        waiting_pulls = []
        for embarked_pull in self._waiting_pulls:
            if embarked_pull.config["name"] in ignored_queues:
                ignored_pulls.append(embarked_pull)
            else:
                waiting_pulls.append(embarked_pull)
        return (
            sorted(
                waiting_pulls,
                key=self._waiting_pulls_sorter,
            ),
            ignored_pulls,
        )

    def _iter_embarked_pulls(
        self,
        ignored_queues: set[str] | frozenset[str] = frozenset(),
    ) -> abc.Iterator[merge_train_types.EmbarkedPullWithCar]:
        for car in self._cars:
            for embarked_pull in car.still_queued_embarked_pulls:
                yield merge_train_types.EmbarkedPullWithCar(embarked_pull, car)

        (
            waiting_pulls_ordered_by_priority,
            _,
        ) = self._get_waiting_pulls_ordered_by_priority(ignored_queues=ignored_queues)
        for embarked_pull in waiting_pulls_ordered_by_priority:
            # NOTE(sileht): NamedTuple doesn't support multiple inheritance
            # the Protocol can't be inherited
            yield merge_train_types.EmbarkedPullWithCar(embarked_pull, None)

    async def is_queue_frozen(self, queue_name: qr_config.QueueName) -> bool:
        return queue_name in await self.get_frozen_queue_names()

    async def get_queue_freezes_by_names(
        self,
    ) -> dict[qr_config.QueueName, freeze.QueueFreeze | None]:
        # NOTE(sileht): this does not return the queue freeze associated with the queue, but
        # queue freeze that block the queue (think cascading effect)
        if self._queues_with_freeze is None:
            queue_freezes = {
                queue_freeze.name: queue_freeze
                async for queue_freeze in freeze.QueueFreeze.get_all(
                    self.repository, self.queue_rules
                )
            }

            self._queues_with_freeze = {}

            # NOTE(sileht): queue_rules are always ordered by priority
            ongoing_freeze = None
            for queue_rule in self.queue_rules:
                # If the queue is not freeze we pick the nearest queue
                new_freeze = queue_freezes.get(queue_rule.name, ongoing_freeze)
                self._queues_with_freeze[queue_rule.name] = new_freeze
                if new_freeze is not None and new_freeze.cascading:
                    ongoing_freeze = new_freeze

        return self._queues_with_freeze

    async def get_current_queue_freeze(
        self, current_queue_name: qr_config.QueueName
    ) -> freeze.QueueFreeze | None:
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return queues_with_freeze.get(current_queue_name)

    async def get_frozen_queue_names(self) -> set[qr_config.QueueName]:
        # NOTE(Syffe): When checking for where to position a newly added PR in queues,
        # all unfrozen queues with lower priorities than the highest frozen queue have
        # to be considered as non-usable to queue the newly added PR.
        queues_with_freeze = await self.get_queue_freezes_by_names()
        return {name for name, qf in queues_with_freeze.items() if qf is not None}

    async def add_pull(
        self,
        ctxt: context.Context,
        config: queue.PullQueueConfig,
        signal_trigger: str,
    ) -> None:
        # NOTE(sileht): first, ensure the pull is not in another branch
        await self.force_remove_pull(
            self.repository,
            self.queue_rules,
            ctxt.pull["number"],
            signal_trigger,
            exclude_ref=ctxt.pull["base"]["ref"],
        )

        # NOTE(charly): ensure there is no configuration change since the train
        # creation
        for embarked_pull, _ in list(self._iter_embarked_pulls()):
            queue_rule = self.queue_rules.get(embarked_pull.config["name"])
            if queue_rule is None:
                await self._remove_pull(
                    embarked_pull.user_pull_request_number,
                    signal_trigger,
                    queue_utils.QueueRuleMissing(),
                )
        new_pull_queue_rule = self.queue_rules[config["name"]]

        best_position = -1
        need_to_be_readded = False
        frozen_queues = await self.get_frozen_queue_names()

        for position, (embarked_pull, car) in enumerate(self._iter_embarked_pulls()):
            embarked_pull_queue_rule = self.queue_rules[embarked_pull.config["name"]]
            car_can_be_interrupted = car is None or (
                (
                    car.can_be_interrupted()
                    or (
                        embarked_pull.config["name"] != config["name"]
                        # NOTE(Syffe): If we don't consider unfrozen queues with lower priority
                        # than the highest frozen queue, this condition will be false,
                        # and so car_can_be_interrupted will also be. In that case, adding
                        # an urgent PR in the urgent queue will be impossible since embarked pull
                        # in lower priority queues that are not frozen will not validate this condition
                        # and thus, the best_position of the urgent PR will still be -1
                        # See:
                        and embarked_pull.config["name"] in frozen_queues
                    )
                )
                and new_pull_queue_rule.config["priority"]
                >= embarked_pull_queue_rule.config["priority"]
                and config["name"]
                not in embarked_pull_queue_rule.config[
                    "disallow_checks_interruption_from_queues"
                ]
            )

            if embarked_pull.user_pull_request_number == ctxt.pull["number"]:
                if (
                    config["effective_priority"]
                    != embarked_pull.config["effective_priority"]
                    or config["name"] != embarked_pull.config["name"]
                ) and car_can_be_interrupted:
                    ctxt.log.info(
                        "pull request already in train but misplaced",
                        config=config,
                        **self.log_queue_extras,
                    )
                    need_to_be_readded = True
                    break

                # already in queue at right place, we are good
                ctxt.log.info(
                    "pull request already in train",
                    config=config,
                    **self.log_queue_extras,
                )
                return

            if (
                best_position == -1
                and config["effective_priority"]
                > embarked_pull.config["effective_priority"]
                and car_can_be_interrupted
            ):
                # We found a car with lower priority
                best_position = position

        if need_to_be_readded:
            # FIXME(sileht): this can be optimised by not dropping spec checks,
            # if the position in the queue does not change
            await self._remove_pull_from_context(
                ctxt.pull["number"],
                signal_trigger,
                queue_utils.PrWithHigherPriorityQueued(ctxt.pull["number"]),
            )
            await self.add_pull(ctxt, config, signal_trigger)
            return

        new_embarked_pull = ep_import.EmbarkedPull(
            self, ctxt.pull["number"], config, date.utcnow()
        )
        self._waiting_pulls.append(new_embarked_pull)

        if best_position != -1:
            await self._slice_cars(
                best_position,
                reason=queue_utils.PrWithHigherPriorityQueued(
                    pr_number=ctxt.pull["number"]
                ),
            )

        await self.save()

        final_position, _ = self.find_embarked_pull(ctxt.pull["number"])
        if final_position is None:
            raise RuntimeError(
                "Could not find the pull request we just added in the queue"
            )

        ctxt.log.info(
            "pull request added to train",
            gh_pull=ctxt.pull["number"],
            position=final_position,
            queue_name=config["name"],
            **self.log_queue_extras,
        )
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.queue.enter",
            signals.EventQueueEnterMetadata(
                {
                    "queue_name": new_embarked_pull.config["name"],
                    "branch": self.ref,
                    "position": final_position,
                    "queued_at": new_embarked_pull.queued_at,
                }
            ),
            signal_trigger,
        )
        # Refresh summary of all pull requests
        await self.refresh_pulls(
            source=f"pull {ctxt.pull['number']} added to queue",
        )

    async def remove_pull(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        # NOTE(sileht): Remove the pull request from all trains, just in case
        # the base branch change in the meantime
        await self.force_remove_pull(
            self.repository,
            self.queue_rules,
            pull_number,
            signal_trigger,
            exclude_ref=self.ref,
        )
        await self._remove_pull_from_context(
            pull_number, signal_trigger, unqueue_reason
        )

    async def _remove_pull_from_context(
        self,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        if not self.is_queued(pull_number):
            self.log.info(
                "already absent from train",
                gh_pull=pull_number,
                gh_branch=self.ref,
                **self.log_queue_extras,
            )
            return
        if isinstance(unqueue_reason, queue_utils.PrMerged):
            await self._remove_merged_head_of_train(
                pull_number, signal_trigger, unqueue_reason
            )
        else:
            await self._remove_pull(pull_number, signal_trigger, unqueue_reason)

    async def _remove_merged_head_of_train(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.PrMerged,
    ) -> None:
        embarked_pull = self._cars[0].still_queued_embarked_pulls[0]
        if embarked_pull.user_pull_request_number != pr_number:
            raise RuntimeError("The head of train is not the expected pull_request")

        # Need to create the event here because the `self._cars[0]` might get deleted in the `if` below
        event_metadata = signals.EventQueueLeaveMetadata(
            {
                "reason": str(unqueue_reason),
                "merged": True,
                "queue_name": embarked_pull.config["name"],
                "branch": self.ref,
                "position": 0,
                "queued_at": embarked_pull.queued_at,
                "seconds_waiting_for_schedule": self._cars[
                    0
                ].train_car_state.seconds_waiting_for_schedule,
                "seconds_waiting_for_freeze": self._cars[
                    0
                ].train_car_state.seconds_waiting_for_freeze,
            }
        )
        # Head of the train was merged and the base_sha haven't changed, we can keep
        # other running cars
        await self._cars[0].send_checks_end_signal(
            self._cars[0].still_queued_embarked_pulls[0].user_pull_request_number,
            unqueue_reason,
            "DEFINITIVE",
        )
        del self._cars[0].still_queued_embarked_pulls[0]
        if len(self._cars[0].still_queued_embarked_pulls) == 0:
            deleted_car = self._cars[0]
            await deleted_car.end_checking(reason=None, not_reembarked_pull_requests={})
            self._cars = self._cars[1:]

        self._current_base_sha = unqueue_reason.sha

        await self.save()
        self.log.info(
            "removed from head train",
            position=0,
            gh_pull=pr_number,
            gh_branch=self.ref,
            **self.log_queue_extras,
        )
        await self.refresh_pulls(
            source=f"merged pull {pr_number} removed from queue",
            additional_pull_request=pr_number,
            # process quickly the next one,
            priority_first_pull_request=worker_pusher.Priority.immediate,
        )
        await signals.send(
            self.repository,
            pr_number,
            "action.queue.leave",
            event_metadata,
            signal_trigger,
        )

    async def _remove_pull(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        unqueue_reason: queue_utils.BaseUnqueueReason,
    ) -> None:
        position, embarked_pull_with_car = self.find_embarked_pull(pr_number)
        if position is None or embarked_pull_with_car is None:
            return

        other_prs_reason: queue_utils.BaseUnqueueReason
        if isinstance(unqueue_reason, queue_utils.UnexpectedQueueChange):
            other_prs_reason = unqueue_reason
        else:
            other_prs_reason = queue_utils.PrAheadDequeued(pr_number=pr_number)

        await self._slice_cars(
            position,
            reason=other_prs_reason,
            drop_pull_requests={pr_number: unqueue_reason},
        )
        event_metadata = signals.EventQueueLeaveMetadata(
            {
                "reason": str(unqueue_reason),
                "merged": False,
                "queue_name": embarked_pull_with_car.embarked_pull.config["name"],
                "branch": self.ref,
                "position": position,
                "queued_at": embarked_pull_with_car.embarked_pull.queued_at,
            }
        )
        if embarked_pull_with_car.car is not None:
            event_metadata.update(
                {
                    "seconds_waiting_for_schedule": embarked_pull_with_car.car.train_car_state.seconds_waiting_for_schedule,
                    "seconds_waiting_for_freeze": embarked_pull_with_car.car.train_car_state.seconds_waiting_for_freeze,
                }
            )

        await self.save()
        await signals.send(
            self.repository,
            pr_number,
            "action.queue.leave",
            event_metadata,
            signal_trigger,
        )

        self.log.info(
            "removed from train",
            position=position,
            gh_pull=pr_number,
            gh_branch=self.ref,
            **self.log_queue_extras,
        )
        await self.refresh_pulls(
            source=f"pull {pr_number} removed from queue",
            additional_pull_request=pr_number,
        )

    async def _split_failed_train_car(self, car: train_car.TrainCar) -> None:
        current_queue_position = sum(
            len(c.still_queued_embarked_pulls)
            for c in itertools.takewhile(lambda c: c is not car, self._cars)
        ) + len(car.still_queued_embarked_pulls)
        self.log.info("spliting failed car", position=current_queue_position, car=car)

        queue_name = car.get_queue_name()
        try:
            queue_rule = self.queue_rules[queue_name]
        except KeyError:
            # We just need to wait the pull request has been removed from
            # the queue by the action
            self.log.info(
                "cant split failed batch train_car.TrainCar, queue rule does not exist anymore",
                queue_rules=self.queue_rules,
                queue_name=queue_name,
            )
            return

        # NOTE(sileht): This batch failed, we can drop everything else
        # after has we known now they will not work, and split this one
        # in two
        await self._slice_cars(
            current_queue_position, reason=queue_utils.PrAheadFailedToMerge()
        )

        # We move this car later at the end to not retest it
        del self._cars[-1]

        # NOTE(sileht): if speculative_checks == 1 we split the batch
        # in two parts, but check only the first one
        parts = max(2, queue_rule.config["speculative_checks"])

        parents: list[ep_import.EmbarkedPull] = []
        for pos, pulls in enumerate(
            utils.split_list(car.still_queued_embarked_pulls[:-1], parts)
        ):
            self._cars.append(
                train_car.TrainCar(
                    train=self,
                    train_car_state=tcs_import.TrainCarState(),
                    initial_embarked_pulls=pulls,
                    still_queued_embarked_pulls=pulls.copy(),
                    parent_pull_request_numbers=car.parent_pull_request_numbers
                    + [ep.user_pull_request_number for ep in parents],
                    initial_current_base_sha=car.initial_current_base_sha,
                    failure_history=car.failure_history + [car],
                )
            )

            parents += pulls
            # NOTE(sileht): if speculative_checks == 1 we must check
            # only the first car, keep the second one as pending.
            # _populate_cars() will create the second one, when the
            # first car has finished and passed
            if queue_rule.config["speculative_checks"] > 1 or pos == 0:
                try:
                    previous_car = self._cars[-2]
                except IndexError:
                    previous_car = None
                try:
                    await self._start_checking_car(
                        self._cars[-1],
                        previous_car,
                    )
                except (
                    train_car.TrainCarPullRequestCreationPostponed,
                    train_car.TrainCarPullRequestCreationFailure,
                ):
                    self.log.info(
                        "failed to create draft pull request",
                        car=car,
                        exc_info=True,
                    )

        # Update the car to pull that was part of the batch into parent, but keep
        # the result as we already test it.
        car.parent_pull_request_numbers = car.parent_pull_request_numbers + [
            ep.user_pull_request_number for ep in parents
        ]
        car.still_queued_embarked_pulls = [car.still_queued_embarked_pulls[-1]]
        car.initial_embarked_pulls = car.still_queued_embarked_pulls.copy()
        self._cars.append(car)

        # Refresh summary of others
        await self.refresh_pulls(source="batch got split")

    async def _split_failed_batches(self) -> None:
        if (
            len(self._cars) == 1
            and self._cars[0].train_car_state.outcome
            == train_car.TrainCarOutcome.CHECKS_FAILED
            and len(self._cars[0].initial_embarked_pulls) == 1
        ):
            # we refresh the state, to set the final conclusion
            await self._cars[0].check_mergeability(
                origin="batch_split",
                # NOTE(sileht): We should pass the original pull request rule
                # in case of inplace checks, but since the outcome is
                # train_car.TrainCarOutcome.CHECKS_FAILED, it's not a bug deal.
                original_pull_request_rule=None,
                original_pull_request_number=None,
            )
            return

        # NOTE(sileht): Looks for batch failure and split if needed
        first_failed_batch_train_car = first.first(
            car
            for car in self._cars
            if (
                car.train_car_state.outcome == train_car.TrainCarOutcome.CHECKS_FAILED
                and car.has_previous_car_status_succeeded()
                and len(car.initial_embarked_pulls) > 1
            )
        )
        if first_failed_batch_train_car is not None:
            await self._split_failed_train_car(first_failed_batch_train_car)

        # NOTE(sileht): speculative_checks=1 may create car without the
        # attached draft pull request if this car become the first, it means
        # the previous car has been merged and we can start testing it
        if (
            self._cars
            and len(self._cars[0].failure_history) > 0
            and self._cars[0].train_car_state.checks_type is None
        ):
            queue_name = self._cars[0].get_queue_name()
            try:
                self.queue_rules[queue_name]
            except KeyError:
                # We just need to wait the pull request has been removed from
                # the queue by the action
                self.log.info(
                    "can't start testing second half of a failed batch train_car.TrainCar, queue rule does not exist anymore",
                    queue_rules=self.queue_rules,
                    queue_name=queue_name,
                )
                return

            try:
                await self._start_checking_car(self._cars[0], None)
            except train_car.TrainCarPullRequestCreationPostponed:
                return
            except train_car.TrainCarPullRequestCreationFailure:
                # NOTE(sileht): We posted failure merge queue check-run on
                # car.user_pull_request_number and refreshed it, so it will be removed
                # from the train soon. We don't need to create remaining cars now.
                # When this car will be removed the remaining one will be created
                return

    async def _slice_frozen_cars(self, frozen_queues: set[str]) -> None:
        for i, car in enumerate(self._cars):
            for embarked_pull in car.still_queued_embarked_pulls:
                if embarked_pull.config["name"] in frozen_queues:
                    await self._slice_cars(
                        i,
                        reason=queue_utils.PrFrozenNoCascading(),
                    )
                    return

    async def _populate_cars(self) -> None:
        if self._cars and (
            self._cars[-1].train_car_state.checks_type
            == train_car.TrainCarChecksType.FAILED
            or self._cars[-1].train_car_state.outcome
            not in (
                train_car.TrainCarOutcome.MERGEABLE,
                train_car.TrainCarOutcome.UNKNOWN,
            )
        ):
            # We are searching the responsible of a failure don't touch anything
            return

        non_cascading_queue_freeze_filter = {
            queue_freeze.name
            async for queue_freeze in freeze.QueueFreeze.get_all_non_cascading(
                self.repository, self.queue_rules
            )
        }

        try:
            head = next(
                self._iter_embarked_pulls(
                    ignored_queues=non_cascading_queue_freeze_filter
                )
            ).embarked_pull
        except StopIteration:
            return

        if self._current_base_sha is None or not self._cars:
            self._current_base_sha = await self.get_base_sha()

        if non_cascading_queue_freeze_filter and self._cars:
            await self._slice_frozen_cars(
                frozen_queues=non_cascading_queue_freeze_filter
            )

        try:
            queue_rule = self.queue_rules[head.config["name"]]
        except KeyError:
            # We just need to wait the pull request has been removed from
            # the queue by the action
            self.log.info(
                "cant populate cars, queue rule does not exist",
                queue_rules=self.queue_rules,
                queue_name=head.config["name"],
            )
            car = train_car.TrainCar(
                self,
                tcs_import.TrainCarState(),
                [head],
                [head],
                [],
                self._current_base_sha,
            )
            await car._set_creation_failure(
                f"queue named `{head.config['name']}` does not exist anymore",
            )
            return

        speculative_checks = queue_rule.config["speculative_checks"]
        missing_cars = speculative_checks - len(self._cars)

        if missing_cars < 0:
            # Too many cars
            new_queue_size = sum(
                [
                    len(car.still_queued_embarked_pulls)
                    for car in self._cars[:speculative_checks]
                ]
            )
            await self._slice_cars(
                new_queue_size,
                reason=queue_utils.SpeculativeCheckNumberReduced(),
            )

        elif missing_cars > 0 and self._waiting_pulls:
            # Not enough cars
            for _ in range(missing_cars):
                (
                    waiting_pulls_ordered_by_priority,
                    frozen_pulls,
                ) = self._get_waiting_pulls_ordered_by_priority(
                    ignored_queues=non_cascading_queue_freeze_filter
                )

                pulls_to_check, remaining_pulls = self._get_next_batch(
                    waiting_pulls_ordered_by_priority,
                    head.config["name"],
                    queue_rule.config["batch_size"],
                )

                if frozen_pulls:
                    remaining_pulls += frozen_pulls

                if not pulls_to_check:
                    return

                enough_to_batch = len(pulls_to_check) == queue_rule.config["batch_size"]
                wait_enough_time_to_batch = (
                    date.utcnow() - pulls_to_check[0].queued_at
                    >= queue_rule.config["batch_max_wait_time"]
                )
                if not enough_to_batch and not wait_enough_time_to_batch:
                    await delayed_refresh.plan_refresh_at_least_at(
                        self.repository,
                        pulls_to_check[0].user_pull_request_number,
                        pulls_to_check[0].queued_at
                        + queue_rule.config["batch_max_wait_time"],
                    )

                    return

                self._waiting_pulls = remaining_pulls

                # NOTE(sileht): still_queued_embarked_pulls is always in sync with self._current_base_sha.
                # A train_car.TrainCar can be partially deleted and the next car may looks wierd as some parent PRs
                # may look missing but because the current_base_sha as moved too, this is safe.
                parent_pull_request_numbers = [
                    ep.user_pull_request_number
                    for ep in itertools.chain.from_iterable(
                        [car.still_queued_embarked_pulls for car in self._cars]
                    )
                ]

                car = train_car.TrainCar(
                    self,
                    tcs_import.TrainCarState(),
                    pulls_to_check,
                    pulls_to_check.copy(),
                    parent_pull_request_numbers,
                    self._current_base_sha,
                )

                if self._cars:
                    previous_car = self._cars[-1]
                else:
                    previous_car = None

                self._cars.append(car)

                try:
                    await self._start_checking_car(car, previous_car)
                except train_car.TrainCarPullRequestCreationPostponed:
                    return
                except train_car.TrainCarPullRequestCreationFailure:
                    # NOTE(sileht): We posted failure merge queue check-run on
                    # car.user_pull_request_number and refreshed it, so it will be removed
                    # from the train soon. We don't need to create remaining cars now.
                    # When this car will be removed the remaining one will be created
                    return

    async def _start_checking_car(
        self,
        car: train_car.TrainCar,
        previous_car: "train_car.TrainCar | None",
    ) -> None:
        queue_rule = car.get_queue_rule()
        can_be_updated = (
            self._cars[0] == car
            and len(car.still_queued_embarked_pulls) == 1
            and len(car.parent_pull_request_numbers) == 0
        )
        if can_be_updated and queue_rule.config["allow_inplace_checks"]:
            # smart mode
            if (
                queue_rule.config["speculative_checks"] == 1
                and queue_rule.config["batch_size"] == 1
            ):
                do_inplace_checks = True
            else:
                do_inplace_checks = not await car.is_behind()
        else:
            do_inplace_checks = False

        try:
            # get_next_batch() ensure all embarked_pulls has same config
            if do_inplace_checks:
                # No need to create a pull request
                await car.start_checking_inplace()
            else:
                await car.start_checking_with_draft(previous_car)

        except train_car.TrainCarPullRequestCreationPostponed:
            # NOTE(sileht): We can't create the tmp pull request, we will
            # retry later. In worse case, that will be retried until the pull
            # request become the first one in queue
            del self._cars[-1]
            self._waiting_pulls.extend(car.still_queued_embarked_pulls)
            raise

    async def get_base_sha(self) -> github_types.SHAType:
        escaped_branch_name = parse.quote(self.ref, safe="")
        try:
            branch = typing.cast(
                github_types.GitHubBranch,
                await self.repository.installation.client.item(
                    f"repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/branches/{escaped_branch_name}"
                ),
            )
        except http.HTTPNotFound:
            raise train_utils.BaseBranchVanished(self.ref)
        return branch["commit"]["sha"]

    async def is_synced_with_the_base_branch(
        self, base_sha: github_types.SHAType
    ) -> bool:
        if not self._cars:
            return True

        if base_sha == self._current_base_sha:
            return True

        if not self._cars:
            # NOTE(sileht): the PR that call this method will be deleted soon
            return False

        # Base branch just moved but the last merged PR is the one we have on top on our
        # train, we just not yet received the event that have called Train.remove_pull()
        # NOTE(sileht): I wonder if it's robust enough, these cases should be enough to
        # catch everything I have in mind
        # * We run it when we remove the top car
        # * We run it when a tmp PR is refreshed
        # * We run it on each push events
        # * We run it before merge
        pull: github_types.GitHubPullRequest = await self.repository.installation.client.item(
            f"{self.repository.base_url}/pulls/{self._cars[0].still_queued_embarked_pulls[0].user_pull_request_number}"
        )
        return pull["merged"] and base_sha == pull["merge_commit_sha"]

    async def get_config(
        self, pull_number: github_types.GitHubPullRequestNumber
    ) -> queue.PullQueueConfig:
        _, item = self.find_embarked_pull(pull_number)
        if item is not None:
            return item.embarked_pull.config

        raise RuntimeError("get_config on unknown pull request")

    def is_queued(self, pull: github_types.GitHubPullRequestNumber) -> bool:
        return any(
            item.embarked_pull.user_pull_request_number == pull
            for item in self._iter_embarked_pulls()
        )

    async def get_pulls(self) -> list[github_types.GitHubPullRequestNumber]:
        return [
            item.embarked_pull.user_pull_request_number
            for item in self._iter_embarked_pulls()
        ]

    async def is_first_pull(self, ctxt: context.Context) -> bool:
        item = first.first(self._iter_embarked_pulls())
        return (
            item is not None
            and item.embarked_pull.user_pull_request_number == ctxt.pull["number"]
        )

    @staticmethod
    def _get_next_batch(
        pulls: list[ep_import.EmbarkedPull], queue_name: str, batch_size: int = 1
    ) -> tuple[list[ep_import.EmbarkedPull], list[ep_import.EmbarkedPull]]:
        if not pulls:
            return [], []

        for _i, pull in enumerate(pulls[:batch_size]):
            if pull.config["name"] != queue_name:
                # The queue change, wait first queue to be empty before processing
                # the next queue
                break
        else:
            _i += 1
        return pulls[:_i], pulls[_i:]

    @classmethod
    async def force_remove_pull(
        cls,
        repository: context.Repository,
        queue_rules: qr_config.QueueRules,
        pull_number: github_types.GitHubPullRequestNumber,
        signal_trigger: str,
        *,
        exclude_ref: github_types.GitHubRefType | None = None,
    ) -> None:
        async for train in cls.iter_trains(
            repository,
            queue_rules,
            exclude_ref=exclude_ref,
        ):
            await train._remove_pull_from_context(
                pull_number,
                signal_trigger,
                queue_utils.TargetBranchChanged(),
            )

    async def generate_merge_queue_summary_footer(
        self,
        queue_rule_report: merge_train_types.QueueRuleReport,
        *,
        pull_rule: "prr_config.EvaluatedPullRequestRule | None" = None,
        show_queue: bool = True,
        for_queue_pull_request: bool = False,
    ) -> str:
        description = f"\n\n**Required conditions of queue** `{queue_rule_report.name}` **for merge:**\n\n"
        description += queue_rule_report.summary

        if pull_rule and not pull_rule.conditions.match:
            description += "\n\n**Required conditions to stay in the queue:**\n\n"
            description += pull_rule.conditions.get_unmatched_summary()

        if show_queue:
            table = [
                "| | Pull request | Queue/Priority | Speculative checks | Queued",
                "| ---: | :--- | :--- | :--- | :--- |",
            ]
            for i, (embarked_pull, car) in enumerate(self._iter_embarked_pulls()):
                title = await self.repository.get_pull_request_title(
                    embarked_pull.user_pull_request_number
                )
                # NOTE(sileht): we use this wierd url format to not trigger the GitHub pull request cross references
                # [#1234](/Mergifyio/mergify-engine/pull/1234]
                try:
                    fancy_priority = queue.PriorityAliases(
                        embarked_pull.config["priority"]
                    ).name
                except ValueError:
                    fancy_priority = str(embarked_pull.config["priority"])

                speculative_checks = ""
                if car is not None:
                    if (
                        car.train_car_state.checks_type
                        == train_car.TrainCarChecksType.INPLACE
                    ):
                        speculative_checks = train_utils.build_pr_link(
                            self.repository,
                            embarked_pull.user_pull_request_number,
                            "in place",
                        )
                    elif (
                        car.train_car_state.checks_type
                        == train_car.TrainCarChecksType.DRAFT
                    ):
                        if car.queue_pull_request_number is None:
                            raise RuntimeError(
                                "car's spec checks type is draft, but queue_pull_request_number is None"
                            )

                        speculative_checks = train_utils.build_pr_link(
                            self.repository, car.queue_pull_request_number
                        )

                queued_at = date.pretty_datetime(embarked_pull.queued_at)
                table.append(
                    f"| {i + 1} "
                    f"| {title} ({train_utils.build_pr_link(self.repository, embarked_pull.user_pull_request_number)}) "
                    f"| {embarked_pull.config['name']}/{fancy_priority} "
                    f"| {speculative_checks} "
                    f"| {queued_at}"
                    "|"
                )

            description += (
                "\n\n**The following pull requests are queued:**\n\n" + "\n".join(table)
            )

        description += constants.MERGIFY_MERGE_QUEUE_PULL_REQUEST_DOC

        if for_queue_pull_request:
            # FIXME(sileht): This should be on top of the description in case
            # of the summary is truncated
            description += utils.get_mergify_payload(constants.MERGE_QUEUE_BODY_INFO)
        return description

    async def get_pull_summary(
        self,
        ctxt: context.Context,
        queue_rule: "qr_config.QueueRule",
        pull_rule: "prr_config.EvaluatedPullRequestRule | None" = None,
    ) -> str:
        # NOTE(sileht): beware before using this method, car.update_state() must have been called earlier
        # to have up2date informations
        _, ep = self.find_embarked_pull(ctxt.pull["number"])
        if ep is None:
            return ""
        if ep.car is None:
            description = f"#{ctxt.pull['number']} is queued for merge."
            description += await self.generate_merge_queue_summary_footer(
                queue_rule_report=merge_train_types.QueueRuleReport(
                    name=ep.embarked_pull.config["name"],
                    summary=queue_rule.merge_conditions.get_summary(),
                ),
                pull_rule=pull_rule,
            )
            return description.strip()
        else:
            return await ep.car.generate_merge_queue_summary(pull_rule=pull_rule)

    async def _close_pull_request(
        self, pull_request_number: github_types.GitHubPullRequestNumber
    ) -> None:
        try:
            await self.repository.installation.client.patch(
                f"/repos/{self.repository.installation.owner_login}/{self.repository.repo['name']}/pulls/{pull_request_number}",
                json={"state": "closed"},
            )
        except http.HTTPNotFound:
            self.log.warning(
                "fail to close merge queue pull request",
                pull_request_number=pull_request_number,
                exc_info=True,
            )

    async def refresh_pulls(
        self,
        source: str,
        priority_first_pull_request: worker_pusher.Priority = worker_pusher.Priority.medium,
        additional_pull_request: None | (github_types.GitHubPullRequestNumber) = None,
    ) -> None:
        pulls = await self.get_pulls()
        if additional_pull_request and additional_pull_request not in pulls:
            pulls.append(additional_pull_request)

        pipe = await self.repository.installation.redis.stream.pipeline()
        for i, pull_number in enumerate(pulls):
            await refresher.send_pull_refresh(
                pipe,
                self.repository.repo,
                pull_request_number=pull_number,
                action="internal",
                source=source,
                priority=priority_first_pull_request
                if i == 0
                else worker_pusher.Priority.medium,
            )
        await pipe.execute()
