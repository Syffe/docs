import enum
import typing

from mergify_engine import json


if typing.TYPE_CHECKING:
    from mergify_engine.queue.merge_train.embarked_pull import EmbarkedPull
    from mergify_engine.queue.merge_train.train import Train
    from mergify_engine.queue.merge_train.train_car import TrainCar
    from mergify_engine.rules.config import partition_rules as partr_config


class QueueRuleReport(typing.NamedTuple):
    name: str
    summary: str


class EmbarkedPullWithCar(typing.NamedTuple):
    embarked_pull: "EmbarkedPull"
    car: "TrainCar | None"


class TrainAndTrainCar(typing.NamedTuple):
    train: "Train"
    train_car: "TrainCar"


# TODO: Maybe find a better name?
class ConvoyEmbarkedPullWithCarAndPos(typing.NamedTuple):
    car: "TrainCar | None"
    embarked_pull: "EmbarkedPull"
    partition_name: "partr_config.PartitionRuleName | None"
    position: int


class ConvoyEmbarkedPullWithTrain(typing.NamedTuple):
    train: "Train"
    convoy_embarked_pull: ConvoyEmbarkedPullWithCarAndPos


@json.register_enum_type
@enum.unique
class CiState(enum.Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
