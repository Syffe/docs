import enum
import typing

from mergify_engine import json


if typing.TYPE_CHECKING:
    from mergify_engine.queue.merge_train.embarked_pull import EmbarkedPull
    from mergify_engine.queue.merge_train.train_car import TrainCar


class QueueRuleReport(typing.NamedTuple):
    name: str
    summary: str


class EmbarkedPullWithCar(typing.NamedTuple):
    embarked_pull: "EmbarkedPull"
    car: "TrainCar | None"


@enum.unique
class CiState(enum.Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


json.register_type(CiState)
