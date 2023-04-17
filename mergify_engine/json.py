import datetime
import enum
import json
import typing
import uuid


_ENUM_TYPES = {}


_EnumT = typing.TypeVar("_EnumT", bound=type[enum.Enum])


def register_enum_type(enum_cls: _EnumT) -> _EnumT:
    if enum_cls.__name__ in _ENUM_TYPES:
        raise RuntimeError(f"{enum_cls.__name__} already registered")

    _ENUM_TYPES[enum_cls.__name__] = enum_cls
    return enum_cls


class Encoder(json.JSONEncoder):
    def default(self, v: typing.Any) -> typing.Any:
        if isinstance(v, enum.Enum):
            return {
                "__pytype__": "enum",
                "class": type(v).__name__,
                "name": v.name,
            }

        if isinstance(v, datetime.timedelta):
            return {
                "__pytype__": "datetime.timedelta",
                "value": str(v.total_seconds()),
            }

        if isinstance(v, datetime.datetime):
            return {
                "__pytype__": "datetime.datetime",
                "value": v.isoformat(),
            }

        if isinstance(v, set):
            return {
                "__pytype__": "set",
                "value": list(v),
            }

        if isinstance(v, uuid.UUID):
            return {
                "__pytype__": "uuid.UUID",
                "value": v.hex,
            }

        return super().default(v)


JSONPyType = typing.Literal["enum"]


class JSONObjectDict(typing.TypedDict, total=False):
    __pytype__: JSONPyType


def _decode(v: dict[typing.Any, typing.Any]) -> typing.Any:
    if v.get("__pytype__") == "enum":
        cls_name = v["class"]
        enum_cls = _ENUM_TYPES[cls_name]
        enum_name = v["name"]
        return enum_cls[enum_name]
    if v.get("__pytype__") == "datetime.timedelta":
        return datetime.timedelta(seconds=float(v["value"]))
    if v.get("__pytype__") == "datetime.datetime":
        return datetime.datetime.fromisoformat(v["value"])
    if v.get("__pytype__") == "set":
        return set(v["value"])
    if v.get("__pytype__") == "uuid.UUID":
        return uuid.UUID(v["value"])
    return v


def dumps(v: typing.Any) -> str:
    return json.dumps(v, cls=Encoder)


def loads(v: str | bytes) -> typing.Any:
    return json.loads(v, object_hook=_decode)
