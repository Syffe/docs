import datetime
import enum
import json
import typing
import uuid


_JSON_TYPES = {}


def register_type(enum_cls: type[enum.Enum]) -> None:
    if enum_cls.__name__ in _JSON_TYPES:
        raise RuntimeError(f"{enum_cls.__name__} already registered")
    else:
        _JSON_TYPES[enum_cls.__name__] = enum_cls


class Encoder(json.JSONEncoder):
    def default(self, v: typing.Any) -> typing.Any:
        if isinstance(v, enum.Enum):
            return {
                "__pytype__": "enum",
                "class": type(v).__name__,
                "name": v.name,
            }
        elif isinstance(v, datetime.timedelta):
            return {
                "__pytype__": "datetime.timedelta",
                "value": str(v.total_seconds()),
            }

        elif isinstance(v, datetime.datetime):
            return {
                "__pytype__": "datetime.datetime",
                "value": v.isoformat(),
            }
        elif isinstance(v, set):
            return {
                "__pytype__": "set",
                "value": list(v),
            }
        elif isinstance(v, uuid.UUID):
            return {
                "__pytype__": "uuid.UUID",
                "value": v.hex,
            }
        else:
            return super().default(v)


JSONPyType = typing.Literal["enum"]


class JSONObjectDict(typing.TypedDict, total=False):
    __pytype__: JSONPyType


def _decode(v: dict[typing.Any, typing.Any]) -> typing.Any:
    if v.get("__pytype__") == "enum":
        cls_name = v["class"]
        enum_cls = _JSON_TYPES[cls_name]
        enum_name = v["name"]
        return enum_cls[enum_name]
    elif v.get("__pytype__") == "datetime.timedelta":
        return datetime.timedelta(seconds=float(v["value"]))
    elif v.get("__pytype__") == "datetime.datetime":
        return datetime.datetime.fromisoformat(v["value"])
    elif v.get("__pytype__") == "set":
        return set(v["value"])
    elif v.get("__pytype__") == "uuid.UUID":
        return uuid.UUID(v["value"])
    return v


def dumps(v: typing.Any) -> str:
    return json.dumps(v, cls=Encoder)


def loads(v: str | bytes) -> typing.Any:
    return json.loads(v, object_hook=_decode)
