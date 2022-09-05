import dataclasses
import enum
import typing


_K = typing.TypeVar("_K")
_V = typing.TypeVar("_V")


# NOTE(sileht): Sentinel object (eg: `marker = object()`) can't be expressed
# with typing yet use the proposed workaround instead:
#   https://github.com/python/typing/issues/689
#   https://www.python.org/dev/peps/pep-0661/
class UnsetType(enum.Enum):
    _MARKER = 0


Unset: typing.Final = UnsetType._MARKER


@dataclasses.dataclass
class Cache(typing.Generic[_K, _V]):
    _cache: typing.Dict[_K, _V] = dataclasses.field(default_factory=dict)

    def get(self, key: _K) -> typing.Union[_V, UnsetType]:
        return self._cache.get(key, Unset)

    def set(self, key: _K, value: _V) -> None:
        self._cache[key] = value

    def delete(self, key: _K) -> None:
        try:
            del self._cache[key]
        except KeyError:
            pass

    def clear(self) -> None:
        self._cache.clear()

    def __setitem__(self, key: _K, value: _V) -> None:
        return self.set(key, value)

    def __getitem__(self, key: _K) -> typing.Union[_V, UnsetType]:
        return self.get(key)

    def __delitem__(self, key: _K) -> None:
        self.delete(key)


@dataclasses.dataclass
class SingleCache(typing.Generic[_V]):
    _cache: typing.Union[_V, UnsetType] = dataclasses.field(default=Unset)

    def get(self) -> typing.Union[_V, UnsetType]:
        return self._cache

    def set(self, value: _V) -> None:
        self._cache = value

    def delete(self) -> None:
        self._cache = Unset
