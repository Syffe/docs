"""
This is a copy of https://github.com/spulec/freezegun/blob/master/freezegun/api.py
with some custom made improvements, like remove deprecated/unused stuff, add typing.
"""
from __future__ import annotations

import asyncio
import calendar
from collections import abc
import copyreg
import dataclasses
import datetime
import functools
import inspect
import sys
import time
import typing
import unittest
import warnings


if typing.TYPE_CHECKING:
    import types


DEFAULT_IGNORE_LIST = [
    "nose.plugins",
    "six.moves",
    "django.utils.six.moves",
    "google.gax",
    "threading",
    "multiprocessing",
    "Queue",
    "selenium",
    "_pytest.terminal.",
    "_pytest.runner.",
    "gi",
    "prompt_toolkit",
]


class TardisSettings:
    def __init__(self, default_ignore_list: list[str] | None = None) -> None:
        self.default_ignore_list = default_ignore_list or DEFAULT_IGNORE_LIST[:]


tardis_settings = TardisSettings()


class ConfigurationError(Exception):
    pass


def configure(
    default_ignore_list: list[str] | None = None,
    extend_ignore_list: list[str] | None = None,
) -> None:
    if default_ignore_list is not None and extend_ignore_list is not None:
        raise ConfigurationError(
            "Either default_ignore_list or extend_ignore_list might be given, not both",
        )
    if default_ignore_list:
        tardis_settings.default_ignore_list = default_ignore_list
    if extend_ignore_list:
        tardis_settings.default_ignore_list = [
            *tardis_settings.default_ignore_list,
            *extend_ignore_list,
        ]


def reset_config() -> None:
    global tardis_settings
    tardis_settings = TardisSettings()


_CallableT = typing.TypeVar("_CallableT", bound=typing.Callable[..., typing.Any])


def wrap_coroutine(api: typing.Any, coroutine: _CallableT) -> _CallableT:
    @functools.wraps(coroutine)
    async def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        with api as time_factory:
            if api.as_arg:
                result = await coroutine(time_factory, *args, **kwargs)
            else:
                result = await coroutine(*args, **kwargs)
        return result

    return typing.cast(_CallableT, wrapper)


_TIME_NS_PRESENT = hasattr(time, "time_ns")
_MONOTONIC_NS_PRESENT = hasattr(time, "monotonic_ns")
_PERF_COUNTER_NS_PRESENT = hasattr(time, "perf_counter_ns")
_EPOCH = datetime.datetime(1970, 1, 1)  # noqa: DTZ001
_EPOCHTZ = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

real_time = time.time
real_localtime = time.localtime
real_gmtime = time.gmtime
real_monotonic = time.monotonic
real_perf_counter = time.perf_counter
real_strftime = time.strftime
real_date = datetime.date
real_datetime = datetime.datetime
real_date_objects = [
    real_time,
    real_localtime,
    real_gmtime,
    real_monotonic,
    real_perf_counter,
    real_strftime,
    real_date,
    real_datetime,
]

real_clock: typing.Callable[[], float] | None = getattr(time, "clock", None)

if _TIME_NS_PRESENT:
    real_time_ns = time.time_ns
    real_date_objects.append(real_time_ns)

if _MONOTONIC_NS_PRESENT:
    real_monotonic_ns = time.monotonic_ns
    real_date_objects.append(real_monotonic_ns)

if _PERF_COUNTER_NS_PRESENT:
    real_perf_counter_ns = time.perf_counter_ns
    real_date_objects.append(real_perf_counter_ns)

_real_time_object_ids = {id(obj) for obj in real_date_objects}

freeze_factories: list[FrozenDateTimeFactory | TickingDateTimeFactory] = []
ignore_lists: list[str | tuple[str, ...]] = []
tick_flags: list[bool] = []

# keep a cache of module attributes otherwise freezegun will need to analyze too many modules all the time
_GLOBAL_MODULES_CACHE = {}


def _get_module_attributes(
    module: types.ModuleType,
) -> list[tuple[typing.Any, typing.Any]]:
    result: list[tuple[typing.Any, typing.Any]] = []
    try:
        module_attributes = dir(module)
    except (ImportError, TypeError):
        return result
    for attribute_name in module_attributes:
        try:
            attribute_value = getattr(module, attribute_name)
        except (ImportError, AttributeError, TypeError):
            # For certain libraries, this can result in ImportError(_winreg) or AttributeError (celery)
            continue
        else:
            result.append((attribute_name, attribute_value))
    return result


def _setup_module_cache(module: types.ModuleType) -> None:
    date_attrs = []
    all_module_attributes = _get_module_attributes(module)
    for attribute_name, attribute_value in all_module_attributes:
        if id(attribute_value) in _real_time_object_ids:
            date_attrs.append((attribute_name, attribute_value))

    _GLOBAL_MODULES_CACHE[module.__name__] = (
        _get_module_attributes_hash(module),
        date_attrs,
    )


def _get_module_attributes_hash(module: types.ModuleType) -> str:
    try:
        module_dir = dir(module)
    except (ImportError, TypeError):
        module_dir = []
    return f"{id(module)}-{hash(frozenset(module_dir))}"


def _get_cached_module_attributes(
    module: types.ModuleType,
) -> list[tuple[typing.Any, typing.Any]]:
    module_hash, cached_attrs = _GLOBAL_MODULES_CACHE.get(module.__name__, ("0", []))
    if _get_module_attributes_hash(module) == module_hash:
        return cached_attrs

    # cache miss: update the cache and return the refreshed value
    _setup_module_cache(module)
    # return the newly cached value
    _, cached_attrs = _GLOBAL_MODULES_CACHE[module.__name__]
    return cached_attrs


ParsableTimeT = str | datetime.datetime | datetime.date | datetime.timedelta | None


def convert_to_timezone_naive(time_to_convert: datetime.datetime) -> datetime.datetime:
    """
    Converts a potentially timezone-aware datetime to be a naive UTC datetime
    """
    if time_to_convert.tzinfo:
        time_to_convert -= time_to_convert.utcoffset()  # type: ignore[operator]
        time_to_convert = time_to_convert.replace(tzinfo=None)
    return time_to_convert


def _parse_time(time_to_convert: ParsableTimeT) -> datetime.datetime:
    if time_to_convert is None:
        time_to_convert = datetime.datetime.now(tz=datetime.UTC)

    if isinstance(time_to_convert, datetime.datetime):
        return convert_to_timezone_naive(time_to_convert)

    if isinstance(time_to_convert, datetime.date):
        return convert_to_timezone_naive(
            datetime.datetime.combine(time_to_convert, datetime.time()),
        )

    if isinstance(time_to_convert, datetime.timedelta):
        return convert_to_timezone_naive(
            datetime.datetime.now(tz=datetime.UTC) + time_to_convert,
        )

    # isinstance(time_to_convert, str)
    return convert_to_timezone_naive(datetime.datetime.fromisoformat(time_to_convert))


@dataclasses.dataclass
class TickingDateTimeFactory:
    time_traveled_to: datetime.datetime
    start: datetime.datetime

    def __call__(self) -> datetime.datetime:
        return self.time_traveled_to + (real_datetime.now() - self.start)

    def move_to(self, target_datetime: ParsableTimeT) -> None:
        # This method is not implemented in the original freezegun,
        # this is just to have correct typing
        pass


@dataclasses.dataclass
class FrozenDateTimeFactory:
    time_traveled_to: datetime.datetime

    def __call__(self) -> datetime.datetime:
        return self.time_traveled_to

    def tick(
        self,
        delta: int | float | datetime.timedelta = datetime.timedelta(seconds=1),
    ) -> None:
        if isinstance(delta, int | float):
            self.time_traveled_to += datetime.timedelta(seconds=delta)
        else:
            self.time_traveled_to += delta

    def move_to(self, target_datetime: ParsableTimeT) -> None:
        target_datetime = _parse_time(target_datetime)
        delta = target_datetime - self.time_traveled_to
        self.tick(delta=delta)


CALL_STACK_INSPECTION_LIMIT = 5


def _should_use_real_time() -> bool:
    if not CALL_STACK_INSPECTION_LIMIT:
        return False

    # Means stop() has already been called, so we can now return the real time
    if not ignore_lists:
        return True

    if not ignore_lists[-1]:
        return False

    current_frame = inspect.currentframe()
    if current_frame is None:
        return False

    frame = current_frame.f_back
    if frame is None:
        return False

    frame = frame.f_back
    if frame is None:
        return False

    for _ in range(CALL_STACK_INSPECTION_LIMIT):
        module_name = frame.f_globals.get("__name__")
        if module_name and module_name.startswith(ignore_lists[-1]):
            return True

        frame = frame.f_back
        if frame is None:
            break

    return False


def get_current_time() -> datetime.datetime:
    return freeze_factories[-1]()


def fake_time() -> float:
    if _should_use_real_time():
        return real_time()

    current_time = get_current_time()
    return (
        calendar.timegm(current_time.timetuple()) + current_time.microsecond / 1000000.0
    )


if _TIME_NS_PRESENT:

    def fake_time_ns() -> int:
        if _should_use_real_time():
            return real_time_ns()
        return int(int(fake_time()) * 1e9)


def fake_localtime(secs: float | None = None) -> time.struct_time:
    if secs is not None:
        return real_localtime(secs)

    if _should_use_real_time():
        return real_localtime()

    shifted_time = get_current_time() - datetime.timedelta(seconds=time.timezone)
    return shifted_time.timetuple()


def fake_gmtime(secs: float | None = None) -> time.struct_time:
    if secs is not None:
        return real_gmtime(secs)

    if _should_use_real_time():
        return real_gmtime()

    return get_current_time().timetuple()


def _get_fake_monotonic() -> float:
    # For monotonic timers like .monotonic(), .perf_counter(), etc
    current_time = get_current_time()
    return calendar.timegm(current_time.timetuple()) + current_time.microsecond / 1e6


def _get_fake_monotonic_ns() -> int:
    # For monotonic timers like .monotonic(), .perf_counter(), etc
    current_time = get_current_time()
    return (
        calendar.timegm(current_time.timetuple()) * 1000000 + current_time.microsecond
    ) * 1000


def fake_monotonic() -> float:
    if _should_use_real_time():
        return real_monotonic()

    return _get_fake_monotonic()


def fake_perf_counter() -> float:
    if _should_use_real_time():
        return real_perf_counter()

    return _get_fake_monotonic()


if _MONOTONIC_NS_PRESENT:

    def fake_monotonic_ns() -> int:
        if _should_use_real_time():
            return real_monotonic_ns()

        return _get_fake_monotonic_ns()


if _PERF_COUNTER_NS_PRESENT:

    def fake_perf_counter_ns() -> int:
        if _should_use_real_time():
            return real_perf_counter_ns()

        return _get_fake_monotonic_ns()


def fake_strftime(
    date_format: str,
    time_to_format: tuple[int, int, int, int, int, int, int, int, int]
    | time.struct_time
    | None = None,
) -> str:
    if time_to_format is None and not _should_use_real_time():
        time_to_format = fake_localtime()

    if time_to_format is None:
        return real_strftime(date_format)

    return real_strftime(date_format, time_to_format)


if real_clock is not None:

    def fake_clock() -> float:
        if _should_use_real_time():
            return real_clock()  # type: ignore[misc]

        if len(freeze_factories) == 1:
            return 0.0 if not tick_flags[-1] else real_clock()  # type: ignore[misc]

        first_frozen_time = freeze_factories[0]()
        last_frozen_time = get_current_time()

        timedelta = last_frozen_time - first_frozen_time
        total_seconds = timedelta.total_seconds()

        if tick_flags[-1]:
            total_seconds += real_clock()  # type: ignore[misc]

        return total_seconds


class FakeDateMeta(type):
    @classmethod
    def __instancecheck__(self, obj: typing.Any) -> bool:
        return isinstance(obj, real_date)

    @classmethod
    def __subclasscheck__(cls, subclass: typing.Any) -> bool:
        return issubclass(subclass, real_date)


class FakeDate(real_date, metaclass=FakeDateMeta):
    def __add__(self, other):  # type: ignore[no-untyped-def]
        result = real_date.__add__(self, other)
        if result is NotImplemented:
            return result
        return date_to_fakedate(result)

    def __sub__(self, other):  # type: ignore[no-untyped-def]
        result = real_date.__sub__(self, other)
        if result is NotImplemented:
            return result

        if isinstance(result, real_date):
            return date_to_fakedate(result)

        return result

    @classmethod
    def today(cls) -> FakeDate:
        result = cls._date_to_freeze()
        return date_to_fakedate(result)

    @staticmethod
    def _date_to_freeze() -> datetime.datetime:
        return get_current_time()


def date_to_fakedate(date: datetime.date) -> FakeDate:
    return FakeDate(date.year, date.month, date.day)


FakeDate.min = date_to_fakedate(real_date.min)
FakeDate.max = date_to_fakedate(real_date.max)


class FakeDatetimeMeta(FakeDateMeta):
    @classmethod
    def __instancecheck__(self, obj: typing.Any) -> bool:
        return isinstance(obj, real_datetime)

    @classmethod
    def __subclasscheck__(cls, subclass: typing.Any) -> bool:
        return issubclass(subclass, real_datetime)


class FakeDatetime(real_datetime, FakeDate, metaclass=FakeDatetimeMeta):
    def __add__(self, other):  # type: ignore[no-untyped-def]
        result = real_datetime.__add__(self, other)
        if result is NotImplemented:
            return result
        return datetime_to_fakedatetime(result)

    def __sub__(self, other):  # type: ignore[no-untyped-def]
        result = real_datetime.__sub__(self, other)
        if result is NotImplemented:
            return result
        if isinstance(result, real_datetime):
            return datetime_to_fakedatetime(result)
        return result

    def astimezone(self, tz: datetime.tzinfo | None = None) -> FakeDatetime:
        if tz is None:
            tz = real_datetime.now().astimezone().tzinfo
        return datetime_to_fakedatetime(real_datetime.astimezone(self, tz))

    @classmethod
    def fromtimestamp(
        cls,
        timestamp: float,
        tz: datetime.tzinfo | None = None,
    ) -> FakeDatetime:
        if tz is None:
            return datetime_to_fakedatetime(
                real_datetime.fromtimestamp(timestamp).replace(tzinfo=None),
            )
        return datetime_to_fakedatetime(real_datetime.fromtimestamp(timestamp, tz))

    def timestamp(self) -> float:
        if self.tzinfo is None:
            return typing.cast(float, (self - _EPOCH).total_seconds())
        return typing.cast(float, (self - _EPOCHTZ).total_seconds())

    @classmethod
    def now(cls, tz: datetime.tzinfo | None = None) -> FakeDatetime:
        now = cls._time_traveled_to() or real_datetime.now()
        result = tz.fromutc(now.replace(tzinfo=tz)) if tz else now
        return datetime_to_fakedatetime(result)

    def date(self) -> FakeDate:
        return date_to_fakedate(self)

    @property
    def nanosecond(self) -> int:
        return 0

    @classmethod
    def today(cls) -> FakeDatetime:
        return cls.now(tz=None)

    @classmethod
    def utcnow(cls) -> FakeDatetime:
        result = cls._time_traveled_to() or real_datetime.now(tz=datetime.UTC).replace(
            tzinfo=None,
        )
        return datetime_to_fakedatetime(result)

    @staticmethod
    def _time_traveled_to() -> datetime.datetime | None:
        if freeze_factories:
            return get_current_time()
        return None


def datetime_to_fakedatetime(datetime: datetime.datetime) -> FakeDatetime:
    return FakeDatetime(
        datetime.year,
        datetime.month,
        datetime.day,
        datetime.hour,
        datetime.minute,
        datetime.second,
        datetime.microsecond,
        datetime.tzinfo,
    )


FakeDatetime.min = datetime_to_fakedatetime(real_datetime.min)
FakeDatetime.max = datetime_to_fakedatetime(real_datetime.max)


def pickle_fake_date(
    datetime_: datetime.date | FakeDate,
) -> tuple[type[FakeDate], tuple[int, int, int]]:
    # A pickle function for FakeDate
    return FakeDate, (
        datetime_.year,
        datetime_.month,
        datetime_.day,
    )


def pickle_fake_datetime(
    datetime_: datetime.datetime | FakeDatetime,
) -> tuple[
    type[FakeDatetime],
    tuple[int, int, int, int, int, int, int, datetime.tzinfo | None],
]:
    # A pickle function for FakeDatetime
    return FakeDatetime, (
        datetime_.year,
        datetime_.month,
        datetime_.day,
        datetime_.hour,
        datetime_.minute,
        datetime_.second,
        datetime_.microsecond,
        datetime_.tzinfo,
    )


_T = typing.TypeVar("_T")


class _time_travel:
    def __init__(
        self,
        time_to_travel_to: ParsableTimeT,
        ignore: list[str],
        tick: bool,
        as_arg: bool = False,
        as_kwarg: str = "",
    ) -> None:
        self.time_to_travel_to = _parse_time(time_to_travel_to)
        self.ignore = tuple(ignore)
        self.tick = tick
        self.undo_changes: list[
            tuple[
                types.ModuleType | type[asyncio.AbstractEventLoop],
                str,
                typing.Callable[..., typing.Any],
            ]
        ] = []
        self.modules_at_start: set[str] = set()
        self.as_arg = as_arg
        self.as_kwarg = as_kwarg

    @typing.overload
    def __call__(self, func: type[_T]) -> type[_T]:
        ...

    @typing.overload
    def __call__(self, func: _CallableT) -> _CallableT:
        ...

    @typing.overload
    def __call__(self, func: typing.Callable[..., _T]) -> typing.Callable[..., _T]:
        ...

    def __call__(self, func):  # type: ignore[no-untyped-def]
        if inspect.isclass(func):
            return self.decorate_class(func)
        if inspect.iscoroutinefunction(func):
            return self.decorate_coroutine(func)
        return self.decorate_callable(func)

    def decorate_class(self, klass: type[_T]) -> type[_T]:
        if issubclass(klass, unittest.TestCase):
            # If it's a TestCase, we freeze time around setup and teardown, as well
            # as for every test case. This requires some care to avoid freezing
            # the time pytest sees, as otherwise this would distort the reported
            # timings.

            orig_setUpClass = klass.setUpClass
            orig_tearDownClass = klass.tearDownClass

            @classmethod  # type: ignore[misc]
            def setUpClass(cls) -> None:  # type: ignore[no-untyped-def]  # noqa: ARG001
                self.start()
                if orig_setUpClass is not None:
                    orig_setUpClass()
                self.stop()

            @classmethod  # type: ignore[misc]
            def tearDownClass(cls) -> None:  # type: ignore[no-untyped-def]  # noqa: ARG001
                self.start()
                if orig_tearDownClass is not None:
                    orig_tearDownClass()
                self.stop()

            klass.setUpClass = setUpClass  # type: ignore[method-assign, assignment]
            klass.tearDownClass = tearDownClass  # type: ignore[method-assign, assignment]

            orig_setUp = klass.setUp
            orig_tearDown = klass.tearDown

            def setUp(*args: typing.Any, **kwargs: typing.Any) -> None:
                self.start()
                if orig_setUp is not None:
                    orig_setUp(*args, **kwargs)

            def tearDown(*args: typing.Any, **kwargs: typing.Any) -> None:
                if orig_tearDown is not None:
                    orig_tearDown(*args, **kwargs)
                self.stop()

            klass.setUp = setUp  # type: ignore[method-assign]
            klass.tearDown = tearDown  # type: ignore[method-assign]

            return klass  # type: ignore[return-value]

        seen = set()

        klasses = klass.mro()
        for base_klass in klasses:
            for attr, attr_value in base_klass.__dict__.items():
                if attr.startswith("_") or attr in seen:
                    continue
                seen.add(attr)

                if (
                    not callable(attr_value)
                    or inspect.isclass(attr_value)
                    or isinstance(attr_value, staticmethod)
                ):
                    continue

                try:
                    setattr(klass, attr, self(attr_value))
                except (AttributeError, TypeError):
                    # Sometimes we can't set this for built-in types and custom callables
                    continue
        return klass

    def __enter__(self) -> TickingDateTimeFactory | FrozenDateTimeFactory:
        return self.start()

    def __exit__(self, *args: typing.Any) -> None:
        self.stop()

    def start(self) -> TickingDateTimeFactory | FrozenDateTimeFactory:
        freeze_factory: TickingDateTimeFactory | FrozenDateTimeFactory
        if self.tick:
            freeze_factory = TickingDateTimeFactory(
                self.time_to_travel_to,
                real_datetime.now(),
            )
        else:
            freeze_factory = FrozenDateTimeFactory(self.time_to_travel_to)

        is_already_started = len(freeze_factories) > 0
        freeze_factories.append(freeze_factory)
        ignore_lists.append(self.ignore)
        tick_flags.append(self.tick)

        if is_already_started:
            return freeze_factory

        # Change the modules
        datetime.datetime = FakeDatetime  # type: ignore[misc]
        datetime.date = FakeDate  # type: ignore[misc]

        time.time = fake_time
        time.monotonic = fake_monotonic
        time.perf_counter = fake_perf_counter
        time.localtime = fake_localtime
        time.gmtime = fake_gmtime
        time.strftime = fake_strftime  # type: ignore[assignment]

        copyreg.dispatch_table[real_datetime] = pickle_fake_datetime
        copyreg.dispatch_table[real_date] = pickle_fake_date

        # Change any place where the module had already been imported
        to_patch = [
            ("real_date", real_date, FakeDate),
            ("real_datetime", real_datetime, FakeDatetime),
            ("real_gmtime", real_gmtime, fake_gmtime),
            ("real_localtime", real_localtime, fake_localtime),
            ("real_monotonic", real_monotonic, fake_monotonic),
            ("real_perf_counter", real_perf_counter, fake_perf_counter),
            ("real_strftime", real_strftime, fake_strftime),
            ("real_time", real_time, fake_time),
        ]

        if _TIME_NS_PRESENT:
            time.time_ns = fake_time_ns
            to_patch.append(("real_time_ns", real_time_ns, fake_time_ns))

        if _MONOTONIC_NS_PRESENT:
            time.monotonic_ns = fake_monotonic_ns
            to_patch.append(("real_monotonic_ns", real_monotonic_ns, fake_monotonic_ns))

        if _PERF_COUNTER_NS_PRESENT:
            time.perf_counter_ns = fake_perf_counter_ns
            to_patch.append(
                ("real_perf_counter_ns", real_perf_counter_ns, fake_perf_counter_ns),
            )

        if real_clock is not None:
            # time.clock is deprecated and was removed in Python 3.8
            time.clock = fake_clock  # type: ignore[attr-defined]
            to_patch.append(("real_clock", real_clock, fake_clock))

        self.fake_names = tuple(fake.__name__ for real_name, real, fake in to_patch)  # type: ignore[attr-defined]
        self.reals = {id(fake): real for real_name, real, fake in to_patch}
        fakes = {id(real): fake for real_name, real, fake in to_patch}

        # Save the current loaded modules
        self.modules_at_start = set(sys.modules.keys())

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")

            for mod_name, module in list(sys.modules.items()):
                if mod_name is None or module is None or mod_name == __name__:
                    continue

                if mod_name.startswith(self.ignore) or mod_name.endswith(".six.moves"):
                    continue

                if not hasattr(module, "__name__") or module.__name__ in (
                    "datetime",
                    "time",
                ):
                    continue

                module_attrs = _get_cached_module_attributes(module)
                for attribute_name, attribute_value in module_attrs:
                    fake = fakes.get(id(attribute_value))
                    if fake:
                        setattr(module, attribute_name, fake)
                        self.undo_changes.append(
                            (module, attribute_name, attribute_value),
                        )

        # To avoid breaking `asyncio.sleep()`, let asyncio event loops see real
        # monotonic time even though we've just frozen `time.monotonic()` which
        # is normally used there. If we didn't do this, `await asyncio.sleep()`
        # would be hanging forever breaking many tests that use `time_travel`.
        #
        # Note that we cannot statically tell the class of asyncio event loops
        # because it is not officially documented and can actually be changed
        # at run time using `asyncio.set_event_loop_policy`. That's why we check
        # the type by creating a loop here and destroying it immediately.
        event_loop = asyncio.new_event_loop()
        event_loop.close()
        EventLoopClass = type(event_loop)
        self.undo_changes.append((EventLoopClass, "time", EventLoopClass.time))
        EventLoopClass.time = lambda self: real_monotonic()  # type: ignore[method-assign]  # noqa: ARG005

        return freeze_factory

    def stop(self) -> None:
        freeze_factories.pop()
        ignore_lists.pop()
        tick_flags.pop()

        if not freeze_factories:
            datetime.datetime = real_datetime  # type: ignore[misc]
            datetime.date = real_date  # type: ignore[misc]
            copyreg.dispatch_table.pop(real_datetime)
            copyreg.dispatch_table.pop(real_date)
            for module_or_object, attribute, original_value in self.undo_changes:
                setattr(module_or_object, attribute, original_value)
            self.undo_changes = []

            # Restore modules loaded after start()
            modules_to_restore = set(sys.modules.keys()) - self.modules_at_start
            self.modules_at_start = set()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for mod_name in modules_to_restore:
                    module = sys.modules.get(mod_name, None)
                    if mod_name is None or module is None:
                        continue

                    if mod_name.startswith(self.ignore) or mod_name.endswith(
                        ".six.moves",
                    ):
                        continue
                    if not hasattr(module, "__name__") or module.__name__ in (
                        "datetime",
                        "time",
                    ):
                        continue
                    for module_attribute in dir(module):
                        if module_attribute in self.fake_names:
                            continue
                        try:
                            attribute_value = getattr(module, module_attribute)
                        except (ImportError, AttributeError, TypeError):
                            # For certain libraries, this can result in ImportError(_winreg) or AttributeError (celery)
                            continue

                        real = self.reals.get(id(attribute_value))
                        if real:
                            setattr(module, module_attribute, real)

            time.time = real_time
            time.monotonic = real_monotonic
            time.perf_counter = real_perf_counter
            time.gmtime = real_gmtime
            time.localtime = real_localtime
            time.strftime = real_strftime
            if real_clock is not None:
                time.clock = real_clock  # type: ignore[attr-defined]

            if _TIME_NS_PRESENT:
                time.time_ns = real_time_ns

            if _MONOTONIC_NS_PRESENT:
                time.monotonic_ns = real_monotonic_ns

            if _PERF_COUNTER_NS_PRESENT:
                time.perf_counter_ns = real_perf_counter_ns

    def decorate_coroutine(self, coroutine: _CallableT) -> _CallableT:
        return wrap_coroutine(self, coroutine)

    def decorate_callable(
        self,
        func: typing.Callable[..., _T],
    ) -> typing.Callable[..., _T]:
        def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            with self as time_factory:
                if self.as_arg and self.as_kwarg:
                    raise AssertionError(
                        "You can't specify both as_arg and as_kwarg at the same time. Pick one.",
                    )

                if self.as_arg:
                    result = func(time_factory, *args, **kwargs)
                elif self.as_kwarg:
                    kwargs[self.as_kwarg] = time_factory
                    result = func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
            return result

        functools.update_wrapper(wrapper, func)

        return wrapper


AcceptableTimesT = (
    ParsableTimeT
    | typing.Callable[[], ParsableTimeT]
    | abc.Generator[ParsableTimeT, None, None]
)


def time_travel(
    time_to_travel_to: AcceptableTimesT | None = None,
    ignore: list[str] | None = None,
    tick: bool = False,
    as_arg: bool = False,
    as_kwarg: str = "",
) -> _time_travel:
    if callable(time_to_travel_to):
        return time_travel(
            time_to_travel_to(),
            ignore,
            tick,
            as_arg,
            as_kwarg,
        )

    if isinstance(time_to_travel_to, abc.Generator):
        return time_travel(
            next(time_to_travel_to),
            ignore,
            tick,
            as_arg,
            as_kwarg,
        )

    if ignore is None:
        ignore = []
    ignore = ignore[:]

    if tardis_settings.default_ignore_list:
        ignore.extend(tardis_settings.default_ignore_list)

    return _time_travel(
        time_to_travel_to=time_to_travel_to,
        ignore=ignore,
        tick=tick,
        as_arg=as_arg,
        as_kwarg=as_kwarg,
    )
