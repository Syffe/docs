import ddtrace.pin
import redis.asyncio
from redis.asyncio import connection as redis_connection_mod


ddtrace.pin.Pin(service=None).onto(redis.asyncio.Redis)


# TODO(sileht): remove me when redis > 4.3.4 is released
original_error_message = redis_connection_mod.Connection._error_message  # type: ignore[attr-defined]


def patch_error_message(
    self: redis_connection_mod.Connection, exception: Exception
) -> str:
    try:
        return original_error_message(self, exception)  # type: ignore[no-any-return]
    except IndexError:
        return f"Error connecting to {self.host}:{self.port}. Connection reset by peer."


redis_connection_mod.Connection._error_message = patch_error_message  # type: ignore[attr-defined]
