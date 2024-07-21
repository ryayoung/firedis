from __future__ import annotations
from typing import Any
import errno
import pickle
import datetime as dt
from pickle import UnpicklingError
from dataclasses import field, dataclass
from redis import Redis

NONBLOCKING_EXCEPTION_ERROR_NUMBERS: dict[type[Exception], Any] = {
    BlockingIOError: errno.EWOULDBLOCK
}

try:
    import ssl

    if hasattr(ssl, "SSLWantReadError"):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

except ImportError:
    pass

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())


ExpiryT = int | dt.timedelta
AbsExpiryT = int | dt.datetime
EncodableT = str | int | float | bytes | memoryview

HiredisPackArgs = tuple[EncodableT, ...]


class MISSING:
    pass


def default_loads(value: bytes) -> Any:
    try:
        # Expected to fail if decoded value is a string, which we don't pickle.
        return pickle.loads(value)
    except UnpicklingError:
        return value.decode()


def default_dumps(value: Any) -> bytes:
    if type(value) is str:
        # Optimization: don't pickle strings. Also, we don't need
        # to encode them since hiredis.pack_command will do that quicker
        return value  # type:ignore
    return pickle.dumps(value)


def delay_init() -> Any:
    # Avoids the need to use type:ignore for attrs to initialize in post_init
    return field(init=False, default=MISSING)


@dataclass
class _BaseNamespace:
    namespace: str
    redis: Redis
