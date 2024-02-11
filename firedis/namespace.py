from __future__ import annotations
from typing import (
    Any,
    Literal,
    Iterator,
    NoReturn,
    TypeVar,
    Generic,
    Callable,
    overload,
)
import datetime as dt
import logging
import socket
import pickle
from pickle import UnpicklingError
from dataclasses import dataclass, field
from redis import Redis, Connection, ResponseError
from redis.exceptions import DataError
from redis._parsers.hiredis import _HiredisParser
import hiredis


ExpiryT = int | dt.timedelta
AbsExpiryT = int | dt.datetime
EncodableT = str | int | float | bytes | memoryview
T = TypeVar("T")
J = TypeVar("J")

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


@dataclass(slots=True)
class Namespace(Generic[T]):
    """
    Provides generic, type-safe access to a Redis namespace, wrapping all value-based
    I/O in `loads` and `dumps` respectively.

    Makes significant performance optimizations by assuming that `redis` is:
        - A SINGLE connection client
        - Connected to a LOCAL redis instance, so we don't have to handle connection issues.
        - NOT using local caching. All operations should be real-time.
        - Only being used synchronously
        - A controlled environment where you can safely store python objects
          and unpickle values to Python objects. DO NOT read data with a Namespace that you
          did not write with a Namespace or otherwise pickled yourself.
    """

    namespace: str
    redis: Redis
    loads: Callable[[bytes], Any] = default_loads
    dumps: Callable[..., EncodableT] = default_dumps
    pack_command: Callable[..., Any] = hiredis.pack_command
    prefix: str = delay_init()
    _conn: Connection = delay_init()
    _parser: _HiredisParser = delay_init()
    _reader: hiredis.Reader = delay_init()
    _sock: socket.socket = delay_init()
    _execute: Callable[[tuple[EncodableT, ...]], Any] = delay_init()
    _scripts: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """
        Decides which method to use for `self._execute`.

        Our optimized implementation of `Redis.execute_command` requires specific assumptions
        about the redis client and its internal API. This is risky, so we should validate these
        assumptions here and default to the public API otherwise.
        """

        def fail(msg):
            logging.error(msg + ". Disabling optimizations.")
            self._execute = self.execute_command_fallback

        self.prefix = self.namespace + ":"
        conn = self.redis.connection
        connection_kwargs = getattr(conn, "connection_kwargs", {})
        cache_enabled = connection_kwargs.get("cache_enabled", False)
        sock = getattr(conn, "_sock", MISSING)
        parser = getattr(conn, "_parser", MISSING)
        reader = getattr(parser, "_reader", MISSING)

        if conn is None:
            return fail("Namespace should be used with single connection Redis.")

        if cache_enabled:
            return fail("Namespace should be used with `cache_enabled=False` Redis")

        if sock is MISSING:
            return fail("Redis client doesn't have a `_sock` attribute")

        if not sock:
            conn.connect()

        if not isinstance(sock, socket.socket):
            return fail("`Redis._sock` is not connected")

        if not isinstance(parser, _HiredisParser):
            return fail("Expected `Connection._parser` to be a `_HiredisParser`")

        if not isinstance(reader, hiredis.Reader):
            return fail("Expected `_HiredisParser._reader` to be a `hiredis.Reader`")

        self._conn = conn
        self._sock = sock
        self._parser = parser
        self._reader = reader
        self._execute = self.execute_command_optimized

    def execute_command_fallback(self, args: tuple[EncodableT, ...]):
        """
        Used if Redis client doesn't meet our assumptions, or its internal API changes.
        """
        return self.redis.execute_command(*args)

    def execute_command_optimized(self, args: tuple[EncodableT, ...]):
        """
        A custom implementation of `Redis.execute_command` that eliminates a dozen or so
        layers of function calls and checks that are unnecessary given our assumptions.

        Assumes `args` are already valid arguments to `hiredis.pack_command`
        """
        self._sock.sendall(self.pack_command(args))

        try:
            while (res := self._reader.gets(False)) is False:
                self._parser.read_from_socket()

        except socket.timeout:
            self._conn.disconnect()
            raise TimeoutError(
                f"Timeout reading from {self._conn.host}:{self._conn.port}"
            )
        except OSError as e:
            self._conn.disconnect()
            raise ConnectionError(
                f"Error while reading from {self._conn.host}:{self._conn.port} : {e.args}"
            )
        except BaseException:
            self._conn.disconnect()
            raise

        if isinstance(res, ResponseError):
            try:
                raise res
            finally:
                del res

        return res

    # DICT METHODS
    # -----------------

    def __setitem__(self, key: str, value: T):
        self._execute((b"SET", self.prefix + key, self.dumps(value)))

    def __getitem__(self, key: str) -> T:
        if (v := self._execute((b"GET", self.prefix + key))) is None:
            raise KeyError(key)
        return self.loads(v)

    def __delitem__(self, key: str):
        if not 1 == self._execute((b"DEL", self.prefix + key)):
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        return 1 == self._execute((b"EXISTS", self.prefix + key))

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (dict, type(self))):
            return NotImplemented
        if isinstance(other, type(self)):
            return self.to_dict() == other.to_dict()
        return self.to_dict() == other

    def __or__(self, other: dict[str, J] | Namespace[J]) -> dict[str, T | J]:
        if not isinstance(other, (dict, type(self))):
            return NotImplemented
        other_d = other if isinstance(other, dict) else other.to_dict()  # type:ignore
        result = self.to_dict()
        result.update(other_d)  # type:ignore
        return result  # type:ignore

    def __ror__(self, other: dict[str, J] | Namespace[J]) -> dict[str, T | J]:
        return self.__or__(other)

    def __ior__(self, other: dict[str, T] | Namespace[T]) -> Namespace[T]:
        if not isinstance(other, (dict, type(self))):
            return NotImplemented
        if isinstance(other, dict):
            self.update(other)
        else:
            self.update(other.to_dict())
        return self

    def __reversed__(self) -> NoReturn:
        raise NotImplementedError(
            "reverse cannot be supported since redis keys are unordered."
        )

    def update(self, mapping: dict[str, T]):
        return self.mset(mapping)

    def clear(self) -> int:
        keys = self._execute((b"KEYS", self.prefix + "*"))
        return 0 if not keys else self._execute((b"DEL",) + tuple(keys))

    def popitem(self) -> NoReturn:
        raise NotImplementedError(
            "popitem cannot be supported since insertion order is not known. Use pop instead."
        )

    @overload
    def pop(self, key: str) -> T: ...

    @overload
    def pop(self, key: str, default: T) -> T: ...

    @overload
    def pop(self, key: str, default: None) -> T | None: ...

    def pop(self, key: str, default: Any = MISSING) -> T | None:
        if (
            v := self._execute((b"GETDEL", self.prefix + key))
        ) is None and default is MISSING:
            raise KeyError(key)
        return default if v is None else self.loads(v)

    def items(self) -> list[tuple[str, T]]:
        keys = self._execute((b"KEYS", self.prefix + "*"))
        if not keys:
            return []
        return list(
            zip(
                [k.decode().removeprefix(self.prefix) for k in keys],
                [self.loads(v) for v in self._execute((b"MGET",) + tuple(keys))],
            )
        )

    def values(self) -> list[T]:
        keys = self._execute((b"KEYS", self.prefix + "*"))
        if not keys:
            return []
        return [self.loads(v) for v in self._execute((b"MGET",) + tuple(keys))]

    def setdefault(self, key: str, default: T) -> T:
        if 1 == self._execute((b"SETNX", self.prefix + key, self.dumps(default))):
            return default
        return self.get(key, default)

    def to_dict(self) -> dict[str, T]:
        return {k: v for k, v in self.items()}

    # DICT & REDIS METHODS
    # --------------------

    def keys(self) -> list[str]:
        "Returns all keys in the namespace"
        return [
            k.decode().removeprefix(self.prefix)
            for k in self._execute((b"KEYS", self.prefix + "*"))
        ]

    @overload
    def get(self, name: str) -> T | None: ...

    @overload
    def get(self, name: str, default: T) -> T: ...

    def get(self, name: str, default=None) -> T | None:
        return (
            default
            if (v := self._execute((b"GET", self.prefix + name))) is None
            else self.loads(v)
        )

    # REDIS COMMANDS
    # --------------

    @overload
    def set(
        self,
        name: str,
        value: T,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: Literal[False] = False,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
    ) -> None: ...

    @overload
    def set(
        self,
        name: str,
        value: T,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: Literal[True] = True,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
    ) -> T | None: ...

    def set(
        self,
        name: str,
        value: T,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
    ) -> T | None:
        pieces: list[EncodableT] = [b"SET", self.prefix + name, self.dumps(value)]

        if ex is not None:
            pieces.append(b"EX")
            if isinstance(ex, dt.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            elif isinstance(ex, str) and ex.isdigit():
                pieces.append(int(ex))
            else:
                raise DataError("ex must be datetime.timedelta or int")

        if px is not None:
            pieces.append(b"PX")
            if isinstance(px, dt.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")

        if exat is not None:
            pieces.append(b"EXAT")
            if isinstance(exat, dt.datetime):
                exat = int(exat.timestamp())
            pieces.append(exat)

        if pxat is not None:
            pieces.append(b"PXAT")
            if isinstance(pxat, dt.datetime):
                pxat = int(pxat.timestamp() * 1000)
            pieces.append(pxat)

        if keepttl:
            pieces.append(b"KEEPTTL")

        if nx:
            pieces.append(b"NX")
        if xx:
            pieces.append(b"XX")

        if get:
            pieces.append(b"GET")
            return (
                None if (v := self._execute(tuple(pieces))) is None else self.loads(v)
            )
        return self._execute(tuple(pieces))

    def setnx(self, name: str, value: T) -> int:
        return self._execute((b"SETNX", self.prefix + name, self.dumps(value)))

    def setex(self, name: str, time: ExpiryT, value: T):
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds())
        return self._execute((b"SETEX", self.prefix + name, time, self.dumps(value)))

    def psetex(self, name: str, time: ExpiryT, value: T):
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds() * 1000)
        return self._execute((b"PSETEX", self.prefix + name, time, self.dumps(value)))

    def mset(self, mapping: dict[str, T]):
        items: list[EncodableT] = [b"MSET"]
        for k, v in mapping.items():
            items.extend((self.prefix + k, self.dumps(v)))
        self._execute(tuple(items))

    def msetnx(self, mapping: dict[str, T]) -> bool:
        items: list[EncodableT] = [b"MSETNX"]
        for k, v in mapping.items():
            items.extend((self.prefix + k, self.dumps(v)))
        return self._execute(tuple(items))

    @overload
    def mget(self, keys: list[str]) -> list[T | None]: ...

    @overload
    def mget(self, keys: list[str], default: T) -> list[T]: ...

    def mget(self, keys: list[str], default: T | None = None):
        return [
            default if v is None else self.loads(v)
            for v in self._execute((b"MGET",) + tuple([self.prefix + k for k in keys]))
        ]

    # STRING COMMANDS
    # ---------------

    def rename(self, src: str, dst: str):
        return self._execute((b"RENAME", self.prefix + src, self.prefix + dst))

    def renamenx(self, src: str, dst: str) -> int:
        return self._execute((b"RENAMENX", self.prefix + src, self.prefix + dst))

    def append(self, key: str, value: T) -> int:
        return self._execute((b"APPEND", self.prefix + key, self.dumps(value)))

    def strlen(self, name: str) -> int:
        return self._execute((b"STRLEN", self.prefix + name))

    def substr(self, name: str, start: int, end: int = -1) -> str:
        return self._execute((b"SUBSTR", self.prefix + name, start, end)).decode()

    @overload
    def lcs(
        self,
        key1: str,
        key2: str,
        len: Literal[False] = False,
        idx: Literal[False] = False,
        minmatchlen: int = 0,
        withmatchlen: bool = False,
    ) -> str: ...

    @overload
    def lcs(
        self,
        key1: str,
        key2: str,
        len: Literal[True] = True,
        idx: Literal[False] = False,
        minmatchlen: int = 0,
        withmatchlen: bool = False,
    ) -> int: ...

    @overload
    def lcs(
        self,
        key1: str,
        key2: str,
        len: Literal[False] = False,
        idx: Literal[True] = True,
        minmatchlen: int = 0,
        withmatchlen: bool = False,
    ) -> list: ...

    def lcs(
        self,
        key1: str,
        key2: str,
        len: bool = False,
        idx: bool = False,
        minmatchlen: int = 0,
        withmatchlen: bool = False,
    ) -> str | int | list:
        pieces: list[EncodableT] = [b"LCS", self.prefix + key1, self.prefix + key2]

        if len:
            pieces.append(b"LEN")
            return self._execute(tuple(pieces))

        if idx:
            pieces.append(b"IDX")
            if minmatchlen:
                pieces.extend((b"MINMATCHLEN", minmatchlen))
            if withmatchlen:
                pieces.append(b"WITHMATCHLEN")
            return self._execute(tuple(pieces))

        return self._execute(tuple(pieces)).decode()

    # TIME COMMANDS
    # -------------

    def expire(
        self,
        name: str,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds())
        items: list[EncodableT] = [b"EXPIRE", self.prefix + name, time]
        if nx:
            items.append(b"NX")
        if xx:
            items.append(b"XX")
        if gt:
            items.append(b"GT")
        if lt:
            items.append(b"LT")
        return self._execute(tuple(items))

    def expireat(
        self,
        name: str,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(when, dt.datetime):
            when = int(when.timestamp())
        items: list[EncodableT] = [b"EXPIREAT", self.prefix + name, when]
        if nx:
            items.append(b"NX")
        if xx:
            items.append(b"XX")
        if gt:
            items.append(b"GT")
        if lt:
            items.append(b"LT")
        return self._execute(tuple(items))

    def pexpire(
        self,
        name: str,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds() * 1000)
        items: list[EncodableT] = [b"PEXPIRE", self.prefix + name, time]
        if nx:
            items.append(b"NX")
        if xx:
            items.append(b"XX")
        if gt:
            items.append(b"GT")
        if lt:
            items.append(b"LT")
        return self._execute(tuple(items))

    def pexpireat(
        self,
        name: str,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(when, dt.datetime):
            when = int(when.timestamp() * 1000)
        items: list[EncodableT] = [b"PEXPIREAT", self.prefix + name, when]
        if nx:
            items.append(b"NX")
        if xx:
            items.append(b"XX")
        if gt:
            items.append(b"GT")
        if lt:
            items.append(b"LT")
        return self._execute(tuple(items))

    def ttl(self, name: str) -> int:
        return self._execute((b"TTL", self.prefix + name))

    def pttl(self, name: str) -> int:
        return self._execute((b"PTTL", self.prefix + name))

    def expiretime(self, name: str) -> int:
        return self._execute((b"EXPIRETIME", self.prefix + name))

    def pexpiretime(self, name: str) -> int:
        return self._execute((b"PEXPIRETIME", self.prefix + name))

    def persist(self, name: str) -> int:
        return self._execute((b"PERSIST", self.prefix + name))

    # MOTION COMMANDS
    # ---------------

    def exists(self, *names: str) -> int:
        "Returns the number of `keys` that exist"
        return self._execute((b"EXISTS",) + tuple([self.prefix + k for k in names]))

    def delete(self, *names: str) -> int:
        return self._execute((b"DEL",) + tuple([self.prefix + k for k in names]))

    def getdel(self, name: str) -> None | T:
        return (
            None
            if (v := self._execute((b"GETDEL", self.prefix + name))) is None
            else self.loads(v)
        )

    def move(self, name: str, db: int):
        return self._execute((b"MOVE", self.prefix + name, db))

    def copy(
        self,
        source: str,
        destination: str,
        destination_db: int | None = None,
        replace: bool = False,
    ):
        params: list[EncodableT] = [b"COPY", source, destination]
        if destination_db is not None:
            params.extend((b"DB", destination_db))
        if replace:
            params.append(b"REPLACE")
        return self._execute(tuple(params))

    def dump(self, name: str) -> bytes:
        return self._execute((b"DUMP", self.prefix + name))

    def restore(
        self,
        name: str,
        ttl: float,
        value: bytes,
        replace: bool = False,
        absttl: bool = False,
        idletime: int | None = None,
        frequency: int | None = None,
    ):
        items: list[EncodableT] = [b"RESTORE", self.prefix + name, ttl, value]
        if replace:
            items.append(b"REPLACE")
        if absttl:
            items.append(b"ABSTTL")
        if idletime is not None:
            items.extend((b"IDLETIME", idletime))
        if frequency is not None:
            items.extend((b"FREQ", frequency))
        return self._execute(tuple(items))

    # MISC COMMANDS
    # -------------

    def type(self, name: str) -> str | None:
        return (
            None
            if (v := self._execute((b"TYPE", self.prefix + name))) == b"none"
            else v.decode()
        )

    def memory_usage(self, key: str, samples: int | None = None) -> int:
        args: list[EncodableT] = [b"MEMORY", b"USAGE", self.prefix + key]
        if samples is not None:
            args.extend((b"SAMPLES", samples))
        return self._execute(tuple(args))

    @overload
    def object(
        self, infotype: Literal["refcount", "idletime", "freq"], key: str
    ) -> int: ...

    @overload
    def object(self, infotype: Literal["encoding"], key: str) -> str: ...

    def object(
        self,
        infotype: Literal["refcount", "encoding", "idletime", "freq"],
        key: str,
    ) -> int | str:
        res = self._execute((b"OBJECT", infotype, self.prefix + key))
        return res if infotype != "encoding" else res.decode()

    def debug_object(self, key: str):
        return self._execute((b"DEBUG", b"OBJECT", self.prefix + key))

    def unlink(self, *names: str):
        return self._execute((b"UNLINK",) + tuple([self.prefix + k for k in names]))
