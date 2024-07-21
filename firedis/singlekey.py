from __future__ import annotations
from typing import (
    Any,
    Literal,
    TypeVar,
    Generic,
    Callable,
    overload,
)
import socket
import logging
import datetime as dt
from dataclasses import dataclass, field
from redis import Redis, Connection, ResponseError
from redis.exceptions import DataError
from redis._parsers.hiredis import _HiredisParser
import hiredis
from firedis.common import (
    NONBLOCKING_EXCEPTIONS,
    ExpiryT,
    AbsExpiryT,
    EncodableT,
    MISSING,
    default_loads,
    default_dumps,
    delay_init,
    _BaseNamespace,
)


Owner = TypeVar("Owner")
T = TypeVar("T")
J = TypeVar("J")


@dataclass(slots=True)
class SingleKey(_BaseNamespace, Generic[T]):
    """
    Provides generic, type-safe access to a single value in Redis, wrapping all value-based
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
    name: str = delay_init()
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

        self.name = self.namespace
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

    def execute_command_optimized(self, args: tuple[EncodableT, ...]):
        """
        Custom implementation of `Redis.execute_command` that eliminates a dozen or so
        layers of function calls and checks that are unnecessary given our assumptions.

        Assumes `args` are already valid arguments to `hiredis.pack_command`
        """
        self._sock.sendall(self.pack_command(args))
        reader = self._reader
        buffer = self._parser._buffer

        try:
            while (result := reader.gets(False)) is False:
                try:
                    buffer_length = self._sock.recv_into(buffer)

                    if buffer_length == 0:
                        raise ConnectionError("Server closed connection")

                    reader.feed(buffer, 0, buffer_length)

                except NONBLOCKING_EXCEPTIONS as ex:
                    raise ConnectionError(f"Error while reading from socket: {ex.args}")

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

        if isinstance(result, ResponseError):
            try:
                raise result
            finally:
                del result

        return result

    def execute_command_fallback(self, args: tuple[EncodableT, ...]):
        """
        Used if Redis client doesn't meet our assumptions, or its internal API changes.
        """
        return self.redis.execute_command(*args)

    @property
    def value(self) -> T:
        if (val := self._execute((b"GET", self.name))) is None:
            raise KeyError(f"Key '{self.name}' not found")
        return self.loads(val)

    def get(self) -> T | None:
        if (val := self._execute((b"GET", self.name))) is None:
            return None
        return self.loads(val)

    # REDIS COMMANDS
    # --------------

    @overload
    def set(
        self,
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
        pieces: list[EncodableT] = [b"SET", self.name, self.dumps(value)]

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

    def setnx(self, value: T) -> int:
        return self._execute((b"SETNX", self.name, self.dumps(value)))

    def setex(self, time: ExpiryT, value: T):
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds())
        return self._execute((b"SETEX", self.name, time, self.dumps(value)))

    def psetex(self, time: ExpiryT, value: T):
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds() * 1000)
        return self._execute((b"PSETEX", self.name, time, self.dumps(value)))

    # STRING COMMANDS
    # ---------------

    def append(self, value: T) -> int:
        return self._execute((b"APPEND", self.name, self.dumps(value)))

    def strlen(self) -> int:
        return self._execute((b"STRLEN", self.name))

    def substr(self, start: int, end: int = -1) -> str:
        return self._execute((b"SUBSTR", self.name, start, end)).decode()

    # TIME COMMANDS
    # -------------

    def expire(
        self,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds())
        items: list[EncodableT] = [b"EXPIRE", self.name, time]
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
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(when, dt.datetime):
            when = int(when.timestamp())
        items: list[EncodableT] = [b"EXPIREAT", self.name, when]
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
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(time, dt.timedelta):
            time = int(time.total_seconds() * 1000)
        items: list[EncodableT] = [b"PEXPIRE", self.name, time]
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
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        if isinstance(when, dt.datetime):
            when = int(when.timestamp() * 1000)
        items: list[EncodableT] = [b"PEXPIREAT", self.name, when]
        if nx:
            items.append(b"NX")
        if xx:
            items.append(b"XX")
        if gt:
            items.append(b"GT")
        if lt:
            items.append(b"LT")
        return self._execute(tuple(items))

    def ttl(self) -> int:
        return self._execute((b"TTL", self.name))

    def pttl(self) -> int:
        return self._execute((b"PTTL", self.name))

    def expiretime(self) -> int:
        return self._execute((b"EXPIRETIME", self.name))

    def pexpiretime(self) -> int:
        return self._execute((b"PEXPIRETIME", self.name))

    def persist(self) -> int:
        return self._execute((b"PERSIST", self.name))

    # MOTION COMMANDS
    # ---------------

    def exists(self) -> int:
        return self._execute((b"EXISTS", self.name))

    def delete(self) -> int:
        return self._execute((b"DEL", self.name))

    def getdel(self) -> None | T:
        return (
            None
            if (v := self._execute((b"GETDEL", self.name))) is None
            else self.loads(v)
        )

    def move(self, db: int):
        return self._execute((b"MOVE", self.name, db))

    def copy(
        self,
        destination: str,
        destination_db: int | None = None,
        replace: bool = False,
    ):
        params: list[EncodableT] = [b"COPY", self.name, destination]
        if destination_db is not None:
            params.extend((b"DB", destination_db))
        if replace:
            params.append(b"REPLACE")
        return self._execute(tuple(params))

    def dump(self) -> bytes:
        return self._execute((b"DUMP", self.name))

    def restore(
        self,
        ttl: float,
        value: bytes,
        replace: bool = False,
        absttl: bool = False,
        idletime: int | None = None,
        frequency: int | None = None,
    ):
        items: list[EncodableT] = [b"RESTORE", self.name, ttl, value]
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

    def type(self) -> str | None:
        return (
            None
            if (v := self._execute((b"TYPE", self.name))) == b"none"
            else v.decode()
        )

    def memory_usage(self, samples: int | None = None) -> int:
        args: list[EncodableT] = [b"MEMORY", b"USAGE", self.name]
        if samples is not None:
            args.extend((b"SAMPLES", samples))
        return self._execute(tuple(args))

    @overload
    def object(self, infotype: Literal["refcount", "idletime", "freq"]) -> int: ...

    @overload
    def object(self, infotype: Literal["encoding"]) -> str: ...

    def object(
        self,
        infotype: Literal["refcount", "encoding", "idletime", "freq"],
    ) -> int | str:
        res = self._execute((b"OBJECT", infotype, self.name))
        return res if infotype != "encoding" else res.decode()

    def debug_object(self):
        return self._execute((b"DEBUG", b"OBJECT", self.name))

    def unlink(self):
        return self._execute((b"UNLINK", self.name))
