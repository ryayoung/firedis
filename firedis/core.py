from typing import get_type_hints
import os
from redis import Redis, ConnectionError, RedisError
from firedis.common import _BaseNamespace


class _FiredisMeta(type):
    def __new__(mcs, name, bases, attrs, slots: bool = True):
        # If they want to use slots, combine existing slots, annotation names from the
        # current class, as well as slots and annotation names from all bases

        if slots:
            all_slots = set(attrs.pop("__slots__", ()))

            for name in attrs.get("__annotations__", {}).keys():
                all_slots.add(name)

            for base in bases:
                base_slots = getattr(base, "__slots__", ())
                all_slots.update(
                    [base_slots] if isinstance(base_slots, str) else base_slots
                )

                all_slots.update(get_type_hints(base).keys())

            attrs["__slots__"] = list(all_slots)

        cls = super().__new__(mcs, name, bases, attrs)

        # Find the type hints that are subclasses of Namespace. We will instantiate
        # them in `__init__`
        namespaces: dict[str, type[_BaseNamespace]] = {
            name: tp.__origin__
            for name, tp in get_type_hints(cls).items()
            if issubclass(getattr(tp, "__origin__", int), _BaseNamespace)
        }
        if "redis" in namespaces:
            raise ValueError("Invalid namespace name: 'redis'")

        def __init__(self, *args, **kwargs):
            if "single_connection_client" not in kwargs:
                kwargs["single_connection_client"] = True

            if not "unix_socket_path" in kwargs:
                if should_be_using_unix_socket(kwargs.get("host"), kwargs.get("port")):
                    if can_connect_via_unix_socket():
                        kwargs["unix_socket_path"] = "/tmp/redis.sock"
                    else:
                        print(USE_UNIX_SOCKET_MESSAGE)

            self.redis = Redis(*args, **kwargs)
            for name, tp in namespaces.items():
                setattr(self, name, tp(name, self.redis))

        cls.__init__ = __init__
        return cls


class Firedis(metaclass=_FiredisMeta):
    """
    A redis client with type-safe namespaces.
    """

    redis: Redis
    # This is how we trick the type checker into knowing that __init__ takes the same
    # arguments as redis.Redis. We will define __init__ dynamically in the metaclass though.
    __init__: type[Redis]  # type:ignore


def can_connect_via_unix_socket() -> bool:
    try:
        test_redis = Redis(unix_socket_path="/tmp/redis.sock")
        return bool(test_redis.ping())
    except (ConnectionError, RedisError):
        return False


def should_be_using_unix_socket(host: str | None, port: str | None) -> bool:
    is_using_localhost = host in ["127.0.0.1", "localhost", None]
    is_using_default_port = port in [6379, None]
    is_not_windows = os.name != "nt"
    return is_using_localhost and is_using_default_port and is_not_windows


USE_UNIX_SOCKET_MESSAGE = """\
INFO: You should be using a unix socket to connect to redis. It can be 2-3x faster \
than an IP connection. To enable it, add `unixsocket /tmp/redis.sock` and `unixsocketperm 700` \
to your redis.conf file, or pass `--unixsocket /tmp/redis.sock --unixsocketperm 700` \
to the redis-server command used to start the server. To disable this warning and continue \
using localhost anyway, pass `unix_socket_path=None`.
"""
