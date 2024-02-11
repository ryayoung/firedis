# Firedis

```
pip install firedis
```

A faster, type-safe Python client for your **local** Redis server. Optimized for ultra high
frequency, synchronous transactions over a **Unix socket, or localhost**

**130,000+** synchronous transactions per second while serializing/deserializing any Python
object in/out of Redis.


```py
import datetime as dt
from firedis import Firedis, Namespace

class MyRedis(Firedis):
    login_times: Namespace[dt.datetime]
    numbers: Namespace[int | float]
    documents: Namespace[dict[str, str]]


r = MyRedis()  # Takes same arguments as `redis.Redis()`

r.login_times.set("Peter", dt.datetime.now())
r.numbers.set('foo', 1)
r.documents.set('bar', {'a': 'A', 'b': 'B'})
```

## Static Type Safety, and direct storage of Python objects

- ...

## Performance

- ...

## Mirrors `redis.Redis()` API

- ...

