<h1 align="center">🥔 CASHEWS 🥔</h1>

<p align="center">
    <em>Async cache utils with simple API to build fast and reliable applications</em>
</p>

```bash
pip install cashews
pip install cashews[redis]
```

---

## Why

Cache plays a significant role in modern applications and everybody want to use all power of async programming and cache.
There are a few advanced techniques with cache and async programming that can help you build simple, fast,
scalable and reliable applications. This library intends to make it easy to implement such techniques.

## Features

- Easy to configurate and use
- Decorator-based API, just decorate and play
- Different cache strategies out-of-the-box
- Support for multiple storage backends ([In-memory](#in-memory), [Redis](#redis))
- Client-side cache
- Different cache invalidation techniques (time-based and function-call based)
- Cache any objects securely with pickle (use [hash key](#redis))
- Cache usage API
- Stats for usage

## Usage Example

```python
from datetime import timedelta

from cashews import cache

cache.setup("mem://")  # configure as in-memory cache, but redis is also supported

# use a decorator-based API
@cache(ttl=timedelta(hours=3), key="user:{request.user.uid}")
async def long_running_function(request):
    ...

# or for fine-grained control, use it directly in a function
async def cache_using_function(request):
    await cache.set(key=request.user.uid, value=request.user, expire=60)
    ...
```

## Table of Contents

- [Configuration](#configuration)
- [Available Backends](#available-backends)
- [Basic API](#basic-api)
- [Strategies](#strategies)
- [Cache Invalidation](#cache-invalidation)
  - [Cache invalidation on code change](#cache-invalidation-on-code-change)
- [Detect the source of a result](#detect-the-source-of-a-result)

### Configuration

`cashews` provides a default cache, that you can setup in a two different ways:

```python
from cashews import cache

# via url
cache.setup("redis://0.0.0.0/?db=1&create_connection_timeout=0.5&safe=0&hash_key=my_secret&enable=1")
# or via kwargs
cache.setup("redis://0.0.0.0/", db=1, create_connection_timeout=0.5, safe=False, hash_key=b"my_key", enable=True)
```

Alternatively, you can create cache instance yourself:

```python
from cashews import Cache

cache = Cache()
cache.setup(...)
```

Optionally, you can disable cache with `enable` parameter:

```python
cache.setup("redis://redis/0?enable=1")
cache.setup("mem://?size=500", enable=False)
cache.setup("redis://redis?enable=True")
```

### Available Backends

#### In-memory

In-memory cache uses fixed-sized LRU dict to store values. It checks expiration on `get`
and periodically purge expired keys.

```python
cache.setup("mem://")
cache.setup("mem://?check_interval=10&size=10000")
```

#### Redis

*Requires [aioredis](https://github.com/aio-libs/aioredis) package.*

This will use Redis as a storage.

This backend uses [pickle](https://docs.python.org/3/library/pickle.html) module to store
values, but the cashes can store values with sha1-keyed hash.
Use `hash_key` parameter to protect your application from security vulnerabilities.

To supress any connections errors use `safe` parameter.

You can set parameters for [redis pool](https://aioredis.readthedocs.io/en/v1.3.0/api_reference.html#aioredis.create_pool)
with `minsize` or `maxsize` parameters.

If you would like to use [client-side cache](https://redis.io/topics/client-side-caching) set `client_side=True`

```python
cache.setup("redis://0.0.0.0/?db=1&minsize=10&safe=0&hash_key=my_secret", prefix="func")
cache.setup("redis://0.0.0.0/?db=2", hash_key=None, prefix="super", index_name="user", index_field="user_uid")
cache.setup("redis://0.0.0.0/", db=1, password="my_pass", create_connection_timeout=0.1, safe=1, hash_key="my_secret", client_side=True)
```

### Basic API

There are few basic methods to work with cache:

```python
from cashews import cache

cache.setup("mem://")  # configure as in-memory cache

await cache.set(key="key", value={"any": True}, expire=60, exist=None)  # -> bool
await cache.get("key")  # -> Any
await cache.get_many("key1", "key2")
await cache.incr("key") # -> int
await cache.delete("key")
await cache.expire("key", timeout=10)
await cache.get_expire("key")  # -> int seconds to expire
await cache.ping(message=None)  # -> bytes
await cache.clear()
await cache.is_locked("key", wait=60)  # -> bool
async with cache.lock("key", expire=10):
   ...
await cache.set_lock("key", value="value", expire=60)  # -> bool
await cache.unlock("key", "value")  # -> bool
```

### Strategies

- [Simple cache](#simple-cache)
- [Fail cache (Failover cache)](#fail-cache-failover-cache)
- [Hit cache](#hit-cache)
- [Performance downgrade detection](#performance-downgrade-detection)
- [Locked](#locked)
- [Early](#early)
- [Rate limit](#rate-limit)
- [Circuit breaker](#circuit-breaker)

#### Simple cache

This is typical cache strategy: execute, store and return from cache until it expired.

```python
from datetime import timedelta

from cashews import cache

@cache(ttl=timedelta(hours=3), key="user:{request.user.uid}")
async def long_running_function(request):
    ...
```

#### Fail cache (Failover cache)

Return cache result, if one of the given exceptions is raised (at least one function
call should be succeed prior that).

```python
from cashews import cache  # or: from cashews import fail

# note: the key will be "__module__.get_status:name:{name}"
@cache.fail(ttl=timedelta(hours=2), exceptions=(ValueError, MyException))  
async def get_status(name):
    value = await api_call()
    return {"status": value}
```

#### Hit cache

Expire cache after given numbers of call `cache_hits`.

```python
from cashews import cache  # or: from cashews import hit

@cache.hit(ttl=timedelta(hours=2), cache_hits=100, update_before=2)
async def get(name):
    ...
```

#### Performance downgrade detection

Trace time execution of target and throw exception if it downgrades to given condition

```python
from cashews import cache   # or: from cashews import perf

@cache.perf(ttl=timedelta(hours=2))
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Locked

Decorator that can help you to solve [Cache stampede problem](https://en.wikipedia.org/wiki/Cache_stampede).
Lock following function calls until the first one will be finished.
This guarantees exactly one function call for given ttl.

```python
from cashews import cache  # or: from cashews import locked

@cache.locked(ttl=timedelta(minutes=10))
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Early

Cache strategy that tries to solve [Cache stampede problem](https://en.wikipedia.org/wiki/Cache_stampede)
with a hot cache recalculating result in a background.

```python
from cashews import cache  # or: from cashews import early

# if you call this function after 7 min, cache will be updated in a background 
@cache.early(ttl=timedelta(minutes=10), early_ttl=timedelta(minutes=7))  
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Rate limit

Rate limit for a function call - do not call a function if rate limit is reached

```python
from cashews import cache  # or: from cashews import rate_limit

# no more than 10 calls per minute or ban for 10 minutes
@cache.rate_limit(limit=10, period=timedelta(minutes=1), ttl=timedelta(minutes=10))
async def get(name):
    return {"status": value}
```

#### Circuit breaker

Circuit breaker

```python
from cashews import cache  # or: from cashews import circuit_breaker

@cache.circuit_breaker(errors_rate=10, period=timedelta(minutes=1), ttl=timedelta(minutes=5))
async def get(name):
    ...
```

### Cache invalidation

Cache invalidation - one of the main Computer Science well known problem.
That's why `ttl` is a required parameter for all cache decorators.

Sometimes, you want to invalidate cache after some action is triggered.
Consider this example:

```python
from datetime import timedelta

from cashews import cache

@cache(ttl=timedelta(days=1))
async def user_items(user_id, fresh=False):
    ...

@cache(ttl=timedelta(hours=3))
async def items(page=1):
    ...

@cache.invalidate("module:items:page:*")  # or: @cache.invalidate(items)
@cache.invalidate(user_items, {"user_id": lambda user: user.id}, defaults={"fresh": True})
async def create_item(user):
   ...
```

Here, cache for `user_items` and `items` will be invalidated every time `create_item` is called.

#### Cache invalidation on code change

Often, you may face a problem with invalid cache after code is changed. For example:

```python
@cache(ttl=timedelta(days=1), key="user:{user_id}")
async def get_user(user_id):
    return {"name": "Dmitry", "surname": "Krykov"}
```

Then, returned value was changed to:

```bash
-    return {"name": "Dmitry", "surname": "Krykov"}
+    return {"full_name": "Dmitry Krykov"}
```

Since function returning a dict, there is no way simple way to automatically detect
that kind of cache invalidity

One way to solve the problem is to add a prefix for this cache:

```python
@cache(ttl=timedelta(days=1), prefix="v2")
async def get_user(user_id):
    return {"full_name": "Dmitry Krykov"}
```

but it is so easy to forget to do it...

The best defense against this problem is to use your own datacontainers, like
[dataclasses](https://docs.python.org/3/library/dataclasses.html),
with defined `__repr__` method.
This will add distinctness and `cashews` can detect changes in such structures automatically
by checking [object representation](https://docs.python.org/3/reference/datamodel.html#object.__repr__).

```python
from dataclasses import dataclass

from cashews import cache

@dataclass
class User:
    name: str
    surname: str

# or define your own class with __repr__ method

class User:
    
    def __init__(self, name, surname):
        self.name, self.surname = name, surname
        
    def __repr__(self):
        return f"{self.name} {self.surname}"

# Will detect changes of a structure
@cache(ttl=timedelta(days=1), prefix="v2")
async def get_user(user_id):
    return User("Dima", "Krykov")
```

### Detect the source of a result

Decorators give us a very simple API but also make it difficult to understand where
result is coming from - cache or direct call.

To solve this problem `cashews` has `context_cache_detect` context manager:

```python
from cashews import context_cache_detect

with context_cache_detect as detector:
    response = await decorated_function()
    keys = detector.get()
print(keys)
# >>> {"my:key": [{"ttl": 10, "name": "simple", "backend": "redis"}, ], "fail:key": [{"ttl": timedelta(hours=10), "exc": RateLimit}, "name": "fail", "backend": "mem"],}
```

or you can use `CacheDetect` class:

```python
from cashews import CacheDetect

cache_detect = CacheDetect()
await func(_from_cache=cache_detect)
assert cache_detect.keys == {}

await func(_from_cache=cache_detect)
assert len(cache_detect.keys) == 1
```

A simple middleware to use it in a web app:

```python
@app.middleware("http")
async def add_from_cache_headers(request: Request, call_next):
    with context_cache_detect as detector:
        response = await call_next(request)
        if detector.keys:
            key = list(detector.keys.keys())[0]
            response.headers["X-From-Cache"] = key
            expire = await mem.get_expire(key)
            if expire == -1:
                expire = await cache.get_expire(key)
            response.headers["X-From-Cache-Expire-In-Seconds"] = str(expire)
            if "exc" in detector.keys[key]:
                response.headers["X-From-Cache-Exc"] = str(detector.keys[key]["exc"])
    return response
```

- https://www.datadoghq.com/blog/how-to-monitor-redis-performance-metrics/
- Redis with https://github.com/NoneGG/aredis
