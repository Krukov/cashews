<h1 align="center">🥔 CASHEWS 🥔</h1>

<p align="center">
    <em>Async cache framework with simple API to build fast and reliable applications</em>
</p>

```bash
pip install cashews
pip install cashews[redis]
pip install cashews[diskcache]
pip install cashews[dill] # can cache in redis more types of objects
pip install cashews[speedup] # for bloom filters
```

---

## Why

Cache plays a significant role in modern applications and everybody want to use all power of async programming and cache.
There are a few advanced techniques with cache and async programming that can help you build simple, fast,
scalable and reliable applications. This library intends to make it easy to implement such techniques.

## Features

- Easy to configure and use
- Decorator-based API, just decorate and play
- Different cache strategies out-of-the-box
- Support for multiple storage backends ([In-memory](#in-memory), [Redis](#redis), [DiskCache](diskcache))
- Set TTL as a string ("2h5m"), as `timedelta` or use a function in case TTL depends on key parameters
- Transactionality
- Middlewares
- Client-side cache (10x faster than simple cache with redis)
- Bloom filters
- Different cache invalidation techniques (time-based or tags)
- Cache any objects securely with pickle (use [hash key](#redis))
- 2x faster then `aiocache` (with client side caching)

## Usage Example

```python
from cashews import cache

cache.setup("mem://")  # configure as in-memory cache, but redis/diskcache is also supported

# use a decorator-based API
@cache(ttl="3h", key="user:{request.user.uid}")
async def long_running_function(request):
    ...

# or for fine-grained control, use it directly in a function
async def cache_using_function(request):
    await cache.set(key=request.user.uid, value=request.user, expire="20h")
    ...
```

More examples [here](https://github.com/Krukov/cashews/tree/master/examples)

## Table of Contents

- [Configuration](#configuration)
- [Available Backends](#available-backends)
- [Basic API](#basic-api)
- [Disable Cache](#disable-cache)
- [Strategies](#strategies)
  - [Cache condition](#cache-condition)
  - [Keys templating](#template-keys)
  - [TTL](#ttl)
  - [What can be cached](#what-can-be-cached)
- [Cache Invalidation](#cache-invalidation)
  - [Cache invalidation on code change](#cache-invalidation-on-code-change)
- [Detect the source of a result](#detect-the-source-of-a-result)
- [Middleware](#middleware)
- [Transactional mode](#transactional)

### Configuration

`cashews` provides a default cache, that you can setup in two different ways:

```python
from cashews import cache

# via url
cache.setup("redis://0.0.0.0/?db=1&socket_connect_timeout=0.5&suppress=0&hash_key=my_secret&enable=1")
# or via kwargs
cache.setup("redis://0.0.0.0/", db=1, wait_for_connection_timeout=0.5, suppress=False, hash_key=b"my_key", enable=True)
```

Alternatively, you can create cache instance yourself:

```python
from cashews import Cache

cache = Cache()
cache.setup(...)
```

Optionally, you can disable cache with `disable`/`enable` parameter (see [Disable Cache](#disable-cache)):

```python
cache.setup("redis://redis/0?enable=1")
cache.setup("mem://?size=500", disable=True)
cache.setup("mem://?size=500", enable=False)
```

You can setup different Backends based on a prefix:

```python
cache.setup("redis://redis/0")
cache.setup("mem://?size=500", prefix="user")

await cache.get("accounts")  # will use the redis backend
await cache.get("user:1")  # will use the memory backend
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

_Requires [redis](https://github.com/redis/redis-py) package._\

This will use Redis as a storage.

This backend uses [pickle](https://docs.python.org/3/library/pickle.html) module to serialize
values, but the cashes can store values with sha1-keyed hash.

Use `secret` and `digestmod` parameter to protect your application from security vulnerabilities.

The `digestmod` is a hashing algorithm that can be used: `sum`, `md5` (default), `sha1` and `sha256`

The `secret` is a salt for a hash.

Pickle can't serialize any type of objects. In case you need to store more complex types

you can use [dill](https://github.com/uqfoundation/dill) - set `pickle_type="dill"`.
Dill is great, but less performance.
If you need complex serializer for [sqlalchemy](https://docs.sqlalchemy.org/en/14/core/serializer.html) objects you can set `pickle_type="sqlalchemy"`

Any connections errors are suppressed, to disable it use `suppress=False` - a `CacheBackendInteractionError` will be raised

If you would like to use [client-side cache](https://redis.io/topics/client-side-caching) set `client_side=True`

Client side cache will add `cashews:` prefix for each key, to customize it use `client_side_prefix` option.

```python
cache.setup("redis://0.0.0.0/?db=1&minsize=10&suppress=false&hash_key=my_secret", prefix="func")
cache.setup("redis://0.0.0.0/2", password="my_pass", socket_connect_timeout=0.1, retry_on_timeout=True, hash_key="my_secret")
cache.setup("redis://0.0.0.0", client_side=True, client_side_prefix="my_prefix:", pickle_type="dill")
```

For using secure connections to redis (over ssl) uri should have `rediss` as schema

```python
cache.setup("rediss://0.0.0.0/", ssl_ca_certs="path/to/ca.crt", ssl_keyfile="path/to/client.key",ssl_certfile="path/to/client.crt",)
```

#### DiskCache

_Requires [diskcache](https://github.com/grantjenks/python-diskcache) package._

This will use local sqlite databases (with shards) as storage.

It is a good choice if you don't want to use redis, but you need a shared storage, or your cache takes a lot of local memory.
Also, it is good choice for client side local storage.

You cat setup disk cache with [FanoutCache parameters](http://www.grantjenks.com/docs/diskcache/api.html#fanoutcache)

** Warning ** `cache.scan` and `cache.get_match` does not work with this storage (works only if shards are disabled)

```python
cache.setup("disk://")
cache.setup("disk://?directory=/tmp/cache&timeout=1&shards=0")  # disable shards
Gb = 1073741824
cache.setup("disk://", size_limit=3 * Gb, shards=12)
```

### Basic API

There are few basic methods to work with cache:

```python
from cashews import cache

cache.setup("mem://")  # configure as in-memory cache

await cache.set(key="key", value=90, expire=60, exist=None)  # -> bool
await cache.set_raw(key="key", value="str")  # -> bool

await cache.get("key", default=None)  # -> Any
await cache.get_raw("key")
await cache.get_many("key1", "key2", default=None)
async for key, value in cache.get_match("pattern:*", batch_size=100):
    ...

await cache.incr("key") # -> int

await cache.delete("key")
await cache.delete_many("key1", "key2")
await cache.delete_match("pattern:*")

async for key in cache.scan("pattern:*"):
    ...

await cache.expire("key", timeout=10)
await cache.get_expire("key")  # -> int seconds to expire

await cache.ping(message=None)  # -> bytes
await cache.clear()

await cache.is_locked("key", wait=60)  # -> bool
async with cache.lock("key", expire=10):
    ...
await cache.set_lock("key", value="value", expire=60)  # -> bool
await cache.unlock("key", "value")  # -> bool

await cache.get_keys_count()  # -> int - total number of keys in cache
await cache.close()
```

### Disable Cache

Cache can be disabled not only at setup, but also in runtime. Cashews allow you to disable/enable any call of cache or specific commands:

```python
from cashews import cache, Command

cache.setup("mem://")  # configure as in-memory cache

cache.disable(Command.DELETE)
cache.disable()
cache.enable(Command.GET, Command.SET)
cache.enable()

with cache.disabling():
  ...
```

### Strategies

- [Simple cache](#simple-cache)
- [Fail cache (Failover cache)](#fail-cache-failover-cache)
- [Hit cache](#hit-cache)
- [Early](#early)
- [Soft](#soft)
- [Async Iterators](#iterators)
- [Locked](#locked)
- [Rate limit](#rate-limit)
- [Circuit breaker](#circuit-breaker)

#### Simple cache

This is typical cache strategy: execute, store and return from cache until it expired.

```python
from datetime import timedelta
from cashews import cache

cache.setup("mem://")

@cache(ttl=timedelta(hours=3), key="user:{request.user.uid}")
async def long_running_function(request):
    ...
```

#### Fail cache (Failover cache)

Return cache result, if one of the given exceptions is raised (at least one function
call should be succeed prior that).

```python
from cashews import cache

cache.setup("mem://")

# note: the key will be "__module__.get_status:name:{name}"
@cache.failover(ttl="2h", exceptions=(ValueError, MyException))
async def get_status(name):
    value = await api_call()
    return {"status": value}
```

If exceptions didn't get will catch all exceptions or use default if it set by:

```python
cache.set_default_fail_exceptions(ValueError, MyException)
```

#### Hit cache

Expire cache after given numbers of call `cache_hits`.

```python
from cashews import cache

cache.setup("mem://")

@cache.hit(ttl="2h", cache_hits=100, update_after=2)
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
@cache.early(ttl="10m", early_ttl="7m")
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Soft

Like a simple cache, but with a fail protection base on soft ttl.

```python
from cashews import cache

cache.setup("mem://")

# if you call this function after 7 min, cache will be updated and return a new result.
# If it fail on recalculation will return current cached value (if it not more then 10 min old)
@cache.soft(ttl="10m", soft_ttl="7m")
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Iterators

All upper decorators can be use only with coroutines. Cashing async iterators works differently.
To cache async iterators use `iterator` decorator

```python
from cashews import cache

cache.setup("mem://")


@cache.iterator(ttl="10m", key="get:{name}")
async def get(name):
    async for item in get_pages(name):
        yield ...

```

#### Locked

Decorator that can help you to solve [Cache stampede problem](https://en.wikipedia.org/wiki/Cache_stampede).
Lock following function calls until the first one will be finished.
This guarantees exactly one function call for given ttl.

> :warning: \*\*Warning: this decorator will not cache the result
> To do it you can combine this decorator with any cache decorator or use parameter `lock=True` with `@cache()`

```python
from cashews import cache

cache.setup("mem://")

@cache.locked(ttl="10s")
async def get(name):
    value = await api_call()
    return {"status": value}
```

#### Rate limit

Rate limit for a function call: if rate limit is reached raise an `RateLimitError` exception.

> :warning: \*\*Warning: this decorator will not cache the result
> To do it you can combine this decorator with any cache failover decorator`

```python
from cashews import cache, RateLimitError

cache.setup("mem://")

# no more than 10 calls per minute or ban for 10 minutes - raise RateLimitError
@cache.rate_limit(limit=10, period="1m", ttl="10m")
async def get(name):
    value = await api_call()
    return {"status": value}



# no more than 100 calls in 10 minute window. if rate limit will rich -> return from cache
@cache.failover(ttl="10m", exceptions=(RateLimitError, ))
@cache.slice_rate_limit(limit=100, period="10m")
async def get_next(name):
    value = await api_call()
    return {"status": value}

```

#### Circuit breaker

Circuit breaker pattern. Count number of failed calls and if error rate rich specified value will raise `CircuitBreakerOpen` exception

> :warning: \*\*Warning: this decorator will not cache the result
> To do it you can combine this decorator with any cache failover decorator`

```python
from cashews import cache, CircuitBreakerOpen

cache.setup("mem://")

@cache.circuit_breaker(errors_rate=10, period="1m", ttl="5m")
async def get(name):
    ...


@cache.failover(ttl="10m", exceptions=(CircuitBreakerOpen, ))
@cache.circuit_breaker(errors_rate=10, period="10m", ttl="5m", half_open_ttl="1m")
async def get_next(name):
    ...

```

#### Bloom filter (experimental)

Simple Bloom filter:

```python
from cashews import cache

cache.setup("mem://")

@cache.bloom(capacity=10_000, false_positives=1)
async def email_exists(email: str) -> bool:
    ...

for email in all_users_emails:
    await email_exists.set(email)

await email_exists("example@example.com")
```

### Cache condition

By default, any successful result of function call is stored, even it is a `None`.
Caching decorators have the parameter - `condition`, that can be:

- a callable object that receive a result of function call or an exception, args, kwargs and a cache key
- a string: "not_none" or "skip_none" to do not cache `None` values in

```python
from cashews import cache, NOT_NONE

cache.setup("mem://")

@cache(ttl="1h", condition=NOT_NONE)
async def get():
    ...


def skit_test_result(result, args, kwargs, key=None) -> bool:
    return result and result != "test"

@cache(ttl="1h", condition=skit_test_result)
async def get():
    ...

```

It is also possible to cache an exception that function can raise, to do so use special conditions (only for simple, hit and early)

```python
from cashews import cache, with_exceptions, only_exceptions

cache.setup("mem://")

@cache(ttl="1h", condition=with_exceptions(MyException, TimeoutError))
async def get():
    ...


@cache(ttl="1h", condition=only_exceptions(MyException, TimeoutError))
async def get():
    ...

```

Also caching decorators have parameter `time_condition` - min latency in seconds (can be set like `ttl`)
of getting a result of function call to be cached.

```python
from cashews import cache

cache.setup("mem://")

@cache(ttl="1h", time_condition="3s")  # to cache for 1 hour if execution takes more than 3 seconds
async def get():
    ...
```

### Template Keys

Often, to compose a key, you need all the parameters of the function call.
By default, Cashews will generate a key using the function name, module names and parameters

```python
from cashews import cache

cache.setup("mem://")

@cache(ttl=timedelta(hours=3))
async def get_name(user, *args, version="v1", **kwargs):
    ...

# a key template will be "__module__.get_name:user:{user}:{__args__}:version:{version}:{__kwargs__}"

await get_name("me", version="v2")
# a key will be "__module__.get_name:user:me::version:v2"
await get_name("me", version="v1", foo="bar")
# a key will be "__module__.get_name:user:me::version:v1:foo:bar"
await get_name("me", "opt", "attr", opt="opt", attr="attr")
# a key will be "__module__.get_name:user:me:opt:attr:version:v1:attr:attr:opt:opt"
```

The same with a class method

```python
from cashews import cache

cache.setup("mem://")

class MyClass:

    @cache(ttl="2h")
    async def get_name(self, user, version="v1"):
         ...

# a key template will be "__module__:MyClass.get_name:self:{self}:user:{user}:version:{version}

await MyClass().get_name("me", version="v2")
# a key will be "__module__:MyClass.get_name:self:<__module__.MyClass object at 0x105edd6a0>:user:me:version:v1"
```

As you can see, there is an ugly reference to the instance in the key. That is not what we expect to see.
That cache will not work properly. There are 3 solutions to avoid it:

1. define `__str__` magic method in our class

```python

class MyClass:

    @cache(ttl="2h")
    async def get_name(self, user, version="v1"):
         ...

    def __str__(self) -> str:
        return self._host

await MyClass(host="http://example.com").get_name("me", version="v2")
# a key will be "__module__:MyClass.get_name:self:http://example.com:user:me:version:v1"
```

2. Set a key template

```python
class MyClass:

    @cache(ttl="2h", key="{self._host}:name:{user}:{version}")
    async def get_name(self, user, version="v1"):
         ...

await MyClass(host="http://example.com").get_name("me", version="v2")
# a key will be "http://example.com:name:me:v1"
```

3. Use `noself` or `noself_cache` if you want to exclude `self` from a key

```python
from cashews import cache, noself, noself_cache

cache.setup("mem://")

class MyClass:

    @noself(cache)(ttl="2h")
    async def get_name(self, user, version="v1"):
         ...

    @noself_cache(ttl="2h")  # for python <= 3.8
    async def get_name(self, user, version="v1"):
         ...
# a key template will be "__module__:MyClass.get_name:user:{user}:version:{version}

await MyClass().get_name("me", version="v2")
# a key will be "__module__:MyClass.get_name:user:me:version:v1"
```

Sometimes you may need to format the parameters or define your
own template for the key and Cashews allows you to do this:

```python
from cashews import default_formatter, cache

cache.setup("mem://")

@cache.failover(key="name:{user.uid}")
async def get_name(user, version="v1"):
    ...

await get_name(user, version="v2")
# a key will be "fail:name:me"

@cache.hit(key="user:{token:jwt(user_name)}", prefix="new")
async def get_name(token):
    ...

await get_name(".....")
# a key will be "new:user:alex"


@default_formatter.register("upper")
def _upper(value):
    return value.upper()

@default_formatter.type_format(Decimal)
def _decimal(value: Decimal) -> str:
    return value.quantize(Decimal("0.00"))


@cache(key="price-{item.price}:{item.currency:upper}")
async def get_price(item):
    ...

await get_name(item)
# a key will be "price-10.00:USD"
```

### TTL

Cache time to live (`ttl`) is a required parameter for all cache decorators. TTL can be:

- an integer as numbers of seconds
- a `timedelta`
- a string like in golang e.g `1d2h3m50s`
- a callable object like a function that receive `args` and `kwargs` of the decorated function and return one of previous format for TTL

Examples:

```python
from cashews import cache
from datetime import timedelta

cache.setup("mem://")

@cache(ttl=60 * 10)
async def get(item_id: int) -> Item:
    pass

@cache(ttl=timedelta(minutes=10))
async def get(item_id: int) -> Item:
    pass

@cache(ttl="10m")
async def get(item_id: int) -> Item:
    pass

def _ttl(item_id: int) -> str:
    return "2h" if item_id > 10 else "1h"

@cache(ttl=_ttl)
async def get(item_id: int) -> Item:
    pass
```

### What can be cached

Cashews mostly use built-in pickle to store a data, but also support others pickle like serialization like dill.
Some types of objects are not picklable, in this case cashews have api to define custom encoding/decoding:

```python
from cashews.serialize import register_type


async def my_encoder(value: CustomType, *args, **kwargs) -> bytes:
    ...


async def my_decoder(value: bytes, *args, **kwargs) -> CustomType:
    ...


register_type(CustomType, my_encoder, my_decoder)
```

### Cache invalidation

Cache invalidation - one of the main Computer Science well known problem.

Sometimes, you want to invalidate the cache after some action is triggered.
Consider this example:

```python
from cashews import cache

cache.setup("mem://")

@cache(ttl="1h", key="items:page:{page}")
async def items(page=1):
    ...

@cache.invalidate("items:page:*")
async def create_item(item):
   ...
```

Here, the cache for `items` will be invalidated every time `create_item` is called
There are two problems:

1. with redis backend you cashews will scan a full database to get a keys that match a pattern (`items:page:*`) - not good for performance reasons
2. what if we do not specify a key for cache:

```python
@cache(ttl="1h")
async def items(page=1):
    ...
```

Cashews provide the tag system: you can tag cache keys, so they will be stored in a separate [SET](https://redis.io/docs/data-types/sets/)
to avoid high load on redis storage. To use the tags in a more efficient ways please use it with the client side feature

```python
from cashews import cache

cache.setup("redis://", client_side=True)

@cache(ttl="1h", tags=["items", "page:{page}"])
async def items(page=1):
    ...


await cache.delete_tags("page:1")
await cache.delete_tags("items")

# low level api
cache.register_tag("my_tag", key_template="key{i}")

await cache.set("key1", "value", expire="1d", tags=["my_tag"])
```

You can invalidate future call of cache request by context manager:

```python
from cashews import cache, invalidate_further

@cache(ttl="3h")
async def items():
    ...

async def add_item(item: Item) -> List[Item]:
    ...
    with invalidate_further():
        await items
```

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

cache.setup("mem://")

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
@cache(ttl="1d", prefix="v2")
async def get_user(user_id):
    return User("Dima", "Krykov")
```

### Detect the source of a result

Decorators give us a very simple API but also make it difficult to understand where
result is coming from - cache or direct call.

To solve this problem `cashews` has `detect` context manager:

```python
from cashews import cache

with cache.detect as detector:
    response = await something_that_use_cache()
    calls = detector.calls

print(calls)
# >>> {"my:key": [{"ttl": 10, "name": "simple", "backend": "redis"}, ], "fail:key": [{"ttl": 10, "exc": RateLimit}, "name": "fail", "backend": "mem"],}
```

E.g. A simple middleware to use it in a web app:

```python
@app.middleware("http")
async def add_from_cache_headers(request: Request, call_next):
    with cache.detect as detector:
        response = await call_next(request)
        if detector.keys:
            key = list(detector.keys.keys())[0]
            response.headers["X-From-Cache"] = key
            expire = await cache.get_expire(key)
            response.headers["X-From-Cache-Expire-In-Seconds"] = str(expire)
    return response
```

### Middleware

Cashews provide the interface for a "middleware" pattern:

```python
import logging
from cashews import cache

logger = logging.getLogger(__name__)


async def logging_middleware(call, cmd: Command, backend: Backend, *args, **kwargs):
    key = args[0] if args else kwargs.get("key", kwargs.get("pattern", ""))
    logger.info("=> Cache request: %s ", cmd.value, extra={"args": args, "cache_key": key})
    return await call(*args, **kwargs)


cache.setup("mem://", middlewares=(logging_middleware, ))
```

### Transactional

Applications more often based on database with transaction (OLTP) usage. Usually cache support transactions poorly.
Here just simple example how we can make our cache inconsistent:

```python
async def my_handler():
    async with db.transaction():
        await db.insert(user)
        await cache.set(f"key:{user.id}", user)
        await api.service.register(user)
```

Here the api call may fail, the database transaction will rollback, but the cache will not.
Of course, at this code we can solve it by moving the cache call outside transaction, but in real code it may not so easy.
Another case: we want to make bulk operations with group of keys to keep it consistent:

```python
async def login(user, token, session):
    ...
    old_session = await cache.get(f"current_session:{user.id}")
    await cache.incr(f"sessions_count:{user.id}")
    await cache.set(f"current_session:{user.id}", session)
    await cache.set(f"token:{token.id}", user)
    return old_session
```

Here we want to have some way to protect our code from race conditions and do operations with cache simultaneously.

Cashews support transaction operations:

> :warning: \*\*Warning: transaction operations are `set`, `set_many`, `delete`, `delete_many`, `delete_match` and `incr`

```python
from cashews import cache
...

@cache.transaction()
async def my_handler():
    async with db.transaction():
        await db.insert(user)
        await cache.set(f"key:{user.id}", user)
        await api.service.register(user)

# or
async def login(user, token, session):
    async with cache.transaction() as tx:
        old_session = await cache.get(f"current_session:{user.id}")
        await cache.incr(f"sessions_count:{user.id}")
        await cache.set(f"current_session:{user.id}", session)
        await cache.set(f"token:{token.id}", user)
        if ...:
            tx.rollback()
    return old_session

```

Transactions in cashews support different mode of "isolation"

- fast (0-7% overhead) - memory based, can't protect of race conditions, but may use for atomicity
- locked (default - 4-9% overhead) - use kind of shared lock per cache key (in case of redis or disk backend), protect of race conditions
- serializable (7-50% overhead) - use global shared lock - one transaction per time (almost useless)

```python
from cashews import cache, TransactionMode
...

@cache.transaction(TransactionMode.SERIALIZABLE, timeout=1)
async def my_handler():
   ...
```

## Development

### Setup

- Clone the project.
- After creating a virtual environment, install [pre-commit](https://pre-commit.com/):
  ```shell
  pip install pre-commit && pre-commit install --install-hooks
  ```

### Tests

To run tests you can use `tox`:

```shell
pip install tox
tox -e py  // tests for inmemory backend
tox -e py-diskcache  // tests for diskcache backend
tox -e py-redis  // tests for redis backend  - you need to run redis
tox -e py-integration  // tests for integrations with aiohttp and fastapi

tox // to run all tests for all python that is installed on your machine
```

Or use `pytest`, but 2 tests always fail, it is OK:

```shell
pip install .[tests,redis,diskcache,speedup] fastapi aiohttp requests

pytest // run all tests with all backends
pytest -m "not redis" // all tests without tests for redis backend
```
