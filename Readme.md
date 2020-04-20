CASHEWS 🥔
=========

Async cache utils with simple api to build fast and reliable applications
-------------------------------------------------------------------------

    pip install cashews[redis]


Why
---

Cache plays significant role in modern applications and everybody wanna use all power of async programming and cache..
There are a few advance techniques with cache and async programming that can help you to build simple, fast,
 scalable and reliable applications. Caches


# Features
- Decorator base api, just decorate and play
- Cache invalidation by time, 'ttl' is a required parameter to avoid storage overflow and endless cache
- Support Multi backend ([Memory](#memory), [Redis](#redis), memcache by request)
- Can cache any objects securely with pickle (use [hash key](#redis)). 
- Simple configuring and API
- cache invalidation autosystem and API 
- Cache usage detection API

## API
- [simple cache](#simple-cache)
- [fail cache](#fail-cache)
- [hit-rate cache](#hit-cache)
- [perf-rate cache](#performance-downgrade-detection)
- [rate-limit](#rate-limit)
- [cache with early expiration/rebuilding](#early)
- [lock](#locked) 
- [circuit](#circuit-breaker) 
- [api for key storage/backend](#basic-api)
- [auto invalidation](#invalidation)
- [detect cache usage](#detect-source-of-a-result)

Usage
-----

### Configure
Cache object is a single object that can be configured in one place by url::

```python
from cashews import cache

cache.setup("redis://0.0.0.0/?db=1&create_connection_timeout=0.5&safe=0&hash_key=my_sicret&enable=1")
or
cache.setup("redis://0.0.0.0/", db=1, create_connection_timeout=0.5, safe=False, hash_key=b"my_key", enable=True)
or
cache.setup("mem://") # for inmemory cache
```
if you dont like global objects or prefer more manageable way you can work with cache class 
```python

from cashews import Cache

cache = Cache()
cache.setup("mem://?size=500")
```

You can disable cache by 'enable' parameter:
```python

cache.setup("mem://?size=500", enable=False)
cache.setup("redis://redis/0?enable=1")
cache.setup("redis://redis?enable=True")
```
Also read about dynamic disabling at [simple cache](#simple-cache) section

### Backends

#### Memory
Store values in a dict, have 2 strategies to expire keys: 
deferred task to remove key, can overload loop by big amount of async tasks, that's why use strategy with storing expiration time is prefer
This strategy check expiration on 'get' and periodically purge expired keys
Also size of memory cache limit with size parameter (default 1000):

```python
cache.setup("mem://?size=500")
cache.setup("mem://?check_interval=10&size=10000") # using strategy with expiration store, we increase check_interval be
```
#### Redis
Required aioredis package
Store values in a redis key-value storage. Use 'safe' parameter to avoid raising any connection errors, command will return None in this case.
This backend use pickle to store values, but the cashes store values with sha1 keyed hash.
So you should set 'hash_key' parameter to protect your application from security vulnerabilities.
You can set parameters for [redis pool](https://aioredis.readthedocs.io/en/v1.3.0/api_reference.html#aioredis.create_pool) by backend setup    

```python
cache.setup("redis://0.0.0.0/?db=1&minsize=10&safe=1&hash_key=my_sicret")
cache.setup("redis://0.0.0.0/", db=1, password="my_pass", create_connection_timeout=0.1, safe=0, hash_key="my_sicret")
```

### Simple cache

Typical cache strategy: execute, store and return cached value till expiration::

```python

from cashews import cache
from datetime import timedelta

@cache(ttl=timedelta(hours=3))
async def long_running_function(arg, kward):
    ...
```

:param ttl: duration in seconds or in timedelta object or a callable object to store a result

:param func_args: arguments of call that will be used in key, can be tuple or dict with argument name as a key and callable object as a transform function for value of this argument

```python

@cache(ttl=100, func_args=("arg", "token"))
async def long_running_function(arg, user: User, token: str = "token"):
    ...

await long_running_function("name", user=user, token="qdfrevt")  # key will be like "long_running_function:arg:name:token:qdfrevt
```
But what if we want to user argument define a cache key or want to hide token from cache
```python
@cache(ttl=100, func_args={"arg": True, "token": get_md5, "user": attrgetter("uid")})
async def long_running_function(arg, user: User, token: str = "token"):
    ...

await long_running_function("name", user=user, token="qdfrevt")  # key will be like "long_running_function:arg:name:token:7ea802f0544ff108aace43e2d3752a28:user:51e6da60-2553-45ec-9e56-d9538b9614c8
```


:param key: custom cache key, may contain alias to args or kwargs passed to a call (like 'key_{token}/{arg}\{user}')

:param store: callable object that determines whether the result will be saved or not

:param prefix: custom prefix for key


### Fail cache
Return cache result (at list 1 call of function call should be succeed) if call raised one of the given exceptions,
    
:param ttl: duration in seconds or in timedelta object or a callable object to store a result

:param exceptions: exceptions at which returned cache result

:param func_args: [see simple cache params](#simple-cache)

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param prefix: custom prefix for key, default "fail"

Example
-------
```python
from cashews import cache  # or from cashews import fail

@cache.fail(ttl=timedelta(hours=2))
async def get(name):
    value = await api_call()
    return {"status": value}
```

### Hit cache
Cache call results and drop cache after given numbers of call 'cache_hits'

:param ttl: duration in seconds or in timedelta object or a callable object to store a result

:param cache_hits: number of cache hits till cache will dropped

:param update_before: number of cache hits before cache will update

:param func_args: [see simple cache params](#simple-cache)

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param store: callable object that determines whether the result will be saved or not

:param prefix: custom prefix for key, default "hit"

Example
-------
```python
from cashews import cache  # or from cashews import hit

@cache.hit(ttl=timedelta(hours=2), cache_hits=100, update_before=2)
async def get(name):
    ...
```

### Performance downgrade detection
Trace time execution of target and throw exception if it downgrade to given condition
  
:param ttl: duration in seconds or in timedelta object or a callable object to store a result

:param func_args: [see simple cache params](#simple-cache)

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param trace_size: the number of calls that are involved

:param perf_condition: callable object that determines whether the result will be cached,
       default if doubled mean value of time execution less then current

:param prefix: custom prefix for key, default 'perf'

```python
from cashews import cache   # or from cashews import perf

@cache.perf(ttl=timedelta(hours=2))
async def get(name):
    value = await api_call()
    return {"status": value}
``` 

### Locked
Decorator that can help you to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
Lock following function calls till first one will be finished
Can guarantee that one function call for given ttl, if ttl is None

:param ttl: duration to lock wrapped function call

:param func_args: [see simple cache params](#simple-cache)

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param prefix: custom prefix for key, default 'early'

```python

from cashews import cache  # or from cashews import locked

@cache.locked(ttl=timedelta(minutes=10))
async def get(name):
    value = await api_call()
    return {"status": value}
```

### Early
Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
With a hot cache recalculate a result in background near expiration time
Warning! Not good at cold cache

:param ttl: seconds in int or as timedelta object to store a result

:param func_args: [see simple cache params](#simple-cache)

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param store: callable object that determines whether the result will be saved or not

:param prefix: custom prefix for key, default 'early'
```python
from cashews import cache  # or from cashews import early

@cache.early(ttl=timedelta(minutes=10))
async def get(name):
    value = await api_call()
    return {"status": value}
```

### Rate limit 
Rate limit for function call. Do not call function if rate limit is reached, and call given action

:param limit: number of calls

:param period: Period

:param ttl: time to ban, default == period

:param func_args: [see simple cache params](#simple-cache)

:param action: call when rate limit reached, default raise RateLimitException

:param prefix: custom prefix for key, default 'rate_limit'
```python
from cashews import cache  # or from cashews import rate_limit

# no more then 10 calls per minute or ban for 10 minutes
@cache.rate_limit(limit=10, period=timedelta(minutes=1) ttl=timedelta(minutes=10))
async def get(name):
    return {"status": value}
```

### Circuit breaker
Circuit breaker

:param errors_rate: Errors rate in percents

:param period: Period

:param ttl: seconds in int or as timedelta to keep circuit breaker switched

:param func_args: arguments that will be used in key

:param exceptions: exceptions at which returned cache result

:param key: custom cache key, may contain alias to args or kwargs passed to a call

:param prefix: custom prefix for key, default "circuit_breaker"
```python
from cashews import cache  # or from cashews import rate_limit


@cache.circuit_breaker(errors_rate=10, period=timedelta(minutes=1) ttl=timedelta(minutes=5))
async def get(name):
    ...
```    

### Basic api
There are 13 basic methods to work with key-storage::```python
```python
from cashews import cache

cache.setup("mem://")

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

### Invalidation
Cache invalidation - on of the main Computer Science well known problem. That's why `ttl` is a required parameter for all cache decorators
Another strategy to cache invalidation implement in next api:

```python
from cashews import cache
from datetime import timedelta

@cache(ttl=timedelta(days=1))
asunc def user_items(user_id, fresh=False):
    ...

@cache(ttl=timedelta(hours=3))
async def items(page=1):
    ...

@cashews.cache_utils.invalidate.invalidate("module:items:page:*")  # the same as @cache.invalidate(items)
@cashews.cache_utils.invalidate.invalidate(user_items, {"user_id": lambda user: user.id}, defaults={"fresh"; True})
async def create_item(user):
   ...
```

Also you may face problem with invalid cache arising on code changing. For example we have:
```python

@cache(ttl=timedelta(days=1))
asunc def get_user(user_id):
    return {"name": "Dmitry", "surname": "Krykov"}
```
Than we did changes

    -    return {"name": "Dmitry", "surname": "Krykov"}
    +    return {"full_name": "Dmitry Krykov"}


There is no way simple way to automatically detect that kind of cache invalidity, because it is a dict.
Сertainly we can add prefix for this cache:
```python
@cache(ttl=timedelta(days=1), prefix="v2")
asunc def get_user(user_id):
    return {"full_name": "Dmitry Krykov"}
```
but usually we forget to do it...
The best defense against such errors is to use objects like `dataclasses` for operating with structures, 
it adds distinctness and `cashews` can detect changes in this structure automatically by checking representation (repr) of object.
So you can you use your own datacontainer with defined `__repr__` method that rise `AttributeError`:
```python
from dataclasses import dataclass

@dataclass()
class User:
    name: str
    surname: str
# OR
class User:
    
    def __init__(self, name, surname):
        self.name, self.surname = name, surname
        
    def __repr__(self):
        return f"{self.name} {self.surname}"

# Will detect changes of structure
@cache(ttl=timedelta(days=1), prefix="v2")
asunc def get_user(user_id):
    return User("Dima", "Krykov")
```

##Detect source of a result
Decorators give to us very simple api but it makes difficult to understand what led to this result - cache or direct call
To solve this problem cashews have a simple API:
```python
from cashews import context_cache_detect

context_cache_detect.start()
response = await decorated_function()
keys = context_cache_detect.get()
print(keys)
# >>> {"key": {"ttl": 10}, "fail_key": {"ttl": timedelta(hours=10), "exc": RateLimit}}

# OR
from cashews import CacheDetect

cache_detect = CacheDetect()
await func(_from_cache=cache_detect)
assert cache_detect.get() == {}

await func(_from_cache=cache_detect)
assert len(cache_detect.get()) == 1
```
You can use it in your web app:
```python
@app.middleware("http")
async def add_from_cache_headers(request: Request, call_next):
    context_cache_detect.start()
    response = await call_next(request)
    keys = context_cache_detect.get()
    if keys:
        key = list(keys.keys())[0]
        response.headers["X-From-Cache"] = key
        expire = await mem.get_expire(key)
        if expire == -1:
            expire = await cache.get_expire(key)
        response.headers["X-From-Cache-Expire-In-Seconds"] = str(expire)
        response.headers["X-From-Cache-TTL"] = str(keys[key]["ttl"].total_seconds())
        if "exc" in keys[key]:
            response.headers["X-From-Cache-Exc"] = str(keys[key]["exc"])
    return response
```


Scopes [user/method/account]
Info by key template