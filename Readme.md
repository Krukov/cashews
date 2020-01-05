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
- Support Multi backend (Memory, Redis, memcache by request)
- Can cache any objects securely with pickle (use hash key). 
- Simple configuring and API

## API
- [simple cache](#simple-cache)
- [fail cache](#fail-cache)
- [hit-rate cache](#hit-cache)
- [perf-rate cache](#performance-downgrade-cache)
- [rate-limit](#rate-limit)
- [cache with early expiration/rebuilding](#early)
- [locked cache](#locked) 
- [api for key storage/backend](#basic-api)
- [auto invalidation](#invalidation)

Usage
-----

### Configure
Cache object is a single object that can be configured in one place by url::

    from cashews import cache
    
    cache.setup("redis://0.0.0.0/?db=1&create_connection_timeout=0.5&safe=0&hash_key=my_sicret&enable=1")
    or
    cache.setup("redis://0.0.0.0/", db=1, create_connection_timeout=0.5, safe=False, hash_key=b"my_key", enable=True)
    or
    cache.setup("mem://") # for inmemory cache

if you dont like global objects or prefer more manageable way you can work with cache class 

    from cashews import Cache
    
    cache = Cache()
    cache.setup("mem://")



### Simple cache

Typical cache strategy: execute, store and return cached value till expiration::

    from cashews import cache
    from datetime import timedelta
    
    @cache(ttl=timedelta(hours=3))
    async def long_running_function(arg, kward):
        ...



:param ttl: seconds in int value or as timedelta object to define time to store objects
:param func_args: arguments of call that will be used in key, can be tuple or dict with argument name as a key and callable object as a transform function for value of this argument

    @cache(ttl=100, func_args=("arg", "token"))
    async def long_running_function(arg, user: User, token: str = "token"):
        ...

    await long_running_function("name", user=user, token="qdfrevt")  # key will be like "long_running_function:arg:name:token:qdfrevt

But what if we want to user argument define a cache key or want to hide token from cache
    
    
    @cache(ttl=100, func_args={"arg": True, "token": get_md5, "user": attrgetter("uid")})
    async def long_running_function(arg, user: User, token: str = "token"):
        ...

    await long_running_function("name", user=user, token="qdfrevt")  # key will be like "long_running_function:arg:name:token:7ea802f0544ff108aace43e2d3752a28:user:51e6da60-2553-45ec-9e56-d9538b9614c8



:param key: custom cache key, may contain alias to args or kwargs passed to a call (like 'key_{token}/{arg}\{user}')
:param condition: callable object that determines whether the result will be saved or not
:param prefix: custom prefix for key


### Fail cache
Return cache result (at list 1 call of function call should be succeed) if call raised one of the given exceptions,
    
:param ttl: seconds in int or as timedelta object to store a result
:param exceptions: exceptions at which returned cache result
:param func_args: [see simple cache params](#simple-cache)
:param key: custom cache key, may contain alias to args or kwargs passed to a call
:param prefix: custom prefix for key, default "fail"
            
Example
-------
    
    from cashews import cache  # or from cashews import fail
    
    @cache.fail(ttl=timedelta(hours=2))
    async def get(name):
        value = await api_call()
        return {"status": value}


### Hit cache
Cache call results and drop cache after given numbers of call 'cache_hits'

:param ttl: seconds in int or as timedelta object to store a result
:param cache_hits: number of cache hits till cache will dropped
:param update_before: number of cache hits before cache will update
:param func_args: [see simple cache params](#simple-cache)
:param key: custom cache key, may contain alias to args or kwargs passed to a call
:param condition: callable object that determines whether the result will be saved or not
:param prefix: custom prefix for key, default "hit"

Example
-------
    
    from cashews import cache  # or from cashews import hit
    
    @cache.hit(ttl=timedelta(hours=2), cache_hits=100, update_before=2)
    async def get(name):
        ...
        
        
### Performance downgrade cache
Trace time execution of target and enable cache if it downgrade to given condition

:param ttl: seconds in int or as timedelta object to store a result
:param func_args: [see simple cache params](#simple-cache)
:param key: custom cache key, may contain alias to args or kwargs passed to a call
:param trace_size: the number of calls that are involved
:param perf_condition: callable object that determines whether the result will be cached,
       default if doubled mean value of time execution less then current
:param prefix: custom prefix for key, default 'perf'

    
    from cashews import cache   # or from cashews import perf
    
    @cache.perf(ttl=timedelta(hours=2))
    async def get(name):
        value = await api_call()
        return {"status": value}
 

### Locked
Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
Lock following function calls till it be cached
Can guarantee one function call for given ttl

:param ttl: seconds in int or timedelta object to store a result
:param func_args: [see simple cache params](#simple-cache)
:param key: custom cache key, may contain alias to args or kwargs passed to a call
:param lock_ttl: seconds in int or timedelta object to lock wrapped function call
        (should be more than function execution time)
:param prefix: custom prefix for key, default 'early'

    from cashews import cache  # or from cashews import locked
    
    @cache.locked(ttl=timedelta(minutes=10))
    async def get(name):
        value = await api_call()
        return {"status": value}
    

### Early
Cache strategy that try to solve Cache stampede problem (https://en.wikipedia.org/wiki/Cache_stampede),
With a hot cache recalculate a result in background near expiration time
Warning! Not good at cold cache

:param ttl: seconds in int or as timedelta object to store a result
:param func_args: [see simple cache params](#simple-cache)
:param key: custom cache key, may contain alias to args or kwargs passed to a call
:param condition: callable object that determines whether the result will be saved or not
:param prefix: custom prefix for key, default 'early'


### Rate limit 
Rate limit for function call. Do not call function if rate limit is reached, and call given action

:param limit: number of calls
:param period: Period
:param ttl: time to ban, default == period
:param func_args: [see simple cache params](#simple-cache)
:param action: call when rate limit reached, default raise RateLimitException
:param prefix: custom prefix for key, default 'rate_limit'
    
    from cashews import cache  # or from cashews import rate_limit
    
    # no more then 10 calls per minute or ban for 10 minutes
    @cache.rate_limit(limit=10, period=timedelta(minutes=1) ttl=timedelta(minutes=10))
    async def get(name):
        return {"status": value}
    

### Basic api
There are 11 basic methods to work with key-storage::

    from cashews import cache
    
    cache.setup("mem://")

    await cache.set(key="key", value={"any": True}, expire=60, exist=None)  # -> bool
    await cache.get("key")  # -> Any
    await cache.incr("key") # -> int
    await cache.delete("key")
    await cache.expire("key", timeout=10)
    await cache.ping(message=None)  # -> bytes
    await cache.clear()
    await cache.is_locked("key", wait=60)  # -> bool
    async with cache.lock("key", expire=10):
       ...
    await cache.set_lock("key", value="value", expire=60)  # -> bool
    await cache.unlock("key", "value")  # -> bool
    

### Invalidation
Cache invalidation - on of the main Computer Science well known problem. That's why 'ttl' is a required parameter for all cache decorators
Another strategy to cache invalidation implement in next api:

    from cashews import cache
    from datetime import timedelta
    
    @cache(ttl=timedelta(days=1))
    asunc def user_items(user_id, fresh=False):
        ...

    @cache(ttl=timedelta(hours=3))
    async def items(page=1):
        ...

    @cache.invalidate("module:items:page:*")  # the same as @cache.invalidate(items)
    @cache.invalidate(user_items, {"user_id": lambda user: user.id}, defaults={"fresh"; True})
    async def create_item(user):
       ...



 todos:
 * cache size
