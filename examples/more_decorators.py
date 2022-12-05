import asyncio
import random

from cashews import CircuitBreakerOpen, RateLimitError, cache

cache.setup("redis://")
cache.setup("mem://", prefix="fast")


@cache.failover(ttl="10m", exceptions=(RateLimitError,))
@cache.rate_limit(limit=10, period="10m")
async def function_with_limit(foo):
    await asyncio.sleep(0.1)
    return foo


@cache.failover(ttl="1m", prefix="fast:failover")
@cache.failover(ttl="10m", exceptions=(CircuitBreakerOpen,))
@cache.circuit_breaker(errors_rate=10, period="10m", ttl="5m", half_open_ttl="1m")
async def function_that_may_fail(foo):
    await asyncio.sleep(0.1)
    if random.choice([0, 0, 0, 1]):
        raise Exception("error")
    return foo


async def main():
    for _ in range(20):
        print(await function_with_limit("simple"))

    for _ in range(20):
        print(await function_that_may_fail("simple"))


if __name__ == "__main__":
    asyncio.run(main())
