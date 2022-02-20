import asyncio
import uuid

from cashews import cache

capacity = 1000
expect_false_positive = 1
fill_elements = 1000
test_n = 100

call_count = 0

cache.setup("redis://")


@cache.dual_bloom(name="test:{key}", capacity=capacity, false=expect_false_positive, no_collisions=False)
# @cache.bloom(capacity=capacity, false_positives=expect_false_positive)
# @cache.bloom(name="lower:{key}", capacity=capacity, false_positives=expect_false_positive)
async def _simple_bloom(key):
    await asyncio.sleep(0)
    global call_count
    call_count += 1
    return str(key)[0] not in ("c", "a", "b", "d", "e", "f")


async def main():
    await cache.clear()
    global call_count
    all = {}
    # fill bloom
    for _ in range(fill_elements):
        k = uuid.uuid4()
        v = await _simple_bloom(k)
        all[k] = v

    # call_count = 0
    # for _ in range(test_n):
    #     await _simple_bloom(uuid.uuid4())

    # print("False not exist: ", 100 * (test_n - call_count)/test_n)
    # print("Expect False positive: ", expect_false_positive)

    call_count = 0
    false = 0
    for k, v in all.items():
        if await _simple_bloom(k) != v:
            false += 1

    print(f"False {false} and calls {call_count} of {len(all)} ")

    # call_count = 0
    # for _ in range(test_n):
    #     await _simple_bloom(uuid.uuid4())
    #
    # print("False not exist: ", 100 * (test_n - call_count) / test_n)


asyncio.run(main())
